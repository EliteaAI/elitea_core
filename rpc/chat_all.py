from datetime import datetime, timedelta, timezone
from typing import Optional

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import joinedload
from tools import db, auth, serialize, this, VaultClient, rpc_tools, config as c

import redis
from pydantic import ValidationError
from pydantic.utils import deep_update
from sqlalchemy import asc, desc, String

from ..utils.chat_constants import SUMMARIZATION_LOCKING_TTL
from ..utils.conversation_utils import get_conversation_locked_key
from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes, ChatHistoryTemplates, PublishStatus
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.participants import ParticipantMapping, Participant
from ..models.application import ApplicationVersion
from ..models.pd.message import MessageGroupDetail
from ..models.pd.participant import ParticipantEntityUser, ParticipantEntityDummy, ParticipantBase, entity_meta_mapping, ParticipantCreate
from ..models.pd.participant_settings import EntitySettingsApplication, EntitySettingsUser
from ..models.pd.predict import SioPredictModel, SioContinuePredictModel, ToolkitToolCallPayload
from ..utils.chat_history import (
    generate_chat_history,
    generate_user_input
)
from ..utils.chat_feature_flags import get_context_manager_feature_flag
from ..utils.llm_settings import DEFAULT_REASONING_MODEL_MAX_TOKENS, DEFAULT_MAX_TOKENS
from ..utils.participant_utils import get_or_create_one, delete_entity_from_all_conversations, add_participant_to_conversation
from ..utils.sio_utils import get_chat_room
from ..utils.attachments import NotSupportableProcessorExtension, read_file_content, process_single_attachment_file
from ..utils.sio_utils import SioEvents, SioValidationError
from ..utils.authors import get_authors_data
from ..utils.internal_tools import (
    inject_internal_imagegen_tool, ImageGenConfigurationError,
    inject_internal_attachment_tool, ATTACHMENT_INTERNAL_TOOL_KEY
)


CHAT_PREDICT_MAPPER = {
    ParticipantTypes.dummy: 'applications_predict_sio_llm',
    ParticipantTypes.llm: 'applications_predict_sio_llm',
    ParticipantTypes.application: 'applications_predict_sio',
    ParticipantTypes.datasource: 'datasources_predict_sio',
    ParticipantTypes.toolkit: 'applications_test_toolkit_tool_sio',
    # ParticipantTypes.pipeline: 'applications_predict_sio',
}


def generate_toolkit_payload(
    session,
    conversation_uuid: str,
    user_id: int,
    conversation_project_id: int,
    current_participant_id: int = None,
    internal_tools: list[str] = None,
    is_llm_chat: bool = False
) -> Optional[list]:
    """
    Generate toolkit payload for SDK. This is a transport layer - no filtering logic.
    All filtering (duplicates, self-handoff prevention) should happen in SDK.

    Args:
        current_participant_id: The participant being predicted to. Passed to SDK for self-identification.
        internal_tools: List of enabled internal tools (from conversation + agent version meta).
        is_llm_chat: If True, always inject attachment toolkit (LLM/dummy chats).
    """
    tools = []
    conversation = session.query(Conversation).filter(
        Conversation.uuid == conversation_uuid
    ).first()
    participants_toolkits = [p for p in conversation.participants if p.entity_name == ParticipantTypes.toolkit]

    # TODO: expand all at once for speed, but eventually may have different project_ids
    for participant_plus in participants_toolkits:
        try:
            toolkit_id = participant_plus.entity_meta.get('id')
            toolkit_details = this.module.get_toolkit_by_id_expanded(
                project_id=participant_plus.entity_meta.get('project_id'),
                toolkit_id=toolkit_id,
                user_id=user_id,
                unsecret=True,
            )
            if 'error' in toolkit_details:
                log.warning(f"Skipping toolkit id={toolkit_id} due to error: {toolkit_details['error']}")
                continue
            tools.append(toolkit_details)
        except Exception as e:
            log.warning(f"Skipping toolkit id={participant_plus.entity_meta.get('id')} due to error: {str(e)}")
            continue

    # Always include application participants - SDK handles swarm logic and filtering
    participants_applications = [p for p in conversation.participants if p.entity_name == ParticipantTypes.application]
    for app_participant in participants_applications:
        try:
            # Get application version from participant mapping
            participant_mapping = session.query(ParticipantMapping).filter(
                ParticipantMapping.participant_id == app_participant.id,
                ParticipantMapping.conversation_id == conversation.id
            ).first()
            if not participant_mapping:
                log.warning(
                    f"Skipping application participant name={app_participant.meta.get('name')} id={app_participant.id} "
                    f"due to error: not in conversation {conversation.id}"
                )
                continue

            app_id = app_participant.entity_meta['id']
            app_version_id = participant_mapping.entity_settings.get('version_id')

            # Get agent_type from meta, fallback to DB if missing (for old participants)
            agent_type = app_participant.meta.get('agent_type')
            if not agent_type and app_version_id:
                try:
                    version_row = session.query(ApplicationVersion.agent_type).filter(
                        ApplicationVersion.id == app_version_id
                    ).first()
                    if version_row:
                        agent_type = version_row[0]
                except Exception:
                    log.debug(f"Could not fetch agent_type for version {app_version_id}")

            # Include all application participants - SDK will handle:
            # - Whether to create handoff tools (based on internal_tools having 'swarm')
            # - Self-handoff prevention (using participant_id)
            # - Deduplication
            app_toolkit_details = {
                "type": "application",
                "name": app_participant.meta['name'],
                "description": app_participant.meta.get('description', ''),
                "author_id": user_id,
                "participant_id": app_participant.id,  # For SDK to identify self
                "project_id": app_participant.entity_meta.get('project_id'),
                "settings": {
                    "variables": [],
                    "application_id": app_id,
                    "selected_tools": [],
                    "application_version_id": app_version_id,
                },
                "id": None,
                "toolkit_name": app_participant.meta['name'],
                "agent_type": agent_type,
                "created_at": datetime.now(tz=timezone.utc).isoformat(),
            }
            tools.append(app_toolkit_details)
        except Exception as e:
            log.warning(f"Skipping application id={app_participant.entity_meta.get('id')} due to error: {str(e)}")
            continue

    # Inject internal ImageGen tool if toggle is enabled and no manual ImageGen exists
    try:
        imagegen_tool = inject_internal_imagegen_tool(
            conversation_meta=conversation.meta or {},
            user_id=user_id,
            project_id=conversation_project_id,
            existing_tools=tools,
            conversation_uuid=conversation_uuid,
        )
        if imagegen_tool:
            tools.append(imagegen_tool)
    except ImageGenConfigurationError as e:
        # Convert to PayloadGenerationError so caller can handle consistently
        raise PayloadGenerationError(str(e)) from e
    except Exception as e:
        # Log but don't fail the entire payload generation for optional feature
        log.warning(f"Failed to inject internal ImageGen tool: {e}")

    # Inject internal attachment tool:
    # - For LLM/dummy chats: always inject (is_llm_chat=True)
    # - For agent chats: inject only if 'attachments' in internal_tools from agent version
    try:
        # SECURITY: For agent chats, use ONLY agent's internal_tools - never merge with conversation meta
        # For LLM chats, internal_tools will be None and always_inject=True handles it
        attachment_tool = inject_internal_attachment_tool(
            project_id=conversation_project_id,
            existing_tools=tools,
            internal_tools=internal_tools or [],
            always_inject=is_llm_chat
        )
        if attachment_tool:
            tools.append(attachment_tool)
    except Exception as e:
        # Log but don't fail the entire payload generation for optional feature
        log.warning(f"Failed to inject internal attachment tool: {e}")
    
    return tools


def process_attachment_message_items(
    session,
    project_id: int,
    message_group,
    attachments_info,
    stream_id=None,
    message_id=None,
    user_id=None,
    sid=None,
    collection_suffix="attach",
    llm_settings=None
):
    if not attachments_info:
        return message_group

    conversation = message_group.conversation

    attachment_message_items = []
    items_needing_content = []
    failed_attachments = []  # Track failed attachments

    for order_index, attachment_info in enumerate(attachments_info, start=1):
        filepath = attachment_info.filepath
        
        try:
            # Use the common processing function with filepath
            attachment_msg, needs_content_extraction = process_single_attachment_file(
                session=session,
                project_id=project_id,
                msg_group=message_group,
                filepath=filepath,
                order_index=order_index,
                user_id=user_id,
                collection_suffix=collection_suffix,
            )
            
            session.add(attachment_msg)
            attachment_message_items.append(attachment_msg)

            if needs_content_extraction:
                # Track items that need content extraction
                items_needing_content.append(attachment_msg)

        except NotSupportableProcessorExtension:
            error_msg = f"Unsupported file type for filepath {filepath}"
            log.error(error_msg)
            failed_attachments.append({"filepath": filepath, "error": error_msg})
            # Continue processing other attachments
        except Exception as e:
            error_msg = f"Failed to process filepath {filepath}: {str(e)}"
            log.error(error_msg)
            failed_attachments.append({"filepath": filepath, "error": str(e)})
            # Continue processing other attachments
    
    # If all attachments failed, raise an error
    if failed_attachments and not attachment_message_items:
        errors = "; ".join([f"{item['filepath']}: {item['error']}" for item in failed_attachments])
        raise RuntimeError(f"All attachments failed to process: {errors}") from None
    
    # If some attachments failed, log warning but continue
    if failed_attachments:
        log.warning(f"{len(failed_attachments)} attachment(s) failed to process: {failed_attachments}")

    # Content extraction: read file content and enrich attachment messages
    if items_needing_content:
        log.debug(f"Starting content extraction for {len(items_needing_content)} documents in message group {message_group.uuid}")
        try:
            if not llm_settings:
                raise RuntimeError("LLM settings must be provided for reading attachment content")

            # Batch read all file contents
            filepaths = [item.name for item in items_needing_content]
            read_result = read_file_content(
                project_id=project_id,
                llm_settings=llm_settings,
                filepaths=filepaths,
                sid=sid,
                stream_id=stream_id,
                message_id=message_id,
                question_id=str(message_group.uuid),  # Use user message uuid as question_id
            )
            
            # Enrich each AttachmentMessageItem with file content
            # read_result structure: {'success': True, 'result': {filename: content, ...}, ...}
            file_contents = read_result.get('result', {})
            for item in items_needing_content:
                file_content = file_contents.get(item.name, "")
                if file_content:
                    # Append content as text chunk (normal content or size limit error message)
                    current_content = item.content or []
                    current_content.append({
                        "type": "text",
                        "text": str(file_content)
                    })
                    item.content = current_content
                    flag_modified(item, "content")  # Mark JSONB field as modified
                else:
                    log.warning(f"No content returned for file {item.name}")

            log.debug(f"Successfully enriched content for {len(items_needing_content)} documents")

        except Exception as e:
            log.error(f"Content extraction failed for message group {message_group.uuid}: {e}")
            raise RuntimeError(f"Failed to read document content: {str(e)}") from None

    log.debug(f"Processed {len(attachment_message_items)} attachment items for message group {message_group.uuid}")
    return message_group


class PayloadGenerationError(Exception):
    """Custom exception for errors during payload generation."""
    pass


def generate_summary_payload(
        project_id: int,
        message_groups: list[ConversationMessageGroup],
        llm_settings: dict | None = None,
        summary_instructions: str | None = None,
) -> dict:
    """
    Generate payload for LLM summary prediction using message groups ORM objects

    Args:
        project_id: The project ID
        message_groups: List of ConversationMessageGroup ORM objects
        llm_settings: LLM configuration settings
        summary_instructions: Custom instructions for summary generation
    Returns:
        Dict containing the structured payload for applications_predict_sio_llm
    """
    try:
        # Convert message groups to chat history format using existing utility
        chat_history = generate_chat_history(message_groups)

        # Default summary instructions if not provided
        if not summary_instructions:
            summary_instructions = (
                "Generate a concise summary of the following conversation messages."
            )

        # Create the user input for summary generation
        user_input: str = summary_instructions

        # Prepare LLM settings with defaults
        if not llm_settings:
            raise PayloadGenerationError("LLM settings must be provided for summary generation")

        # Create the payload structure compatible with applications_predict_sio_llm
        payload = {
            "project_id": project_id,
            "llm_settings": llm_settings,
            "user_input": user_input,
            "chat_history": chat_history,
            "thread_id": None,
            "instructions": summary_instructions,
            "interaction_uuid": None, # No interaction_uuid needed for summary
            "tools": [],  # No tools needed for summary
            "variables": None,  # No variables needed
        }

        log.debug(f"Chat history contains {len(chat_history)} messages")
        return payload

    except Exception as e:
        log.error(f"Error in generate_summary_payload: {str(e)}")
        raise Exception(f"Failed to generate summary payload: {str(e)}")


def generate_toolkit_participant_payload(
    session,
    msg_group: ConversationMessageGroup,
    predict_payload: SioPredictModel,
) -> dict:
    """
    Generate payload for toolkit participant predict calls.
    
    Args:
        session: Database session
        msg_group: Message group object
        predict_payload: Predict payload model
        
    Returns:
        Dict containing the structured payload for applications_test_toolkit_tool_sio
    """
    participant: Participant = msg_group.sent_to
    
    # Get toolkit details from entity_meta
    toolkit_id = participant.entity_meta.get('id')
    toolkit_project_id = participant.entity_meta.get('project_id')
    
    # Check if tool_call_input is provided first (required for all cases)
    if not predict_payload.tool_call_input:
        raise PayloadGenerationError(
            "Toolkit participant requires tool_call_input to be provided"
        )
    
    user_input_payload: ToolkitToolCallPayload = predict_payload.tool_call_input
    
    # Fetch toolkit from database
    toolkit_details = this.module.get_toolkit_by_id(
        project_id=toolkit_project_id,
        toolkit_id=toolkit_id
    )
    
    if 'error' in toolkit_details:
        raise PayloadGenerationError(
            f"Failed to get toolkit id={toolkit_id} details: {toolkit_details['error']}"
        )
    
    toolkit_name = toolkit_details.get('name') or toolkit_details.get('toolkit_name')
    toolkit_type = toolkit_details.get('type')
    toolkit_settings = toolkit_details.get('settings', {})
    
    # Build the toolkit configuration
    toolkit_config = {
        'type': toolkit_type,
        'toolkit_name': toolkit_name,
        'toolkit_id': toolkit_id,
        'settings': toolkit_settings
    }
    
    # Build the final payload
    result = {
        'toolkit_config': toolkit_config,
        'tool_name': user_input_payload.tool_name,
        'tool_params': user_input_payload.tool_params,
        'project_id': predict_payload.project_id,
        'stream_id': predict_payload.conversation_uuid,
        'message_id': predict_payload.question_id,
        'mcp_tokens': predict_payload.mcp_tokens or {},
        'ignored_mcp_servers': predict_payload.ignored_mcp_servers or [],
    }
    
    # Add llm_settings if provided (ensure it's a dict, not a Pydantic model)
    if predict_payload.llm_settings:
        result['llm_settings'] = predict_payload.llm_settings.dict()
        
    if predict_payload.conversation_uuid:
        conversation = session.query(Conversation).filter(
            Conversation.uuid == predict_payload.conversation_uuid
        ).first()
        if conversation:
            result['conversation_id'] = conversation.id
    
    log.debug(
        f'Generated toolkit participant payload for question_id={predict_payload.question_id}, '
        f'toolkit_id={toolkit_id}, tool_name={user_input_payload.tool_name}'
    )
    
    return result


def generate_application_version_payload(
    session,
    msg_group: ConversationMessageGroup,
    predict_payload: SioPredictModel,
    entity_settings: EntitySettingsApplication,
) -> dict:
    """Generate application version payload from agent version and conversation toolkit participants with expanded toolkits configurations."""
    participant: Participant = msg_group.sent_to
    project_id = participant.entity_meta.get('project_id')

    application_id = participant.entity_meta.get('id')
    user_id = msg_group.author_participant.entity_meta['id']

    app_version_details = this.module.get_application_version_details_expanded(
        project_id=project_id,
        application_id=application_id,
        version_id=entity_settings.version_id,
        user_id=user_id,
        unsecret=True
    )
    if 'error' in app_version_details:
        raise PayloadGenerationError(
            f"Failed to get application version={entity_settings.version_id} details expanded: {app_version_details['error']}"
        )

    # Block prediction for unpublished or embedded-only versions
    version_status = app_version_details.get('status', '')
    if version_status in (PublishStatus.unpublished, PublishStatus.embedded):
        raise PayloadGenerationError(
            f"Agent version {entity_settings.version_id} has status '{version_status}' and cannot be used directly"
        )
    
    # Get internal_tools from agent version meta for attachment injection decision
    agent_internal_tools = app_version_details.get('meta', {}).get('internal_tools', [])
    
    # Get conversation participants as tools - SDK handles all filtering logic:
    # - Swarm mode detection (checks internal_tools for 'swarm')
    # - Self-handoff prevention (uses current_participant_id)
    # - Toolkit deduplication
    # For agent chats, inject attachment toolkit only if 'attachments' in internal_tools
    participants_toolkits = generate_toolkit_payload(
        session=session,
        conversation_uuid=predict_payload.conversation_uuid,
        user_id=msg_group.author_participant.entity_meta['id'],
        conversation_project_id=predict_payload.project_id,
        current_participant_id=msg_group.sent_to_id,
        internal_tools=agent_internal_tools,
        is_llm_chat=False
    )
    if participants_toolkits:
        # Pass all tools to SDK - deduplication happens there
        app_version_details['tools'].extend(participants_toolkits)
    # Pass current participant ID so SDK can identify self for loop prevention
    app_version_details['current_participant_id'] = msg_group.sent_to_id
    log.debug(
        f'Generated chat application version details payload for question_id={predict_payload.question_id}:\n{app_version_details}'
    )
    return app_version_details


def generate_payload(session, msg_group: ConversationMessageGroup, predict_payload: SioPredictModel) -> dict:
    participant_chat_settings: ParticipantMapping = session.query(
        ParticipantMapping.entity_settings
    ).where(
        ParticipantMapping.participant_id == msg_group.sent_to_id,
        ParticipantMapping.conversation_id == msg_group.conversation_id
    ).first()

    participant: Participant = msg_group.sent_to

    result = {
        'stream_id': predict_payload.conversation_uuid,
        'project_id': predict_payload.project_id,
        'interaction_uuid': predict_payload.interaction_uuid,
        'chat_history_template': participant_chat_settings.entity_settings.get(
            'chat_history_template', ChatHistoryTemplates.all.value
        ),
        'mcp_tokens': predict_payload.mcp_tokens or {},
        'ignored_mcp_servers': predict_payload.ignored_mcp_servers or [],
        'conversation_id': predict_payload.conversation_uuid,  # For planning toolkit scoping
        'should_continue': predict_payload.should_continue or False,
        'hitl_resume': bool(getattr(predict_payload, 'hitl_resume', False)),
        'hitl_action': getattr(predict_payload, 'hitl_action', None),
        'hitl_value': getattr(predict_payload, 'hitl_value', None),
        'thread_id': predict_payload.thread_id,
    }

    match participant.entity_name:
        case ParticipantTypes.application:
            entity_settings = EntitySettingsApplication.parse_obj(participant_chat_settings.entity_settings)
            result['project_id'] = participant.entity_meta.get('project_id')
            result['application_id'] = participant.entity_meta.get('id')
            result['entity_name'] = (participant.meta or {}).get('name', '')

            # Merge entity_settings (now WITHOUT llm_settings for new participants)
            result = deep_update(result, entity_settings.dict())

            # Generate version payload which contains the source-of-truth llm_settings
            result['version_details'] = generate_application_version_payload(
                session=session,
                msg_group=msg_group,
                predict_payload=predict_payload,
                entity_settings=entity_settings
            )

            # CRITICAL: Extract llm_settings from version_details (single source of truth)
            # This handles both old participants (with cached llm_settings) and new ones (without)
            if 'llm_settings' in result.get('version_details', {}):
                result['llm_settings'] = result['version_details']['llm_settings'].copy()

            # IMPORTANT: Use offset(1) to retrieve the previous agent message, skipping the newly created response
            last_agent_message: ConversationMessageGroup = session.query(ConversationMessageGroup).where(
                ConversationMessageGroup.author_participant_id == msg_group.sent_to_id,
                ConversationMessageGroup.conversation_id == msg_group.conversation_id
            ).order_by(desc(ConversationMessageGroup.created_at)).offset(1).first()
            log.debug(f'{serialize(last_agent_message)=}')
            if last_agent_message:
                result['thread_id'] = last_agent_message.meta.get('thread_id')

            # Apply user override if provided (e.g., from UI temporary override)
            if predict_payload.llm_settings and predict_payload.llm_settings.model_name:
                if 'llm_settings' not in result:
                    result['llm_settings'] = {}
                result['llm_settings'].update(predict_payload.llm_settings.dict(exclude_unset=True))

            # Merge internal_tools from conversation (UI toggle) and agent version (stored config)
            conversation_internal_tools = msg_group.conversation.meta.get('internal_tools', []) if msg_group.conversation.meta else []
            version_internal_tools = result.get('version_details', {}).get('meta', {}).get('internal_tools', [])
            # Combine both sources, removing duplicates while preserving order
            combined_tools = list(conversation_internal_tools)
            for tool in version_internal_tools:
                if tool not in combined_tools:
                    combined_tools.append(tool)
            result['internal_tools'] = combined_tools
        # case ParticipantTypes.pipeline:
        #     # TODO: handle as simplified application, but should not ?
        #     entity_settings = EntitySettingsApplication.parse_obj(participant_chat_settings.entity_settings)
        #     result['project_id'] = participant.entity_meta.get('project_id')
        #     result['application_id'] = participant.entity_meta.get('id')
        #     result = deep_update(result, entity_settings.dict())
        #     result['user_input'] = predict_payload.user_input
        #     last_agent_message: ConversationMessageGroup = session.query(ConversationMessageGroup).where(
        #         ConversationMessageGroup.author_participant_id == msg_group.sent_to_id,
        #         ConversationMessageGroup.conversation_id == msg_group.conversation_id
        #     ).order_by(desc(ConversationMessageGroup.created_at)).first()
        #     log.debug(f'{serialize(last_agent_message)=}')
        #     if last_agent_message:
        #         result['thread_id'] = last_agent_message.meta.get('thread_id')
        case ParticipantTypes.dummy:
            author_participant_settings = session.query(
                ParticipantMapping
            ).where(
                ParticipantMapping.participant_id == msg_group.author_participant_id,
                ParticipantMapping.conversation_id == msg_group.conversation_id
            ).first()
            author_settings = EntitySettingsUser.parse_obj(author_participant_settings.entity_settings)
            result['llm_settings'] = author_settings.dict().get('llm_settings', {})
            # For LLM chats, always inject attachment toolkit (is_llm_chat=True)
            result['tools'] = generate_toolkit_payload(
                session=session,
                conversation_uuid=predict_payload.conversation_uuid,
                user_id=msg_group.author_participant.entity_meta['id'],
                conversation_project_id=predict_payload.project_id,
                current_participant_id=msg_group.sent_to_id,
                is_llm_chat=True
            )
            # Pass current participant ID so SDK can identify self for loop prevention
            result['current_participant_id'] = msg_group.sent_to_id
            # Get instructions: conversation instructions + user's default instructions from profile
            base_instructions = str(msg_group.conversation.instructions or '')
            user_default_instructions = msg_group.conversation.meta.get('default_instructions', '')
            if user_default_instructions and base_instructions:
                result['instructions'] = f"{base_instructions}\n\n{user_default_instructions}"
            elif user_default_instructions:
                result['instructions'] = user_default_instructions
            else:
                result['instructions'] = base_instructions

            if predict_payload.llm_settings:
                result['llm_settings'].update(predict_payload.llm_settings.dict(exclude_none=True))
            result['internal_tools'] = msg_group.conversation.meta.get('internal_tools', [])
            # Get persona from conversation settings (user's saved preference), default to 'generic'
            result['persona'] = msg_group.conversation.meta.get('persona', 'generic')

        case ParticipantTypes.toolkit:
            # Toolkit participant: generate payload for toolkit tool call
            toolkit_payload = generate_toolkit_participant_payload(
                session=session,
                msg_group=msg_group,
                predict_payload=predict_payload
            )
            # Merge toolkit payload into result
            result = deep_update(result, toolkit_payload)
            return result

    # TODO: detect when continue flow with user input, use the request's user_input instead of message content
    result['user_input'] = generate_user_input(msg_group)

    # Add steps limit parameter if any
    result['steps_limit'] = msg_group.conversation.meta.get('steps_limit', None)

    return result


def prepare_conversation_history(
    session, sio, conversation: Conversation, msg_group: ConversationMessageGroup,
):
    """
    :param session: database session
    :param sio: socketio instance (unused, kept for API compatibility)
    :param conversation: conversation ORM object
    :param msg_group: message group ORM object
    :return: (message_groups list, summaries list, preserve_instructions bool)

    When a summary exists in context_analytics, only messages after the last summarized
    group are returned. The summary is prepended via the summaries list.
    """
    summaries: list[dict] = []
    preserve_instructions: bool = True

    context_analytics = (conversation.meta or {}).get('context_analytics', {})
    last_summarization = context_analytics.get('last_summarization') if context_analytics else None
    last_summarized_group_id = (
        last_summarization.get('last_summarized_group_id') if last_summarization else None
    )

    if last_summarized_group_id and last_summarization.get('summary_content'):
        chat_history_groups = conversation.message_groups.where(
            ConversationMessageGroup.id > last_summarized_group_id,
            ConversationMessageGroup.created_at < msg_group.created_at,
        ).order_by(
            asc(ConversationMessageGroup.created_at)
        ).all()
        summaries = [{'summary_content': last_summarization['summary_content']}]
    else:
        chat_history_template = ChatHistoryTemplates.all.value
        try:
            chat_history_template = int(chat_history_template)
            chat_history_groups = list(reversed(conversation.message_groups.where(
                ConversationMessageGroup.created_at < msg_group.created_at,
            ).order_by(
                desc(ConversationMessageGroup.created_at)
            ).limit(chat_history_template).all()))
        except ValueError:
            chat_history_groups = conversation.message_groups.where(
                ConversationMessageGroup.created_at < msg_group.created_at
            ).order_by(
                asc(ConversationMessageGroup.created_at)
            ).all()

    return list(chat_history_groups), summaries, preserve_instructions


class RPC:
    @web.rpc("chat_predict_sio", "chat_predict_sio")
    def predict_sio(
        self, sid: str | None, data: dict, await_task_timeout: int = -1, return_message_ids: bool = False
    ) -> Optional[str | dict]:
        try:
            parsed = SioPredictModel.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_predict.value,
                error=e.errors(),
                stream_id=data.get("conversation_uuid"),
                message_id=data.get("payload", {}).get("message_id"),
            )

        if sid:
            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
        else:
            current_user = auth.current_user()
        
        # log.info(f'chat {parsed=}')
        with db.get_session(parsed.project_id) as session:
            conversation: Conversation = session.query(Conversation).where(
                Conversation.uuid == parsed.conversation_uuid
            ).first()
            context_management_enabled = get_context_manager_feature_flag(
                parsed.project_id,
            )

            if parsed.participant_id is None and "@everyone" not in parsed.user_input.lower() and not parsed.user_ids:
                dummy_participant, _ = get_or_create_one(
                    session=session,
                    entity_name=ParticipantTypes.dummy,
                    entity_meta=ParticipantEntityDummy()
                )
                parsed.participant_id = dummy_participant.id
                # in case dummy is not added yet to this conversation
                dummy_participant_data = ParticipantCreate(
                    entity_name=ParticipantTypes.dummy,
                    entity_meta=ParticipantEntityDummy()
                )
                try:
                    add_participant_to_conversation(
                        session=session,
                        participant=dummy_participant_data,
                        conversation=conversation,
                        project_id=parsed.project_id,
                        initiator_id=None
                    )
                    session.refresh(conversation)

                except ValueError:
                    # Dummy already in conversation
                    pass

            author_participant, _ = get_or_create_one(
                session=session,
                entity_name=ParticipantTypes.user,
                entity_meta=ParticipantEntityUser(id=current_user['id'])
            )

            room = get_chat_room(conversation.uuid)

            self.check_and_generate_conversation_name(
                parsed.project_id, parsed.user_input, room, conversation
            )

            if not any(p.id == author_participant.id for p in conversation.participants):
                conversation.participants.append(author_participant)
                participant_model = ParticipantBase.from_orm(author_participant)
                if participant_model.entity_name == ParticipantTypes.user.value:
                    authors_data = get_authors_data([participant_model.entity_meta['id']])
                    if authors_data:
                        participant_model.meta['user_name'] = authors_data[0].get('name')
                        participant_model.meta['user_avatar'] = authors_data[0].get('avatar')
                self.context.sio.emit(
                    event=SioEvents.chat_participant_update,
                    data=participant_model.dict(),
                    room=room,
                )

            try:
                client = self.get_redis_client()
                conversation_locked_key: str = get_conversation_locked_key(parsed.project_id, parsed.conversation_uuid)
                is_conversation_locked: bool = client.get(conversation_locked_key) == 'true'

                if is_conversation_locked:
                    log.error("Conversation is locked while summarization is in progress")
                    self.context.sio.emit(
                        event=SioEvents.socket_validation_error.value,
                        data={
                            'event': SioEvents.chat_predict.value,
                            'content': 'Conversation is locked while summarization is in progress',
                            'type': 'error',
                            'stream_id': parsed.conversation_uuid,
                            'message_id': parsed.question_id
                        },
                        room=room
                    )
                    return {"error": "Conversation is locked while summarization is in progress"}
            except redis.exceptions.ConnectionError:
                log.error("Redis connection error")
                self.context.sio.emit(
                    event=SioEvents.socket_validation_error.value,
                    data={
                        'event': SioEvents.chat_predict.value,
                        'content': "Redis connection error",
                        'type': 'error',
                        'stream_id': parsed.conversation_uuid,
                        'message_id': parsed.question_id
                    },
                    room=room
                )
                return {"error": "Redis connection error"}

            # Cleanup: Remove any paused/empty response messages from previous MCP auth interruptions
            # If user sends a new message instead of clicking "Continue", we should clean up the empty response
            paused_empty_responses = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == conversation.id,
                ConversationMessageGroup.is_streaming == False,
                ConversationMessageGroup.reply_to_id.isnot(None),  # It's a response message
            ).all()
            
            for paused_msg in paused_empty_responses:
                # Check if the message has no content (empty response waiting for auth)
                has_content = session.query(TextMessageItem).filter(
                    TextMessageItem.message_group_id == paused_msg.id
                ).first() is not None
                
                if not has_content:
                    log.debug(f"Cleaning up paused empty response message: {paused_msg.uuid}")
                    session.delete(paused_msg)
            
            session.flush()

            msg_group_meta = dict()
            if parsed.interaction_uuid:
                # todo: add [30] min check last message for interaction_uuid
                msg_group_meta['interaction_uuid'] = parsed.interaction_uuid
            
            # Initialize context metadata for new message group
            if context_management_enabled:
                msg_group_meta['context'] = {
                    'token_count': None,  # Will be calculated below
                    'weight': 1.0,
                    'included': True,
                    'priority': 1.0,
                    'created_at': datetime.now(tz=timezone.utc).isoformat()
                }
            
            msg_group: ConversationMessageGroup = ConversationMessageGroup(
                uuid=parsed.question_id,
                conversation=conversation,
                author_participant=author_participant,
                sent_to_id=parsed.participant_id,
                meta=msg_group_meta,
            )
            msg: TextMessageItem = TextMessageItem(
                message_group=msg_group,
                item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
                content=parsed.user_input,
                order_index=0,
            )
            session.add(msg_group)
            session.add(msg)
            session.flush()

            response_msg = None
            if msg_group.sent_to_id:
                response_msg: ConversationMessageGroup = ConversationMessageGroup(
                    conversation=conversation,
                    author_participant_id=msg_group.sent_to_id,
                    is_streaming=True,
                    reply_to_id=msg_group.id,
                    created_at=datetime.now(tz=timezone.utc) + timedelta(seconds=1),
                )
                session.add(response_msg)
                session.flush()

            if parsed.attachments_info:
                try:
                    msg_group = process_attachment_message_items(
                        session,
                        parsed.project_id,
                        msg_group,
                        parsed.attachments_info,
                        stream_id=parsed.conversation_uuid,
                        message_id=str(response_msg.uuid) if response_msg else None,
                        user_id=current_user['id'],
                        sid=sid,
                        llm_settings=parsed.llm_settings.dict() if parsed.llm_settings else None
                    )
                except Exception as e:
                    log.error(e)
                    room = get_chat_room(parsed.conversation_uuid)
                    self.context.sio.emit(
                        event=SioEvents.socket_validation_error.value,
                        data={
                            'event': SioEvents.chat_predict.value,
                            'content': str(e),
                            'type': 'error',
                            'stream_id': parsed.conversation_uuid,
                            'message_id': parsed.question_id
                        },
                        room=room
                    )
                    return {"error": str(e)}

            session.commit()
            session.refresh(msg_group)

            # session.add(msg_group)  # Ensure the updated token count is tracked

            room = get_chat_room(parsed.conversation_uuid)
            self.context.sio.emit(
                event=SioEvents.chat_predict.value,
                data={
                    'type': 'chat_user_message',
                    **serialize(MessageGroupDetail.model_validate(msg_group))
                },
                room=room,
                skip_sid=sid
            )

            if msg_group.sent_to_id:
                # here we need to generate payload
                if rpc_func := CHAT_PREDICT_MAPPER.get(msg_group.sent_to.entity_name):
                    # log.info(f'{msg=} {parsed=}')
                    try:
                        payload: dict = generate_payload(session, msg_group=msg_group, predict_payload=parsed)
                    except PayloadGenerationError as e:
                        # raise SioValidationError(
                        #     sio=self.context.sio,
                        #     sid=sid,
                        #     event=SioEvents.chat_predict.value,
                        #     error=str(e),
                        #     stream_id=parsed.conversation_uuid,
                        #     message_id=parsed.question_id,
                        # )
                        log.error(e)
                        self.context.sio.emit(
                            event=SioEvents.socket_validation_error.value,
                            data={
                                'event': SioEvents.chat_predict.value,
                                'content': str(e),
                                'type': 'error',
                                'stream_id': parsed.conversation_uuid,
                                'message_id': parsed.question_id
                            },
                            room=room
                        )
                        return {"error": str(e)}
                    # log.info(f'{payload=}')

                    chat_history_groups, summaries, preserve_instructions = prepare_conversation_history(
                        session, self.context.sio,
                        conversation, msg_group
                    )
                    if not preserve_instructions:
                        payload['instructions'] = None

                    payload['chat_history'] = generate_chat_history(
                        message_groups=chat_history_groups, summaries=summaries
                    )
                    log.debug(f'chat {payload["chat_history"]=}')

                    context_meta = {
                        'context': {
                            'token_count': None,  # Will be calculated when stream ends
                            'weight': 1.0,
                            'included': True,
                            'priority': 1.0,
                            'created_at': datetime.now(tz=timezone.utc).isoformat()
                        }
                    }
                    if context_management_enabled:
                        context_meta['chat_history_group_ids'] = [g.id for g in chat_history_groups]
                    response_msg.meta = context_meta if context_management_enabled else {}
                    session.commit()

                    payload['message_id'] = str(response_msg.uuid)

                    # log.info(f'chat2 {payload=}')

                    # returns result only for applications
                    result = getattr(self.context.rpc_manager.call, rpc_func)(
                        sid, payload, SioEvents.chat_predict.value,
                        start_event_content={
                            'participant_id': msg_group.sent_to_id,
                            'question_id': str(msg_group.uuid),
                        },
                        chat_project_id=parsed.project_id,
                        await_task_timeout=await_task_timeout,
                        user_id=current_user['id']
                    )

                    if return_message_ids:
                        session.refresh(msg_group)
                        session.refresh(response_msg)
                        return {
                            "request_message_group_id": msg_group.id,
                            "response_message_group_id": response_msg.id,
                        }
                    return result

    @web.rpc("chat_continue_predict_sio", "chat_continue_predict_sio")
    def continue_predict_sio(
        self, sid: str | None, data: dict, await_task_timeout: int = -1
    ) -> Optional[str | dict]:
        """
        Continue execution of a paused chat prediction (e.g., after MCP OAuth interruption).
        
        This is a separate endpoint from chat_predict_sio because the "Continue" flow:
        - Uses existing messages instead of creating new ones
        - Has different validation requirements (message_id required, user_input not needed)
        - Shares minimal logic with the normal predict flow
        """
        log.debug(f'Continue predict: received data: user_input={data.get("user_input")}')
        try:
            parsed = SioContinuePredictModel.model_validate(data)
            log.debug(f'Continue predict: parsed model: user_input={parsed.user_input}')
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_predict.value,
                error=e.errors(),
                stream_id=data.get("conversation_uuid"),
                message_id=data.get("message_id"),
            )

        if sid:
            current_user = auth.current_user(auth_data=auth.sio_users[sid])
        else:
            current_user = auth.current_user()

        with db.get_session(parsed.project_id) as session:
            conversation: Conversation = session.query(Conversation).where(
                Conversation.uuid == parsed.conversation_uuid
            ).first()
            
            if not conversation:
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=SioEvents.chat_predict.value,
                    error=f"Conversation {parsed.conversation_uuid} not found",
                    stream_id=str(parsed.conversation_uuid),
                    message_id=parsed.message_id,
                )

            log.debug(f'Continue: Looking up message with id {parsed.message_id}')
            
            # Find the existing response message that was paused
            response_msg: ConversationMessageGroup = session.query(ConversationMessageGroup).options(
                joinedload(ConversationMessageGroup.author_participant)
            ).filter(
                ConversationMessageGroup.uuid == parsed.message_id
            ).first()

            if not response_msg:
                log.warning(f'Continue: No message found with id {parsed.message_id}')
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=SioEvents.chat_predict.value,
                    error=f"No message found with id {parsed.message_id}",
                    stream_id=str(parsed.conversation_uuid),
                    message_id=parsed.message_id,
                )

            log.debug(f'Continue: Found message {response_msg.uuid}, reply_to_id={response_msg.reply_to_id}, author_participant_id={response_msg.author_participant_id}')

            # Get the question message (reply_to)
            msg_group = None
            if response_msg.reply_to_id:
                msg_group = session.query(ConversationMessageGroup).filter(
                    ConversationMessageGroup.id == response_msg.reply_to_id
                ).first()

            if not msg_group:
                # If no reply_to, this might be the question message - try to find the response
                log.warning(f'Continue: Message {parsed.message_id} has no reply_to, checking if it is a question message')
                actual_response = session.query(ConversationMessageGroup).options(
                    joinedload(ConversationMessageGroup.author_participant)
                ).filter(
                    ConversationMessageGroup.reply_to_id == response_msg.id
                ).first()

                if actual_response:
                    log.debug(f'Continue: Found actual response message {actual_response.uuid}')
                    msg_group = response_msg  # The "response_msg" is actually the question
                    response_msg = actual_response
                else:
                    log.warning(f'Continue: Message {parsed.message_id} has no reply_to and no responses found')
                    raise SioValidationError(
                        sio=self.context.sio,
                        sid=sid,
                        event=SioEvents.chat_predict.value,
                        error=f"Message {parsed.message_id} has no associated question message",
                        stream_id=str(parsed.conversation_uuid),
                        message_id=parsed.message_id,
                    )

            # Set streaming back to true
            response_msg.is_streaming = True
            session.commit()

            rpc_func = CHAT_PREDICT_MAPPER.get(response_msg.author_participant.entity_name)
            if not rpc_func:
                # Reset streaming flag since we're erroring out
                response_msg.is_streaming = False
                session.commit()
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=SioEvents.chat_predict.value,
                    error=f"No RPC function found for participant type: {response_msg.author_participant.entity_name}",
                    stream_id=str(parsed.conversation_uuid),
                    message_id=parsed.message_id,
                )

            try:
                payload: dict = generate_payload(session, msg_group=msg_group, predict_payload=parsed)
            except PayloadGenerationError as e:
                log.error(e)
                # Reset streaming flag since we're erroring out
                response_msg.is_streaming = False
                session.commit()
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=SioEvents.chat_predict.value,
                    error=str(e),
                    stream_id=str(parsed.conversation_uuid),
                    message_id=parsed.message_id,
                )

            try:
                # Use thread_id from payload or fall back to existing message meta
                if not payload.get('thread_id') and response_msg.meta:
                    payload['thread_id'] = response_msg.meta.get('thread_id')
                
                # Override with explicit thread_id from request if provided
                if parsed.thread_id:
                    payload['thread_id'] = parsed.thread_id

                chat_history_groups, summaries, preserve_instructions = prepare_conversation_history(
                    session, self.context.sio, conversation, msg_group
                )
                if not preserve_instructions:
                    payload['instructions'] = None

                payload['chat_history'] = generate_chat_history(
                    message_groups=chat_history_groups, summaries=summaries
                )
                payload['message_id'] = str(response_msg.uuid)

                context_management_enabled = get_context_manager_feature_flag(parsed.project_id)
                if context_management_enabled and response_msg.meta is not None:
                    response_msg.meta['chat_history_group_ids'] = [g.id for g in chat_history_groups]
                    flag_modified(response_msg, 'meta')
                    session.add(response_msg)
                    session.commit()

                result = getattr(self.context.rpc_manager.call, rpc_func)(
                    sid, payload, SioEvents.chat_predict.value,
                    start_event_content={
                        'participant_id': response_msg.author_participant_id,
                        'question_id': str(msg_group.uuid),
                    },
                    chat_project_id=parsed.project_id,
                    await_task_timeout=await_task_timeout,
                    user_id=current_user['id']
                )
                return result
            except Exception as e:
                # Reset streaming flag on any unexpected error
                log.error(f"Error during continue predict: {e}")
                response_msg.is_streaming = False
                session.commit()
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=SioEvents.chat_predict.value,
                    error=f"Continue execution failed: {str(e)}",
                    stream_id=str(parsed.conversation_uuid),
                    message_id=parsed.message_id,
                )

    @web.rpc(f'chat_predict_summary_content', "predict_summary_content")
    def predict_summary_content(
            self,
            project_id: int,
            message_groups: list[ConversationMessageGroup],
            llm_settings: dict | None = None,
            summary_instructions: str | None = None,
            conversation_uuid: str | None = None,
            user_id: int | None = None,
    ) -> dict:
        """
        Predict summary content based on message groups using LLM

        Args:
            project_id: The project ID
            message_groups: List of ConversationMessageGroup ORM objects
            llm_settings: LLM configuration settings
            summary_instructions: Custom instructions for summary generation
            conversation_uuid: The conversation UUID for socket room
            user_id: The user ID
        Returns:
            Dict containing the prediction result
        """
        try:
            # Generate the payload for LLM prediction
            payload = generate_summary_payload(
                project_id=project_id,
                message_groups=message_groups,
                llm_settings=llm_settings,
                summary_instructions=summary_instructions,
            )

            if not message_groups:
                raise Exception(f"No message groups found for summary generation")

            # Make the LLM prediction call
            log.debug(f'chat generate_summary_payload {payload=}')

            try:
                client = self.get_redis_client()

                conversation_locked_key: str = get_conversation_locked_key(project_id, conversation_uuid)

                client.set(conversation_locked_key, 'true')
                client.expire(conversation_locked_key, timedelta(seconds=SUMMARIZATION_LOCKING_TTL))

                try:
                    result = getattr(self.context.rpc_manager.call, CHAT_PREDICT_MAPPER[ParticipantTypes.llm])(
                        None, payload, SioEvents.chat_predict.value,
                        start_event_content={},
                        chat_project_id=project_id,
                        await_task_timeout=SUMMARIZATION_LOCKING_TTL,
                        user_id=user_id,
                        is_system_user=True
                    )
                except Exception:
                    log.exception("Error during RPC call to LLM model in predict_summary_content")
                finally:
                    client.set(conversation_locked_key, 'false')

            except redis.exceptions.ConnectionError as e:
                log.error(f"Redis connection error: {e}")
                return

            log.debug(f"Summary prediction completed for project {project_id}")
            return result

        except Exception as e:
            log.error(f"Error in predict_summary_content: {str(e)}")
            raise Exception(f"Failed to generate summary: {str(e)}")


    @web.rpc(f'chat_get_conversation_count', "get_conversation_count")
    def chat_get_conversation_count(self, project_id: int, **kwargs) -> int:
        with db.with_project_schema_session(project_id) as session:
            return session.query(Conversation).count()

    @web.rpc(f'chat_delete_entity_in_all_conversations', 'delete_entity_in_all_conversations')
    def chat_delete_entity_in_all_conversations(self, project_id: int, entity_name: str, entity_meta: dict):
        entity_name = ParticipantTypes(entity_name)
        entity_meta = entity_meta_mapping[entity_name](**entity_meta)
        delete_entity_from_all_conversations(
            project_id, entity_name, entity_meta)

    @web.rpc("chat_get_stats", "chat_get_stats")
    def get_stats(self, project_id: int, author_id: int):
        result = {}
        with db.with_project_schema_session(project_id) as session:
            query = session.query(Conversation).filter(
                Conversation.author_id == author_id
            )
            result['total_conversations'] = query.count()
            query = query.filter(
                Conversation.is_private == False
            )
            result['public_conversations'] = query.count()
        return result

    @web.rpc("chat_get_message_group_model")
    def get_conversation_message_group_model(self):
        return ConversationMessageGroup
