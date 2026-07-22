"""
Shared utilities for Pipeline Execution.

Contains helpers for:
- Trigger-based conversation creation (scheduled, webhook)
- Pipeline execution via predict_sio

Both scheduled and webhook pipeline triggers use identical patterns for:
- Creating conversations with proper History tab tracking
- Setting up participants and message groups
- Calling applications_predict_sio with the same parameters

This module consolidates that shared logic to eliminate duplication.
"""
from typing import Optional

from pylon.core.tools import log
from tools import db, rpc_tools

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.pd.participant import ParticipantEntityApplication, ParticipantEntityUser
from ..utils.participant_utils import get_or_create_one
from ..utils.sio_utils import SioEvents


class TriggerType:
    """Trigger type constants for conversation metadata."""
    SCHEDULED = "scheduled"
    WEBHOOK = "webhook"


def create_trigger_run_conversation(
    project_id: int,
    version: ApplicationVersion,
    user_id: int,
    trigger_type: str,
    trigger_message: str,
    conversation_name: str,
    extra_meta: Optional[dict] = None,
) -> tuple[str, int, str]:
    """
    Create a conversation for a triggered pipeline run (scheduled or webhook).

    This ensures the run appears in the History tab, matching the pattern
    used by UI-initiated pipeline runs.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        user_id: User ID for execution context (trigger creator)
        trigger_type: Type of trigger ("scheduled" or "webhook")
        trigger_message: Initial message content showing trigger info
        conversation_name: Name for the conversation (e.g., "Scheduled run: Pipeline Name")
        extra_meta: Additional metadata to include in conversation.meta

    Returns:
        Tuple of (conversation UUID string, pipeline participant ID, response message UUID)
    """
    with db.get_session(project_id) as session:
        # Get application name for context
        application = session.query(Application).get(version.application_id)
        pipeline_name = application.name if application else f"Pipeline {version.application_id}"

        # Build single_participant metadata (same as UI creates)
        single_participant = {
            "entity_name": ParticipantTypes.application.value,
            "entity_meta": {
                "id": version.application_id,
                "project_id": project_id,
            },
            "entity_settings": {
                "version_id": version.id,
            },
        }

        # Build conversation metadata
        conversation_meta = {
            "single_participant": single_participant,
        }
        if trigger_type == TriggerType.SCHEDULED:
            conversation_meta["scheduled_run"] = True
        elif trigger_type == TriggerType.WEBHOOK:
            conversation_meta["webhook_trigger"] = True

        if extra_meta:
            conversation_meta.update(extra_meta)

        # Create conversation with proper metadata for History tab
        conversation = Conversation(
            name=conversation_name or f"{trigger_type.capitalize()} run: {pipeline_name}",
            source="pipeline",
            author_id=user_id,
            is_private=True,
            meta=conversation_meta,
        )
        session.add(conversation)
        session.flush()

        # Get or create user participant
        user_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=user_id),
        )
        if user_participant not in conversation.participants:
            conversation.participants.append(user_participant)

        # Get or create pipeline participant
        pipeline_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.application,
            entity_meta=ParticipantEntityApplication(
                id=version.application_id,
                project_id=project_id,
            ),
        )
        if pipeline_participant not in conversation.participants:
            conversation.participants.append(pipeline_participant)

        session.flush()

        # Update entity_settings for pipeline participant mapping
        from ..models.participants import ParticipantMapping
        session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation.id,
            ParticipantMapping.participant_id == pipeline_participant.id
        ).update({'entity_settings': {'version_id': version.id}})

        # Build message metadata
        msg_meta = {}
        if trigger_type == TriggerType.SCHEDULED:
            msg_meta["scheduled_trigger"] = True
        elif trigger_type == TriggerType.WEBHOOK:
            msg_meta["webhook_trigger"] = True
            if extra_meta and "webhook_type" in extra_meta:
                msg_meta["webhook_type"] = extra_meta["webhook_type"]

        # Create initial user message (trigger notification)
        user_msg_group = ConversationMessageGroup(
            conversation=conversation,
            author_participant=user_participant,
            sent_to_id=pipeline_participant.id,
            meta=msg_meta,
        )
        user_msg = TextMessageItem(
            message_group=user_msg_group,
            item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
            content=trigger_message,
            order_index=0,
        )
        session.add(user_msg_group)
        session.add(user_msg)
        session.flush()

        # Create response message placeholder (will be filled by pipeline execution)
        response_msg_group = ConversationMessageGroup(
            conversation=conversation,
            author_participant_id=pipeline_participant.id,
            is_streaming=True,
            reply_to_id=user_msg_group.id,
        )
        session.add(response_msg_group)
        session.flush()

        session.commit()

        log.debug(
            f"Created conversation for {trigger_type} pipeline run: "
            f"conversation_id={conversation.id}, uuid={conversation.uuid}, "
            f"response_id={response_msg_group.uuid}"
        )
        return str(conversation.uuid), pipeline_participant.id, str(response_msg_group.uuid)


def execute_pipeline_via_predict_sio(
    project_id: int,
    version: ApplicationVersion,
    user_id: int,
    conversation_uuid: str,
    response_message_id: str,
    user_input: str = "",
) -> dict:
    """
    Execute a pipeline using the applications_predict_sio RPC.

    This is the shared execution path for both scheduled and webhook triggers.
    The conversation and messages should already be created before calling this.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        user_id: User ID for execution context
        conversation_uuid: UUID of the conversation (used as stream_id)
        response_message_id: UUID of the response message group
        user_input: Input to send to the pipeline (empty for scheduled, payload for webhook)

    Returns:
        Dict with task_id from the RPC call
    """
    result = rpc_tools.RpcMixin().rpc.timeout(5).applications_predict_sio(
        sid=None,  # No socket connection for triggered runs
        sio_event=SioEvents.chat_predict.value,  # Required for metadata persistence
        chat_project_id=project_id,  # Required for correct DB schema routing
        data={
            "project_id": project_id,
            "application_id": version.application_id,
            "version_id": version.id,
            "user_input": user_input,
            "chat_history": [],
            "message_id": response_message_id,
            "stream_id": conversation_uuid,
            "conversation_id": conversation_uuid,
        },
        await_task_timeout=-1,  # Don't wait for completion
        user_id=user_id,
        is_system_user=True,
        # Scheduled & webhook-triggered runs have no live UI consumer; suppress
        # streaming/UI-only events. DB persistence (full/partial_message) and
        # index/HITL/error state events are preserved. Redundant with the
        # sid-is-None auto-derive (sid=None here), but kept to document intent.
        non_interactive=True,
    )

    log.debug(
        f"Pipeline execution started: task_id={result.get('task_id')}, "
        f"conversation={conversation_uuid}"
    )
    return result
