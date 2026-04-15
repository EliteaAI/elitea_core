from sqlalchemy import and_, or_, desc, Integer, Float, func, case, cast, TIMESTAMP
from sqlalchemy.orm import joinedload, Session
from sqlalchemy.dialects.postgresql import JSONB

from tools import rpc_tools, this
from pylon.core.tools import log

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationDetailsOrm, ConversationDetails
from ..utils.authors import get_authors_data

MESSAGES_DISPLAY_COUNT: int = 100


def calculate_conversation_duration(conversation: Conversation, session: Session) -> float:
    """
    Calculate the sum of message durations for a conversation.

    The duration calculation has three cases (in priority order):
    1. Agent/LLM execution: Uses first_tool_timestamp_start and last thinking_step's timestamp_finish
    2. Toolkit-only execution: Uses execution_time_seconds stored in meta (for standalone toolkit testing)
    3. Fallback: Uses updated_at - created_at (for messages without timing metadata)

    Returns:
        Total duration in seconds (float)
    """
    if session is None:
        return 0.0

    duration_expression = case(
        # Case 1: Agent/LLM execution with thinking_steps (most accurate for agent runs)
        (
            and_(
                ConversationMessageGroup.meta['first_tool_timestamp_start'].isnot(None),
                func.jsonb_array_length(
                    func.coalesce(ConversationMessageGroup.meta['thinking_steps'], cast('[]', JSONB))
                ) > 0
            ),
            func.extract(
                'epoch',
                cast(
                    (ConversationMessageGroup.meta['thinking_steps'][-1]['timestamp_finish']).astext,
                    TIMESTAMP
                ) - cast(
                    ConversationMessageGroup.meta['first_tool_timestamp_start'].astext,
                    TIMESTAMP
                )
            )
        ),
        # Case 2: Toolkit-only execution (uses execution_time_seconds from SDK response)
        # This handles standalone toolkit testing where there's no LLM/thinking_steps
        # Check both JSONB key existence (->  IS NOT NULL) and text value (-->> IS NOT NULL)
        # to avoid matching JSON null values which would return SQL NULL after cast
        (
            and_(
                ConversationMessageGroup.meta['execution_time_seconds'].isnot(None),
                ConversationMessageGroup.meta['execution_time_seconds'].astext.isnot(None)
            ),
            cast(ConversationMessageGroup.meta['execution_time_seconds'].astext, Float)
        ),
        # Case 3: Fallback to updated_at - created_at
        else_=func.extract(
            'epoch',
            func.coalesce(ConversationMessageGroup.updated_at, ConversationMessageGroup.created_at) - ConversationMessageGroup.created_at
        )
    )

    result = session.query(
        func.coalesce(func.sum(duration_expression), 0.0)
    ).filter(
        ConversationMessageGroup.conversation_id == conversation.id,
        ConversationMessageGroup.reply_to_id.isnot(None)
    ).scalar()

    return round(float(result or 0.0), 2)


def get_conversation_details(session, conversation_id: int, project_id: int, user_id: int = None) -> ConversationDetails | None:
    # filter participants based on entity_meta['id']
    conversation = session.query(Conversation).filter(
        Conversation.id == conversation_id
    ).first()

    if not conversation:
        return None

    participant_subquery_filters = [Participant.entity_name == ParticipantTypes.user.value]
    user_is_admin: bool = rpc_tools.RpcMixin().rpc.timeout(3).admin_check_user_is_admin(project_id, user_id)

    if not conversation.meta.get('single_participant') or not user_is_admin:
        participant_subquery_filters.append(
            Participant.entity_meta['id'].astext.cast(Integer) == user_id,
        )

    participant_subquery = session.query(Participant.id).filter(
        *participant_subquery_filters
    ).subquery()

    # filter conversations that are private and authored
    # by the user (based on participant metadata)
    private_conversation_subquery = session.query(Conversation.id).join(
        ParticipantMapping,
        Conversation.id == ParticipantMapping.conversation_id
    ).filter(
        and_(
            Conversation.is_private == True,
            ParticipantMapping.participant_id.in_(participant_subquery)
        )
    ).subquery()

    conversation = session.query(Conversation).options(
        joinedload(Conversation.participants)
    ).filter(
        Conversation.id == conversation_id,
        or_(
            Conversation.is_private == False,
            Conversation.id.in_(private_conversation_subquery)
        )
    ).first()

    if not conversation:
        return None

    entity_settings_dict = {
        row[0]: row[1] for row in session.query(
            ParticipantMapping.participant_id,
            ParticipantMapping.entity_settings
        ).filter(
            ParticipantMapping.conversation_id == conversation_id
        ).all()
    }
    conversation_dict = ConversationDetailsOrm.model_validate(conversation).model_dump()
    message_groups = conversation.message_groups.options(
        joinedload(ConversationMessageGroup.message_items)
    )
    conversation_dict['message_groups_count'] = message_groups.count()
    conversation_dict['message_groups'] = message_groups.order_by(
        desc(ConversationMessageGroup.created_at)
    ).limit(
        MESSAGES_DISPLAY_COUNT
    ).all()

    for participant in conversation_dict['participants']:
        # todo: add project_id to every participant
        participant['entity_settings'] = entity_settings_dict.get(participant['id'], {})
        if participant['entity_name'] == ParticipantTypes.user.value:
            authors_data = get_authors_data([participant['entity_meta']['id']])
            if authors_data:
                participant['meta']['user_name'] = authors_data[0].get('name')
                participant['meta']['user_avatar'] = authors_data[0].get('avatar')
        if participant['entity_name'] == ParticipantTypes.toolkit.value:
            # For MCP toolkit participants, fetch the server URL if not present
            toolkit_type = participant['entity_settings'].get('toolkit_type')
            
            if toolkit_type == 'mcp' and not participant['entity_settings'].get('url'):
                try:
                    toolkit_details = this.module.get_toolkit_by_id(
                        project_id=participant['entity_meta']['project_id'],
                        toolkit_id=participant['entity_meta']['id'],
                    )
                    if toolkit_details:
                        mcp_url = toolkit_details.get('settings', {}).get('url')
                        if mcp_url:
                            participant['entity_settings']['url'] = mcp_url
                except Exception as e:
                    log.warning(f"Failed to fetch toolkit details for toolkit {participant['entity_meta']['id']}: {e}")
        if participant['entity_name'] == ParticipantTypes.application.value:
            application_version_details = this.module.get_application_by_version_id(
                project_id=participant['entity_meta']['project_id'],
                application_id=participant['entity_meta']['id'],
                version_id=participant['entity_settings']['version_id'],
            )
            if not application_version_details:
                log.warning(
                    f"Application with ID {participant['entity_meta']['id']} not found"
                )
                continue
            participant['meta']['tools'] = application_version_details['version_details']['tools']

    return ConversationDetails.model_validate(conversation_dict)



def get_conversation_locked_key(project_id: int, conversation_uuid: str) -> str:
    """
    Generates a unique key for identifying a conversation editing based on a project ID and
    a conversation UUID.
    """
    return f"conversation_locked:{project_id}_{conversation_uuid}"
