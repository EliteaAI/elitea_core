from sqlalchemy import and_, or_, asc, desc, Integer, Float, func, case, cast, TIMESTAMP, Text
from sqlalchemy.orm import joinedload, selectinload, Session
from sqlalchemy.dialects.postgresql import JSONB

from tools import rpc_tools, this
from pylon.core.tools import log

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.base import MessageItem
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationDetailsOrm, ConversationDetails
from ..utils.authors import get_authors_data
from ..utils.meta_guard import (
    META_SIZE_LIMIT_BYTES, RESPONSE_META_BUDGET_BYTES,
    meta_bytes_expr, safe_meta_expr, strip_heavy_meta_keys,
)

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


def calculate_conversation_durations_batch(
    conversation_ids: list[int],
    session: Session,
) -> dict[int, float]:
    """
    Batched variant of calculate_conversation_duration: returns
    {conversation_id: duration_seconds} in a single GROUP BY query.

    Used by chat_list_conversations_rpc to avoid one query per row.
    """
    if not conversation_ids or session is None:
        return {}

    duration_expression = case(
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
        (
            and_(
                ConversationMessageGroup.meta['execution_time_seconds'].isnot(None),
                ConversationMessageGroup.meta['execution_time_seconds'].astext.isnot(None)
            ),
            cast(ConversationMessageGroup.meta['execution_time_seconds'].astext, Float)
        ),
        else_=func.extract(
            'epoch',
            func.coalesce(ConversationMessageGroup.updated_at, ConversationMessageGroup.created_at) - ConversationMessageGroup.created_at
        )
    )

    rows = session.query(
        ConversationMessageGroup.conversation_id,
        func.coalesce(func.sum(duration_expression), 0.0),
    ).filter(
        ConversationMessageGroup.conversation_id.in_(conversation_ids),
        ConversationMessageGroup.reply_to_id.isnot(None),
    ).group_by(
        ConversationMessageGroup.conversation_id,
    ).all()

    return {cid: round(float(d or 0.0), 2) for cid, d in rows}


MESSAGES_LIMIT_HARD_CAP: int = 100


# Column list selected for every message-group read. meta is replaced by the server-side safe_meta
# expression so an oversized blob is stripped inside Postgres and never crosses the wire / hits the
# gevent hub; meta_bytes carries the real decompressed size for the cumulative budget below.
def _message_group_columns():
    return [
        ConversationMessageGroup.id,
        ConversationMessageGroup.uuid,
        ConversationMessageGroup.author_participant_id,
        ConversationMessageGroup.created_at,
        ConversationMessageGroup.updated_at,
        ConversationMessageGroup.reply_to_id,
        ConversationMessageGroup.is_streaming,
        ConversationMessageGroup.task_id,
        ConversationMessageGroup.sent_to_id,
        safe_meta_expr(ConversationMessageGroup.meta).label('meta'),
        meta_bytes_expr(ConversationMessageGroup.meta).label('meta_bytes'),
    ]


def fetch_guarded_message_groups(session, rows, log_label: str = 'message_groups') -> list[dict]:
    """Build guarded message-group dicts from rows selected via _message_group_columns().

    Applies the cumulative per-response budget (per-group oversize is already stripped in SQL),
    loads message_items ordered by order_index, and resolves sent_to participants — shared by every
    read path so the stall guard and item ordering stay consistent.
    """
    group_ids = [r.id for r in rows]
    items_by_group = {}
    sent_to_by_id = {}
    if group_ids:
        all_items = session.query(MessageItem).filter(
            MessageItem.message_group_id.in_(group_ids),
            MessageItem.item_type != 'context_message',
        ).order_by(MessageItem.order_index.asc(), MessageItem.id.asc()).all()
        for item in all_items:
            items_by_group.setdefault(item.message_group_id, []).append(item)
        sent_to_ids = {r.sent_to_id for r in rows if r.sent_to_id is not None}
        if sent_to_ids:
            for p in session.query(Participant).filter(Participant.id.in_(sent_to_ids)).all():
                sent_to_by_id[p.id] = p
    group_dicts = []
    cumulative_meta_bytes = 0
    for r in rows:
        meta = r.meta or {}
        group_bytes = r.meta_bytes or 0
        if meta.get('_oversized'):
            # heavy keys already stripped in SQL; stripped group is tiny, don't charge the budget
            log.warning('%s: group %s meta %.1f MB > %d MB — stripped', log_label, r.id,
                        group_bytes / 1024 / 1024, META_SIZE_LIMIT_BYTES // 1024 // 1024)
            meta = strip_heavy_meta_keys(meta)
        elif cumulative_meta_bytes + group_bytes > RESPONSE_META_BUDGET_BYTES:
            # individually fine but the cumulative response would freeze the hub
            log.warning('%s: group %s trimmed — response budget %d MB exhausted', log_label, r.id,
                        RESPONSE_META_BUDGET_BYTES // 1024 // 1024)
            meta = strip_heavy_meta_keys(meta)
        else:
            cumulative_meta_bytes += group_bytes
        group_dicts.append({
            'id': r.id,
            'uuid': r.uuid,
            'author_participant_id': r.author_participant_id,
            'created_at': r.created_at,
            'updated_at': r.updated_at,
            'reply_to_id': r.reply_to_id,
            'is_streaming': r.is_streaming,
            'task_id': r.task_id,
            'sent_to_id': r.sent_to_id,
            'sent_to': sent_to_by_id.get(r.sent_to_id),
            'meta': meta,
            'message_items': items_by_group.get(r.id, []),
        })
    return group_dicts


def get_conversation_details(
    session,
    conversation_id: int,
    project_id: int,
    user_id: int = None,
    check_ownership: bool = True,
    messages_limit: int = MESSAGES_DISPLAY_COUNT,
    messages_offset: int = 0,
    sort_order: str = 'acs',
) -> ConversationDetails | None:
    messages_limit = min(messages_limit, MESSAGES_LIMIT_HARD_CAP)
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

    if check_ownership:
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
            selectinload(Conversation.participants)
        ).filter(
            Conversation.id == conversation_id,
            or_(
                Conversation.is_private == False,
                Conversation.id.in_(private_conversation_subquery)
            )
        ).first()
    else:
        conversation = session.query(Conversation).options(
            selectinload(Conversation.participants)
        ).filter(
            Conversation.id == conversation_id
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
    conversation_dict['message_groups_count'] = conversation.message_groups.count()
    order_func = desc if sort_order == 'desc' else asc
    rows = (
        session.query(*_message_group_columns())
        .filter(ConversationMessageGroup.conversation_id == conversation.id)
        .order_by(order_func(ConversationMessageGroup.created_at))
        .offset(messages_offset)
        .limit(messages_limit)
        .all()
    )
    conversation_dict['message_groups'] = fetch_guarded_message_groups(
        session, rows, log_label=f'conversation_details conv {conversation_id}')

    # Batch author lookups for all user participants in one call instead of
    # one get_authors_data() round-trip per participant (audit #10).
    user_author_ids = [
        participant['entity_meta']['id']
        for participant in conversation_dict['participants']
        if participant['entity_name'] == ParticipantTypes.user.value
    ]
    authors_by_id = {
        author['id']: author
        for author in (get_authors_data(user_author_ids) if user_author_ids else [])
    }

    for participant in conversation_dict['participants']:
        # todo: add project_id to every participant
        participant['entity_settings'] = entity_settings_dict.get(participant['id'], {})
        if participant['entity_name'] == ParticipantTypes.user.value:
            author = authors_by_id.get(participant['entity_meta']['id'])
            if author:
                participant['meta']['user_name'] = author.get('name')
                participant['meta']['user_avatar'] = author.get('avatar')
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
