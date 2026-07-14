from sqlalchemy import and_, or_, asc, desc, Integer, Float, func, case, cast
from sqlalchemy.orm import joinedload, selectinload, Session

from tools import rpc_tools, this
from pylon.core.tools import log

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes, AgentTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_trace_step import MessageTraceStep
from ..models.message_items.base import MessageItem
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationDetailsOrm, ConversationDetails
from ..utils.authors import get_authors_data
from ..utils.meta_guard import strip_heavy_meta_expr

MESSAGES_DISPLAY_COUNT: int = 100


def _thinking_span_subquery(session: Session, conversation_ids: list[int]):
    """Per-group thinking wall-clock (secs) from message_trace_step: max(finished) - min(started).

    Replaces the old meta['thinking_steps'][0/-1] timestamp math (steps now live in the table).
    Steps accumulate chronologically so min/max match the former first-start -> last-finish span.
    Scoped to the target conversations (join message_group inside the aggregation) so the GROUP BY
    only touches relevant groups, not the whole tenant schema.
    """
    return (
        session.query(
            MessageTraceStep.message_group_id.label('mg_id'),
            func.extract(
                'epoch',
                func.max(MessageTraceStep.finished_at) - func.min(MessageTraceStep.started_at),
            ).label('secs'),
        )
        .join(
            ConversationMessageGroup,
            ConversationMessageGroup.id == MessageTraceStep.message_group_id,
        )
        .filter(
            ConversationMessageGroup.conversation_id.in_(conversation_ids),
            MessageTraceStep.kind == 'thinking_step',
            MessageTraceStep.started_at.isnot(None),
            MessageTraceStep.finished_at.isnot(None),
        )
        .group_by(MessageTraceStep.message_group_id)
        .subquery()
    )


def _duration_expression(span):
    """Duration case over the thinking-span subquery, in priority order.

    1. Agent/LLM run: thinking span from message_trace_step (full wall-clock, chat "Thought for X").
    2. Toolkit-only: meta['execution_time_seconds'] (standalone toolkit testing, no thinking steps).
    3. Fallback: updated_at - created_at.
    """
    return case(
        (span.c.secs.isnot(None), span.c.secs),
        (
            and_(
                ConversationMessageGroup.meta['execution_time_seconds'].isnot(None),
                ConversationMessageGroup.meta['execution_time_seconds'].astext.isnot(None),
            ),
            cast(ConversationMessageGroup.meta['execution_time_seconds'].astext, Float),
        ),
        else_=func.extract(
            'epoch',
            func.coalesce(ConversationMessageGroup.updated_at, ConversationMessageGroup.created_at)
            - ConversationMessageGroup.created_at,
        ),
    )


def calculate_conversation_durations_batch(
    conversation_ids: list[int],
    session: Session,
) -> dict[int, float]:
    """Return {conversation_id: duration_seconds} in a single GROUP BY query.

    Used by chat_list_conversations_rpc to avoid one query per row. See _duration_expression.
    """
    if not conversation_ids or session is None:
        return {}

    span = _thinking_span_subquery(session, conversation_ids)
    rows = (
        session.query(
            ConversationMessageGroup.conversation_id,
            func.coalesce(func.sum(_duration_expression(span)), 0.0),
        )
        .select_from(ConversationMessageGroup)
        .outerjoin(span, span.c.mg_id == ConversationMessageGroup.id)
        .filter(
            ConversationMessageGroup.conversation_id.in_(conversation_ids),
            ConversationMessageGroup.reply_to_id.isnot(None),
        )
        .group_by(ConversationMessageGroup.conversation_id)
        .all()
    )

    return {cid: round(float(d or 0.0), 2) for cid, d in rows}


MESSAGES_LIMIT_HARD_CAP: int = 100


# Column list selected for every message-group read. meta has tool_calls / thinking_steps stripped
# in Postgres (strip_heavy_meta_expr) so an old group's monolithic blob is never detoasted onto the
# gevent hub. New groups never carry those keys; steps are served from message_trace_step instead.
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
        strip_heavy_meta_expr(ConversationMessageGroup.meta).label('meta'),
    ]


def fetch_guarded_message_groups(session, rows, log_label: str = 'message_groups') -> list[dict]:
    """Build message-group dicts from rows selected via _message_group_columns().

    Loads message_items ordered by order_index and resolves sent_to participants — shared by every
    read path so item ordering stays consistent. The heavy meta keys are already stripped in SQL.
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
    for r in rows:
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
            'meta': r.meta or {},
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
        # `created_at` uses Postgres `now()` (transaction-scoped), so message groups inserted
        # in the same transaction share a timestamp. Add `id` as tiebreaker so trigger-run
        # pairs render in insertion order rather than implementation-defined order. Issue #5081.
        .order_by(
            order_func(ConversationMessageGroup.created_at),
            order_func(ConversationMessageGroup.id),
        )
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
            version_details = application_version_details['version_details']
            participant['meta']['tools'] = version_details['tools']
            # "Container" flag: a non-pipeline agent that itself uses other agents (has an
            # application-type tool). NOTE (issue #5778): a container is NO LONGER unconditionally
            # skipped as an adhoc chat tool — a tier-2 container is now legal. The adhoc skip is
            # depth-aware (rpc/chat_all.generate_toolkit_payload skips only when the bound subtree
            # exceeds the tier budget). This flag is retained as a factual "uses other agents"
            # signal for the participant chip; consumers deciding whether it can be nested should
            # use the version_details.agent_subtree_tiers contribution field, not this boolean.
            # Pipelines are the sanctioned deep-composition primitive and are never flagged.
            participant['meta']['is_container'] = (
                version_details.get('agent_type') != AgentTypes.pipeline.value
                and any(
                    (t or {}).get('type') == 'application'
                    for t in (version_details.get('tools') or [])
                )
            )
            # Agent-only subtree contribution (issue #5778); a pipeline participant contributes
            # zero for itself. The chat UI participant gate uses it to decide whether a
            # container can still be nested (host current tier + candidate contribution within
            # MAX_AGENT_NESTING_TIERS) instead of the blunt is_container ban —
            # mirrors application_utils.get_application_version_details_expanded so
            # the two can't drift. Advisory: never fail conversation fetch over it,
            # and the UI degrades gracefully (defers to backend) when absent.
            try:
                from .publish_utils import (
                    compute_agent_subtree_tiers,
                    MAX_AGENT_NESTING_TIERS,
                )
                participant['meta']['agent_subtree_tiers'] = compute_agent_subtree_tiers(
                    participant['entity_meta']['project_id'],
                    participant['entity_settings']['version_id'],
                    session=(
                        session
                        if participant['entity_meta']['project_id'] == project_id
                        else None
                    ),
                )
                participant['meta']['max_agent_nesting_tiers'] = MAX_AGENT_NESTING_TIERS
            except Exception as depth_err:
                log.warning(
                    "Could not compute agent_subtree_tiers for participant version "
                    f"{participant['entity_settings'].get('version_id')}: {depth_err}"
                )

    return ConversationDetails.model_validate(conversation_dict)



def get_conversation_locked_key(project_id: int, conversation_uuid: str) -> str:
    """
    Generates a unique key for identifying a conversation editing based on a project ID and
    a conversation UUID.
    """
    return f"conversation_locked:{project_id}_{conversation_uuid}"
