"""
Context analytics utilities.

All context analytics are stored in conversation.meta.context_analytics.
"""

from pylon.core.tools import log
from tools import db

from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.pd.context import ContextStrategy
from .context_window import (
    get_model_limits, is_platform_admin, legacy_context_tokens, manual_context_tokens, manual_context_tokens_allowed,
    normalize_mode, pick_chat_llm_settings, resolve_context_budget, status_max_tokens,
)


TOKEN_BREAKDOWN_KEYS = (
    'prompt_tokens',
    'token_count_user_facing',
    'system_tokens',
    'tool_schema_tokens',
    'tool_message_tokens',
    'overhead_tokens',
    'provider_input_tokens',
    'provider_output_tokens',
    'cache_read_tokens',
    'cache_creation_tokens',
    'token_source',
)

DEFAULT_TOKEN_SOURCE = 'approximate'


def extract_token_breakdown(context_info: dict | None) -> dict:
    """Pick the token breakdown fields out of the SDK's context_info.

    Older SDKs report only token_count / message_count / summarized; the keys
    they omit are simply absent from the result.
    """
    if not context_info:
        return {}
    return {
        key: context_info[key]
        for key in TOKEN_BREAKDOWN_KEYS
        if context_info.get(key) is not None
    }


def get_conversation_meta(project_id: int, conversation_id: int) -> dict:
    """Get conversation meta from database."""
    try:
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()
            return conversation.meta or {} if conversation else {}
    except Exception as e:
        log.warning(f"Failed to get conversation meta: {e}")
        return {}


def merge_meta_expression(meta_updates: dict):
    """Key-level JSONB merge, so a write never rewrites keys it did not pass from a stale read."""
    from sqlalchemy import literal  # pylint: disable=C0415
    from sqlalchemy.dialects.postgresql import JSONB  # pylint: disable=C0415
    return Conversation.meta.op('||')(literal(meta_updates, type_=JSONB))


def update_conversation_meta(project_id: int, conversation_id: int, meta_updates: dict) -> dict:
    """Merge top-level keys into conversation meta in one statement; other keys are untouched."""
    with db.get_session(project_id) as session:
        updated = session.query(Conversation).filter(
            Conversation.id == conversation_id
        ).update(
            {Conversation.meta: merge_meta_expression(meta_updates)},
            synchronize_session=False
        )
        if not updated:
            raise Exception(f"Conversation with ID {conversation_id} not found")
        session.commit()
        row = session.query(Conversation.meta).filter(Conversation.id == conversation_id).first()
        return (row[0] if row else None) or {}


def get_context_analytics(project_id: int, conversation_id: int) -> dict | None:
    """Get context analytics from conversation meta."""
    meta = get_conversation_meta(project_id, conversation_id)
    return meta.get('context_analytics')


def update_context_analytics_after_message_delete(
    project_id: int,
    conversation_id: int,
    session,
) -> None:
    """Recalculate and persist context analytics after a single message group is deleted.

    Finds the most recent remaining message group that has context snapshot data
    (token_count_in_context / message_count) and uses it to update the conversation-level
    analytics counters.  Preserves summarization history (summaries_generated, etc.) since
    that is a cumulative record and is unaffected by removing one message.
    Resets context_analytics to None when no snapshot-bearing message remains.
    """
    current_analytics = get_context_analytics(project_id, conversation_id) or {}

    preserved_count = current_analytics.get('messages_in_context') or 1

    remaining = (
        session.query(ConversationMessageGroup)
        .filter(ConversationMessageGroup.conversation_id == conversation_id)
        .order_by(ConversationMessageGroup.created_at.desc())
        .limit(preserved_count)
        .all()
    )

    ctx = None
    for mg in remaining:
        if mg.meta and mg.meta.get('context', {}).get('included') is False:
            break  # hit the summarized section — stop here
        if mg.meta and mg.meta.get('context', {}).get('token_count_in_context') is not None:
            ctx = mg.meta['context']
            break

    if ctx is not None:
        updated_analytics = {
            **current_analytics,
            'current_context_tokens': ctx.get('token_count_in_context', 0),
            'messages_in_context': ctx.get('message_count', 0),
            'token_breakdown': ctx.get('token_breakdown') or {},
        }
    else:
        updated_analytics = None

    update_conversation_meta(project_id, conversation_id, {'context_analytics': updated_analytics})


def _conversation_model_budget(project_id: int, conversation_id: int, mode: str) -> dict:
    """Budget before the first response: resolve from the chat model (the user participant's llm_settings)."""
    from ..models.participants import Participant, ParticipantMapping
    llm_settings = {}
    try:
        with db.get_session(project_id) as session:
            mappings = session.query(Participant.entity_name, ParticipantMapping.entity_settings).join(
                Participant, Participant.id == ParticipantMapping.participant_id
            ).filter(
                ParticipantMapping.conversation_id == conversation_id
            ).all()
        llm_settings = pick_chat_llm_settings(mappings)
    except Exception as e:
        log.warning(f"Failed to read conversation {conversation_id} model: {e}")
    limits = get_model_limits(llm_settings.get('model_project_id') or project_id, llm_settings.get('model_name'))
    max_tokens = llm_settings.get('max_tokens')
    return resolve_context_budget(
        mode, limits.get('context_window'), limits.get('max_output_tokens'),
        None if max_tokens in (None, -1) else max_tokens,
    )


def _manual_context_info(context_strategy: dict, user_id) -> dict | None:
    """Manual trigger details, shown to platform admins only while the platform toggle is on."""
    if not user_id or not manual_context_tokens_allowed() or not is_platform_admin(user_id):
        return None
    manual = manual_context_tokens(context_strategy)
    if manual and not is_platform_admin(context_strategy.get('manual_set_by')):
        manual = None
    return {'manual_max_tokens_enabled': True, 'manual_max_context_tokens': manual}


def get_context_data(
        project_id: int, conversation_id: int, user_id: int | None = None,
) -> tuple[dict | None, dict, str, dict | None, dict | None, int | None]:
    """Get context analytics, resolved budget, strategy name, chat usage, manual and legacy trigger info."""
    meta = get_conversation_meta(project_id, conversation_id)
    context_analytics = meta.get('context_analytics')
    context_strategy = meta.get('context_strategy') or {}
    mode = normalize_mode(context_strategy.get('context_window_mode'))
    budget = meta.get('context_budget')
    if not isinstance(budget, dict) or budget.get('window_mode') != mode:
        budget = _conversation_model_budget(project_id, conversation_id, mode)
    strategy_name = context_strategy.get('name', 'default')
    manual = _manual_context_info(context_strategy, user_id)
    legacy = legacy_context_tokens(context_strategy)
    return context_analytics, budget, strategy_name, meta.get('usage_totals'), manual, legacy


def build_context_response(
    stored_analytics: dict | None = None,
    budget: dict | None = None,
    strategy_name: str = 'default',
    usage_totals: dict | None = None,
    manual: dict | None = None,
    legacy_max_tokens: int | None = None,
) -> dict:
    """Build context response for UI."""
    budget = budget or resolve_context_budget(None, None)
    summary_count = stored_analytics.get('summaries_generated', 0) if stored_analytics else 0
    current_tokens = stored_analytics.get('current_context_tokens', 0) if stored_analytics else 0
    messages_in_context = stored_analytics.get('messages_in_context', 0) if stored_analytics else 0

    manual = manual or {'manual_max_tokens_enabled': False, 'manual_max_context_tokens': None}
    max_tokens = status_max_tokens(budget, manual['manual_max_context_tokens'], legacy_max_tokens)
    utilization = (current_tokens / max_tokens) if max_tokens > 0 else 0.0

    breakdown = (stored_analytics or {}).get('token_breakdown') or {}

    return {
        'current_tokens': current_tokens,
        'max_tokens': max_tokens,
        'utilization': round(utilization, 4),
        'message_groups_in_context': messages_in_context,
        'strategy_name': strategy_name,
        'summary_count': summary_count,
        'token_source': breakdown.get('token_source', DEFAULT_TOKEN_SOURCE),
        'token_breakdown': breakdown,
        **budget,
        **manual,
        'last_call': {
            'input_tokens': breakdown.get('provider_input_tokens'),
            'output_tokens': breakdown.get('provider_output_tokens'),
        },
        'chat_usage': usage_totals,
        'context_analytics': stored_analytics or {
            'summaries_generated': 0,
            'total_messages_summarized': 0,
            'current_context_tokens': 0,
            'messages_in_context': 0,
            'last_summarization': None,
        },
    }


def set_context_strategy(
    project_id: int,
    conversation_id: int,
    user_context_defaults: dict = None,
    user_summarization_defaults: dict = None,
    context_window_mode: str = None,
) -> dict:
    """Set context strategy for a conversation.

    Creates a context strategy based on user defaults and stores it
    in the conversation meta.

    Args:
        project_id: Project ID
        conversation_id: Conversation ID
        user_context_defaults: User's default context management settings
        user_summarization_defaults: User's default summarization settings
        context_window_mode: Project default window mode; pass it in so the settings RPC runs before the
            caller's transaction. Read from project settings (TTL-cached) when omitted

    Returns:
        dict: The context strategy configuration
    """
    strategy_data = {}

    if user_context_defaults:
        if 'enabled' in user_context_defaults:
            strategy_data['enabled'] = user_context_defaults['enabled']
        if 'preserve_recent_messages' in user_context_defaults:
            strategy_data['preserve_recent_messages'] = user_context_defaults['preserve_recent_messages']
        if 'enable_summarization' in user_context_defaults:
            strategy_data['enable_summarization'] = user_context_defaults['enable_summarization']
        if 'enable_context_editing' in user_context_defaults:
            strategy_data['enable_context_editing'] = user_context_defaults['enable_context_editing']

    if user_summarization_defaults:
        if 'enable_summarization' in user_summarization_defaults and user_summarization_defaults[
            'enable_summarization'] is not None:
            strategy_data['enable_summarization'] = user_summarization_defaults['enable_summarization']
        if 'summary_instructions' in user_summarization_defaults:
            strategy_data['summary_instructions'] = user_summarization_defaults['summary_instructions']
        # Build summary_llm_settings from flat personalization fields
        summary_llm_settings = {}
        if user_summarization_defaults.get('summary_model_name'):
            summary_llm_settings['model_name'] = user_summarization_defaults['summary_model_name']
        if user_summarization_defaults.get('summary_model_project_id') is not None:
            summary_llm_settings['model_project_id'] = user_summarization_defaults['summary_model_project_id']
        if user_summarization_defaults.get('target_summary_tokens') is not None:
            summary_llm_settings['max_tokens'] = user_summarization_defaults['target_summary_tokens']
        if summary_llm_settings:
            strategy_data['summary_llm_settings'] = summary_llm_settings

    if context_window_mode is None:
        from .context_window import get_project_default_mode
        context_window_mode = get_project_default_mode(project_id)
    strategy_data['context_window_mode'] = normalize_mode(context_window_mode)

    try:
        strategy = ContextStrategy(**strategy_data)
        strategy_dict = strategy.model_dump()
    except Exception as e:
        log.warning(f"Failed to create context strategy: {e}, using defaults")
        strategy = ContextStrategy()
        strategy_dict = strategy.model_dump()

    update_conversation_meta(
        project_id=project_id,
        conversation_id=conversation_id,
        meta_updates={'context_strategy': strategy_dict}
    )
    log.debug(f"Set context strategy for conversation {conversation_id}")

    return strategy_dict