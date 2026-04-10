"""
Context analytics utilities.

All context analytics are stored in conversation.meta.context_analytics.
"""

from pylon.core.tools import log
from tools import db

from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.pd.context import ContextStrategy


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


def update_conversation_meta(project_id: int, conversation_id: int, meta_updates: dict) -> dict:
    """Update conversation metadata directly in database."""
    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter(
            Conversation.id == conversation_id
        ).first()

        if not conversation:
            raise Exception(f"Conversation with ID {conversation_id} not found")

        current_meta = conversation.meta or {}
        updated_meta = {**current_meta, **meta_updates}

        session.query(Conversation).filter(
            Conversation.id == conversation_id
        ).update(
            {Conversation.meta: updated_meta},
            synchronize_session=False
        )
        session.commit()

        return updated_meta


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
        }
    else:
        updated_analytics = None

    update_conversation_meta(project_id, conversation_id, {'context_analytics': updated_analytics})


def get_context_data(project_id: int, conversation_id: int) -> tuple[dict | None, int, str]:
    """Get context analytics, max_tokens and strategy_name from conversation meta."""
    meta = get_conversation_meta(project_id, conversation_id)
    context_analytics = meta.get('context_analytics')
    context_strategy = meta.get('context_strategy', {})
    max_tokens = context_strategy.get('max_context_tokens', 64000)
    strategy_name = context_strategy.get('strategy_name', 'default')
    return context_analytics, max_tokens, strategy_name


def build_context_response(
    stored_analytics: dict | None = None,
    max_tokens: int = 64000,
    strategy_name: str = 'default'
) -> dict:
    """Build context response for UI."""
    summary_count = stored_analytics.get('summaries_generated', 0) if stored_analytics else 0
    current_tokens = stored_analytics.get('current_context_tokens', 0) if stored_analytics else 0
    messages_in_context = stored_analytics.get('messages_in_context', 0) if stored_analytics else 0

    utilization = (current_tokens / max_tokens) if max_tokens > 0 else 0.0

    return {
        'current_tokens': current_tokens,
        'max_tokens': max_tokens,
        'utilization': round(utilization, 4),
        'message_groups_in_context': messages_in_context,
        'strategy_name': strategy_name,
        'summary_count': summary_count,
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
) -> dict:
    """Set context strategy for a conversation.

    Creates a context strategy based on user defaults and stores it
    in the conversation meta.

    Args:
        project_id: Project ID
        conversation_id: Conversation ID
        user_context_defaults: User's default context management settings
        user_summarization_defaults: User's default summarization settings

    Returns:
        dict: The context strategy configuration
    """
    strategy_data = {}

    if user_context_defaults:
        if 'max_context_tokens' in user_context_defaults:
            strategy_data['max_context_tokens'] = user_context_defaults['max_context_tokens']
        if 'preserve_recent_messages' in user_context_defaults:
            strategy_data['preserve_recent_messages'] = user_context_defaults['preserve_recent_messages']
        if 'enable_summarization' in user_context_defaults:
            strategy_data['enable_summarization'] = user_context_defaults['enable_summarization']

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