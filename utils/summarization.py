"""Manual summarization endpoint logic."""

import time
import uuid
from datetime import datetime

from pylon.core.tools import log
from sqlalchemy import asc, func
from tools import db

from ..models.message_group import ConversationMessageGroup
from ..utils.context_analytics import get_conversation_meta, update_conversation_meta


DEFAULT_SUMMARY_INSTRUCTIONS = 'Generate a concise summary of the following conversation messages.'


def _get_summary_llm_settings(conversation_meta: dict) -> dict:
    """Extract summary LLM settings from conversation strategy, raise if none."""
    strategy = conversation_meta.get('context_strategy') or {}
    settings = strategy.get('summary_llm_settings') or {}
    if not settings:
        raise ValueError("No summary_llm_settings configured for this conversation")
    return settings


def generate_manual_summary(
    rpc_manager,  # context.rpc_manager
    project_id: int,
    conversation_id: int,
    user_id: int | None = None,
) -> dict:
    """Generate a manual summary for a conversation.

    Fetches all included message groups, calls predict_summary_content,
    then writes the result into conversation.meta.context_analytics.last_summarization.

    Returns a dict with summary details.
    """
    start_time = time.time()
    meta = get_conversation_meta(project_id, conversation_id, missing=False)
    if meta is False:
        raise ValueError(f"Conversation {conversation_id} not found")

    llm_settings = _get_summary_llm_settings(meta)
    strategy = meta.get('context_strategy') or {}
    summary_instructions = strategy.get('summary_instructions') or DEFAULT_SUMMARY_INSTRUCTIONS

    with db.get_session(project_id) as session:
        row = session.query(
            func.min(ConversationMessageGroup.id),
            func.max(ConversationMessageGroup.id),
        ).filter(
            ConversationMessageGroup.conversation_id == conversation_id,
        ).one()
        from_id, to_id = row[0], row[1]

        if from_id is None:
            raise ValueError(f"No message groups found for conversation {conversation_id}")

        message_groups = session.query(ConversationMessageGroup).filter(
            ConversationMessageGroup.conversation_id == conversation_id,
            ConversationMessageGroup.id >= from_id,
            ConversationMessageGroup.id <= to_id,
        ).order_by(asc(ConversationMessageGroup.created_at)).all()

    # Call the existing RPC
    result = rpc_manager.call.chat_predict_summary_content(
        project_id=project_id,
        message_groups=message_groups,
        llm_settings=llm_settings,
        summary_instructions=summary_instructions,
        user_id=user_id,
    )

    if not result:
        raise RuntimeError("Summary generation returned no result")

    chat_history = result.get('result', {}).get('chat_history', [])
    if not chat_history:
        raise RuntimeError("Summary generation produced empty chat history")

    summary_content = str(chat_history[-1]['content'])
    input_tokens = result.get('result', {}).get('chat_history_tokens_input', 0)
    output_tokens = result.get('result', {}).get('llm_response_tokens_output', 0)
    generation_time_ms = int((time.time() - start_time) * 1000)

    if output_tokens == 0:
        raise RuntimeError("Summary generation produced empty output (zero output tokens)")

    summary_uuid = str(uuid.uuid4())
    summarization_record = {
        'uuid': summary_uuid,
        'summary_content': summary_content,
        'summary_type': 'manual',
        'last_summarized_group_id': to_id,
        'summarized_count': len(message_groups),
        'original_token_count': input_tokens,
        'summary_token_count': output_tokens,
        'compression_ratio': round(input_tokens / output_tokens, 2) if output_tokens else 0,
        'generation_time_ms': generation_time_ms,
        'created_at': datetime.utcnow().isoformat(),
        'model_used': llm_settings.get('model_name', ''),
    }

    # Update context_analytics in conversation meta
    current_meta = get_conversation_meta(project_id, conversation_id) or {}
    current_analytics = current_meta.get('context_analytics') or {}
    updated_analytics = {
        **current_analytics,
        'summaries_generated': current_analytics.get('summaries_generated', 0) + 1,
        'last_summarization': summarization_record,
    }
    update_conversation_meta(project_id, conversation_id, {'context_analytics': updated_analytics})

    log.info(
        "Manual summary generated for conversation %d in project %d: %d→%d tokens",
        conversation_id, project_id, input_tokens, output_tokens,
    )

    return {
        'success': True,
        'summary_uuid': summary_uuid,
        'summary_content': summary_content,
        'original_token_count': input_tokens,
        'summary_token_count': output_tokens,
        'compression_ratio': summarization_record['compression_ratio'],
        'generation_time_ms': generation_time_ms,
        'message': f'Generated summary for {len(message_groups)} message groups',
    }


def get_conversation_summary_history(project_id: int, conversation_id: int) -> dict:
    """Return the summary history stored in conversation.meta.context_analytics."""
    meta = get_conversation_meta(project_id, conversation_id, missing=False)
    if meta is False:
        raise ValueError(f"Conversation {conversation_id} not found")

    analytics = meta.get('context_analytics') or {}
    last = analytics.get('last_summarization')
    summaries = [last] if last else []

    return {
        'summaries': summaries,
        'total_count': len(summaries),
        'summaries_generated': analytics.get('summaries_generated', 0),
    }
