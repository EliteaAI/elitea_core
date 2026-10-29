"""Manual summarization endpoint logic.

Mirrors the SDK SummarizationMiddleware and the automatic-summary bookkeeping in
utils/message_stream.py, so a manual summary leaves the conversation in the same state.
"""

import time
import uuid
from datetime import datetime, timezone

from pylon.core.tools import log
from sqlalchemy import asc
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified
from tools import db

from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..utils.chat_history import get_role


DEFAULT_SUMMARY_INSTRUCTIONS = 'Generate a concise summary of the following conversation messages.'
DEFAULT_PRESERVE_RECENT_MESSAGES = 5
# Same input cap as the SDK middleware (trim_tokens_to_summarize)
TRIM_TOKENS_TO_SUMMARIZE = 4000
CHARS_PER_TOKEN = 4


class ConversationNotFoundError(Exception):
    pass


class SummaryRequestError(Exception):
    """The conversation cannot be summarized as requested (config / nothing to summarize)."""


class SummaryGenerationError(Exception):
    """The summary model or worker failed to produce a summary."""


def _estimate_tokens(text: str) -> int:
    return len(text) // CHARS_PER_TOKEN


def _get_summary_llm_settings(rpc_manager, project_id: int, strategy: dict) -> dict:
    """Configured summary model, else the project low-tier model, else the project default (as the SDK does)."""
    settings = strategy.get('summary_llm_settings') or {}
    if settings.get('model_name'):
        return settings
    try:
        models_data = rpc_manager.timeout(3).configurations_get_models(
            project_id=project_id, section='llm', include_shared=True,
        ) or {}
    except Exception as e:
        log.warning("Failed to resolve fallback summary model for project %s: %s", project_id, e)
        models_data = {}
    for name_key, project_key in (
            ('low_tier_default_model_name', 'low_tier_default_model_project_id'),
            ('default_model_name', 'default_model_project_id'),
    ):
        if models_data.get(name_key):
            return {**settings, 'model_name': models_data[name_key], 'model_project_id': models_data.get(project_key)}
    raise SummaryRequestError("No summary model configured and no project default model available")


def _group_text(group: ConversationMessageGroup) -> str:
    """Text-only view of a message group; attachments are named, not inlined as base64."""
    parts = []
    for item in group.message_items:
        latest_version = getattr(item, 'latest_version', None)
        if latest_version is not None and getattr(latest_version, 'canvas_content', None):
            parts.append(latest_version.canvas_content)
            continue
        content = item.content
        if isinstance(content, str):
            if content:
                parts.append(content)
            continue
        chunks = content if isinstance(content, list) else [content] if isinstance(content, dict) else []
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            if chunk.get('type') == 'text' and chunk.get('text'):
                parts.append(chunk['text'])
            elif chunk.get('type'):
                parts.append(f"[{chunk['type']}]")
    text = '\n'.join(parts).strip()
    if not text:
        return ''
    role = 'Human' if get_role(group) == 'user' else 'AI'
    return f'{role}: {text}'


def _build_summary_prompt(instructions: str, previous_summary: str | None, group_texts: list[str]) -> str:
    entries = [f'Previous summary: {previous_summary}'] if previous_summary else []
    entries.extend(t for t in group_texts if t)
    # Keep the most recent entries within the token cap, like trim_messages(strategy="last")
    budget = TRIM_TOKENS_TO_SUMMARIZE * CHARS_PER_TOKEN
    kept = []
    for entry in reversed(entries):
        if len(entry) > budget:
            if not kept:
                kept.append(entry[-budget:])
            break
        kept.append(entry)
        budget -= len(entry) + 2
    formatted = '\n\n'.join(reversed(kept))
    if '{messages}' in instructions:
        return instructions.replace('{messages}', formatted).rstrip()
    return f'{instructions}\n\n<messages>\n{formatted}\n</messages>'


def _check_result(result) -> dict:
    if not result:
        raise SummaryGenerationError("Summary generation returned no result")
    if result.get('code') or (result.get('error') and 'result' not in result):
        raise SummaryGenerationError(f"Summary generation was rejected: {result.get('error') or result.get('code')}")
    if 'result' not in result:
        raise SummaryGenerationError("Summary generation timed out")
    task_result = result['result'] or {}
    if task_result.get('error'):
        raise SummaryGenerationError(f"Summary generation failed: {task_result['error']}")
    return task_result


def generate_manual_summary(
    rpc_manager,  # context.rpc_manager
    project_id: int,
    conversation_id: int,
    user_id: int | None = None,
) -> dict:
    """Summarize the messages since the last summary, keeping the most recent ones verbatim.

    Writes the result into conversation.meta.context_analytics the same way an automatic
    summary does, and marks the summarized groups as no longer in context.
    """
    start_time = time.time()

    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter(Conversation.id == conversation_id).first()
        if conversation is None:
            raise ConversationNotFoundError(f"Conversation {conversation_id} not found")
        meta = conversation.meta or {}
        conversation_uuid = str(conversation.uuid)

        strategy = meta.get('context_strategy') or {}
        analytics = meta.get('context_analytics') or {}
        last_summarization = analytics.get('last_summarization') or {}
        previous_summary = last_summarization.get('summary_content') or None
        last_summarized_group_id = last_summarization.get('last_summarized_group_id') if previous_summary else None

        query = session.query(ConversationMessageGroup).options(
            selectinload(ConversationMessageGroup.author_participant),
            selectinload(ConversationMessageGroup.message_items),
        ).filter(ConversationMessageGroup.conversation_id == conversation_id)
        if last_summarized_group_id:
            query = query.filter(ConversationMessageGroup.id > last_summarized_group_id)
        groups = query.order_by(asc(ConversationMessageGroup.id)).all()

        preserve_recent = strategy.get('preserve_recent_messages') or DEFAULT_PRESERVE_RECENT_MESSAGES
        to_summarize = groups[:-preserve_recent] if len(groups) > preserve_recent else []
        preserved = groups[len(to_summarize):]
        if not to_summarize:
            raise SummaryRequestError(
                f"Not enough new messages to summarize: the last {preserve_recent} messages are always kept"
            )

        group_texts = [_group_text(g) for g in to_summarize]
        preserved_tokens = sum(_estimate_tokens(_group_text(g)) for g in preserved)
        summarized_ids = [g.id for g in to_summarize]
        last_group_id = groups[-1].id

        summary_instructions = strategy.get('summary_instructions') or DEFAULT_SUMMARY_INSTRUCTIONS
        prompt = _build_summary_prompt(summary_instructions, previous_summary, group_texts)

    # The session is closed before the blocking LLM call
    llm_settings = _get_summary_llm_settings(rpc_manager, project_id, strategy)
    result = rpc_manager.call.chat_predict_summary_content(
        project_id=project_id,
        message_groups=[],
        llm_settings=llm_settings,
        prompt=prompt,
        conversation_uuid=conversation_uuid,
        user_id=user_id,
    )
    task_result = _check_result(result)

    summary_content = (task_result.get('output_text') or '').strip()
    if not summary_content:
        raise SummaryGenerationError("Summary generation produced no text")

    input_tokens = task_result.get('chat_history_tokens_input') or _estimate_tokens(prompt)
    output_tokens = task_result.get('llm_response_tokens_output') or _estimate_tokens(summary_content)
    generation_time_ms = int((time.time() - start_time) * 1000)
    summary_uuid = str(uuid.uuid4())
    compression_ratio = round(input_tokens / output_tokens, 2) if output_tokens else 0

    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter(
            Conversation.id == conversation_id
        ).with_for_update().first()
        if conversation is None:
            raise ConversationNotFoundError(f"Conversation {conversation_id} not found")

        for grp in session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id.in_(summarized_ids)
        ).all():
            grp.meta = grp.meta or {}
            grp.meta.setdefault('context', {})['included'] = False
            flag_modified(grp, 'meta')

        meta = conversation.meta or {}
        analytics = dict(meta.get('context_analytics') or {})
        analytics['summaries_generated'] = analytics.get('summaries_generated', 0) + 1
        analytics['total_messages_summarized'] = analytics.get('total_messages_summarized', 0) + len(summarized_ids)
        analytics['current_context_tokens'] = output_tokens + preserved_tokens
        analytics['messages_in_context'] = len(preserved) + 1
        analytics['token_breakdown'] = {}
        analytics['last_summarization'] = {
            'uuid': summary_uuid,
            'summary_type': 'manual',
            'summarized_count': len(summarized_ids),
            'preserved_count': len(preserved),
            'fitting_count': 0,
            'message_group_id': last_group_id,
            'last_summarized_group_id': summarized_ids[-1],
            'summary_content': summary_content,
            'original_token_count': input_tokens,
            'summary_token_count': output_tokens,
            'compression_ratio': compression_ratio,
            'generation_time_ms': generation_time_ms,
            'created_at': datetime.now(tz=timezone.utc).isoformat(),
            'model_used': llm_settings.get('model_name', ''),
        }
        conversation.meta = {**meta, 'context_analytics': analytics}
        flag_modified(conversation, 'meta')
        session.commit()

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
        'compression_ratio': compression_ratio,
        'generation_time_ms': generation_time_ms,
        'message': f'Generated summary for {len(summarized_ids)} message groups',
    }


def get_conversation_summary_history(project_id: int, conversation_id: int) -> dict:
    """Return the summary history stored in conversation.meta.context_analytics."""
    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter(Conversation.id == conversation_id).first()
        if conversation is None:
            raise ConversationNotFoundError(f"Conversation {conversation_id} not found")
        meta = conversation.meta or {}

    analytics = meta.get('context_analytics') or {}
    last = analytics.get('last_summarization')
    summaries = [last] if last else []

    return {
        'summaries': summaries,
        'total_count': len(summaries),
        'summaries_generated': analytics.get('summaries_generated', 0),
    }
