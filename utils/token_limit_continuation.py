"""Helpers for continuing model output after an output-token limit."""


def trim_continuation_overlap(existing_tail: str, incoming_content: str) -> str:
    """Remove a meaningful repeated suffix at a continuation seam.

    A one-character suffix/prefix match is common at valid boundaries such as
    ``33. item-33`` followed by ``34. item-34`` and must not be treated as
    duplicated output.  Require at least four characters and token boundaries
    before removing an overlap.
    """
    stripped = incoming_content.lstrip('\n\r')
    max_overlap = min(len(existing_tail), len(stripped), 150)

    for overlap in range(max_overlap, 3, -1):
        suffix = existing_tail[-overlap:]
        if suffix != stripped[:overlap] or not any(char.isalnum() for char in suffix):
            continue

        starts_at_boundary = (
            overlap == len(existing_tail)
            or not (existing_tail[-overlap - 1].isalnum() and suffix[0].isalnum())
        )
        ends_at_boundary = (
            overlap == len(stripped)
            or not (suffix[-1].isalnum() and stripped[overlap].isalnum())
        )
        if starts_at_boundary and ends_at_boundary:
            return stripped[overlap:]

    return incoming_content


def is_token_limit_continuation(
    *,
    explicitly_requested: bool,
    truncated_content: str,
    hitl_resume: bool,
    mcp_auth_resume: bool,
    has_pending_interrupts: bool,
) -> bool:
    """Distinguish output continuation from checkpoint-based resume flows.

    Older UI versions did not send an explicit discriminator, so visible partial
    content remains a backwards-compatible signal.  The explicit signal is
    required when a reasoning model exhausts its budget before emitting text.
    """
    if hitl_resume or mcp_auth_resume or has_pending_interrupts:
        return False
    return explicitly_requested or bool(truncated_content)


def prepare_token_limit_payload(payload: dict, truncated_content: str) -> None:
    """Mutate a predict payload to request only the missing user-visible output.

    Thinking/reasoning steps are deliberately absent from ``truncated_content``.
    When text is available, its tail anchors a suffix-only continuation.  When
    no text is available, the model must produce the complete visible answer.
    """
    visible_content = truncated_content.rstrip('\n\r')
    original_user_input = payload.get('user_input', '')

    if original_user_input:
        payload['chat_history'].append({
            'role': 'user',
            'content': original_user_input,
        })

    if visible_content:
        tail_chars = 600
        prefill_content = visible_content[-tail_chars:]
        payload['chat_history'].append({
            'role': 'assistant',
            'content': prefill_content,
        })
        word_count = len(visible_content.split())
        original_q_clause = (
            f' The original request was: "{original_user_input}".'
            if original_user_input else ''
        )
        payload['user_input'] = (
            'Your previous response above was cut off due to token limits and is incomplete.'
            f'{original_q_clause}'
            f' It already contains approximately {word_count} words.'
            ' Please complete it: output only the missing ending that finishes the response,'
            ' strictly respecting all constraints from the original request (length, format, scope).'
            ' Do not repeat anything already written.'
        )
    else:
        original_q_clause = (
            f' The original request was: "{original_user_input}".'
            if original_user_input else ''
        )
        payload['user_input'] = (
            'Your previous attempt reached the output-token limit during internal reasoning'
            ' before producing any user-visible answer.'
            f'{original_q_clause}'
            ' Produce the complete answer now, strictly respecting all constraints from the'
            ' original request (length, format, scope). Output only the answer and do not refer'
            ' to the previous attempt.'
        )

    payload['truncated_content'] = truncated_content

    if isinstance(payload.get('llm'), dict):
        llm_kwargs = payload['llm'].get('kwargs', {})
        if 'reasoning_effort' in llm_kwargs:
            llm_kwargs['reasoning_effort'] = 'low'

    app_version = (payload.get('application') or {}).get('version_details') or {}
    app_llm_settings = app_version.get('llm_settings') or {}
    if 'reasoning_effort' in app_llm_settings:
        app_llm_settings['reasoning_effort'] = 'low'
