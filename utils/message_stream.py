from tools import rpc_tools

from sqlalchemy.orm.attributes import flag_modified

from ..models.message_group import ConversationMessageGroup

from pylon.core.tools import log

from .trace_step_writer import sync_trace_steps


def safe_decode_bytes_in_dict(obj):
    if isinstance(obj, dict):
        return {key: safe_decode_bytes_in_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [safe_decode_bytes_in_dict(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(safe_decode_bytes_in_dict(item) for item in obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    elif isinstance(obj, str):
        return obj.replace('\x00', '')
    else:
        return obj


def update_message_group_meta(msg_group: ConversationMessageGroup, payload: dict, session=None) -> ConversationMessageGroup:
    """
    This function merges the existing metadata with new metadata
    extracted from the provided payload attributes and response metadata, ensuring
    the updated metadata contains additional information such as thread IDs,
    references, error indicators, and tool-related fields.
    
    Also ensures token counts are calculated and stored in context metadata.
    """
    # Import here to avoid circular imports
    old_meta = msg_group.meta or {}
    new_meta = {**old_meta}
    # The message group belongs to the root chat run. Durable children save
    # partial traces into that same row using their own checkpoint thread ids;
    # once the root id is established, a child must not replace it.
    thread_id_value = old_meta.get('thread_id') or payload['response_metadata'].get(
        'thread_id'
    )
    meta_update = {
        'thread_id': thread_id_value,
        'references': payload.get('references', []),
        'is_error': payload['response_metadata'].get('is_error', False),
        'error': payload['response_metadata'].get('error', ''),
    }
    exec_time = payload['response_metadata'].get('execution_time_seconds')
    if exec_time is not None:
        meta_update['execution_time_seconds'] = exec_time
    new_meta.update(meta_update)
    response_meta = payload['response_metadata']
    should_continue = payload['response_metadata'].get("should_continue")

    # first_tool_timestamp_start must remain the EARLIEST llm start across all partial saves of
    # this group. new_meta.update() below clobbers keys, and sub-agent / multi-round / HITL-resume
    # saves each carry a fresh, LATER llm_start_timestamp; overwriting collapsed the recorded run
    # duration to the final round only (#5422). ISO-8601 UTC strings sort chronologically -> keep min.
    incoming_first_start = response_meta.get('llm_start_timestamp')
    existing_first_start = old_meta.get('first_tool_timestamp_start')
    if incoming_first_start and existing_first_start:
        first_tool_timestamp_start = min(incoming_first_start, existing_first_start)
    else:
        first_tool_timestamp_start = incoming_first_start or existing_first_start

    response_meta_fields = {
        'first_tool_timestamp_start': first_tool_timestamp_start,
        # Store execution_time_seconds for toolkit testing (no LLM/thinking_steps involved)
        'execution_time_seconds': response_meta.get('execution_time_seconds'),
        **payload['response_metadata'].get('additional_response_meta', {})
    }
    new_meta.update({k: v for k, v in response_meta_fields.items() if v is not None})

    # tool_calls / thinking_steps storage (Epic #5724). Since #5731 the indexer sends DELTAS
    # (one changed entry per event), not the full accumulated state, so these are the incoming
    # delta. The message_trace_step table is the accumulator: write the delta as rows (dedup
    # against existing rows happens there) and keep the two heavy keys OUT of meta.
    new_tool_calls = response_meta.get('tool_calls') or {}
    new_thinking_steps = response_meta.get('thinking_steps') or []

    if session is not None:
        sync_trace_steps(session, msg_group.id, new_tool_calls, new_thinking_steps)
    else:
        log.warning(
            'update_message_group_meta: no session; skipping trace-step write for group %s',
            msg_group.id
        )
    new_meta.pop('tool_calls', None)
    new_meta.pop('thinking_steps', None)

    new_invoked_skills = response_meta.get('invoked_skills') or []
    old_invoked_skills = old_meta.get('invoked_skills', [])
    if new_invoked_skills:
        new_meta['invoked_skills'] = [
            {'skill_id': e.get('skill_id'), 'name': e.get('name')}
            for e in new_invoked_skills
            if isinstance(e, dict)
        ]
    else:
        new_meta['invoked_skills'] = old_invoked_skills

    # Ensure context metadata exists and is properly initialized
    if 'context' not in new_meta:
        new_meta['context'] = {}

    # Only update token_count if we have actual token data from the response
    # This prevents partial saves from overwriting token counts with None
    new_token_count = payload['response_metadata'].get('llm_response_tokens_output')

    if new_token_count is not None:
        # We have new token data - update the count
        if should_continue:
            # For continue flow, add new tokens to existing count
            old_token_count = (old_meta.get('context') or {}).get('token_count') or 0
            new_meta['context']['token_count'] = old_token_count + new_token_count
        else:
            # For non-continue flow, use the new token count
            new_meta['context']['token_count'] = new_token_count
    # else: Don't touch token_count to avoid overwriting with None

    # Initialize other context fields if missing
    if 'weight' not in new_meta['context']:
        new_meta['context']['weight'] = 1.0
    if 'included' not in new_meta['context']:
        new_meta['context']['included'] = True
    if 'priority' not in new_meta['context']:
        new_meta['context']['priority'] = 1.0

    new_meta = safe_decode_bytes_in_dict(new_meta)

    msg_group.meta = new_meta

    # Update conversation meta with context analytics
    conversation = msg_group.conversation
    if conversation:
        if conversation.meta is None:
            conversation.meta = {}

        # Initialize context_analytics if not present or reset to None
        if not conversation.meta.get('context_analytics'):
            conversation.meta['context_analytics'] = {
                'summaries_generated': 0,
                'total_messages_summarized': 0,
                'current_context_tokens': 0,
                'messages_in_context': 0,
                'last_summarization': None,
            }

        analytics = conversation.meta['context_analytics']

        # Update context analytics from unified context_info
        context_info = response_meta.get('context_info')
        if context_info:
            analytics['current_context_tokens'] = context_info.get('token_count', 0)
            analytics['messages_in_context'] = context_info.get('message_count', 0)

            # Store message_count and token_count on the response message group meta
            if 'context' not in new_meta:
                new_meta['context'] = {}
            new_meta['context']['message_count'] = context_info.get('message_count', 0)
            new_meta['context']['token_count_in_context'] = context_info.get('token_count', 0)

            if context_info.get('summarized'):
                analytics['summaries_generated'] = analytics.get('summaries_generated', 0) + 1
                analytics['total_messages_summarized'] = (
                    analytics.get('total_messages_summarized', 0) +
                    context_info.get('summarized_count', 0)
                )

                # Determine which message groups were summarized using stored IDs
                chat_history_group_ids = new_meta.pop('chat_history_group_ids', [])
                summarized_count = context_info.get('summarized_count', 0)
                summarized_group_ids = chat_history_group_ids[:summarized_count]
                last_summarized_group_id = summarized_group_ids[-1] if summarized_group_ids else None

                analytics['last_summarization'] = {
                    'summarized_count': context_info.get('summarized_count'),
                    'preserved_count': context_info.get('preserved_count'),
                    'fitting_count': context_info.get('fitting_count', 0),
                    'message_group_id': msg_group.id,
                    'last_summarized_group_id': last_summarized_group_id,
                    'summary_content': context_info.get('summary_content'),
                }

                # Mark summarized message groups as included=False
                if session and summarized_group_ids:
                    for group_id in summarized_group_ids:
                        grp = session.query(ConversationMessageGroup).filter(
                            ConversationMessageGroup.id == group_id
                        ).first()
                        if grp:
                            if grp.meta is None:
                                grp.meta = {}
                            if 'context' not in grp.meta:
                                grp.meta['context'] = {}
                            grp.meta['context']['included'] = False
                            flag_modified(grp, 'meta')
                            session.add(grp)
            else:
                # Clean up chat_history_group_ids if no summarization occurred
                new_meta.pop('chat_history_group_ids', None)

    return msg_group
