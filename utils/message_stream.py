from tools import rpc_tools

from sqlalchemy.orm.attributes import flag_modified

from ..models.message_group import ConversationMessageGroup

from pylon.core.tools import log

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
    thread_id_value = payload['response_metadata'].get(
        'thread_id'
    ) if payload['response_metadata'].get('thread_id') else old_meta.get('thread_id')
    new_meta.update(
        {
            'thread_id': thread_id_value,
            'references': payload.get('references', []),
            'is_error': payload['response_metadata'].get('is_error', False),
            'error': payload['response_metadata'].get('error', ''),
            'execution_time_seconds': payload['response_metadata'].get('execution_time_seconds'),
        }
    )
    response_meta = payload['response_metadata']
    should_continue = payload['response_metadata'].get("should_continue")

    response_meta_fields = {
        'first_tool_timestamp_start': response_meta.get('llm_start_timestamp'),
        # Store execution_time_seconds for toolkit testing (no LLM/thinking_steps involved)
        'execution_time_seconds': response_meta.get('execution_time_seconds'),
        **payload['response_metadata'].get('additional_response_meta', {})
    }
    new_meta.update({k: v for k, v in response_meta_fields.items() if v is not None})

    # Merge tool_calls to preserve tools from previous "Continue" runs
    # Tool calls are dicts keyed by run_id (UUID), use run_id for deduplication
    # IMPORTANT: Update existing entries to capture tool_output from full_message events
    new_tool_calls = response_meta.get('tool_calls') or {}
    old_tool_calls = old_meta.get('tool_calls', {})

    if old_tool_calls and new_tool_calls:
        # Merge old and new, with new values taking precedence (to capture tool_output updates)
        merged_tool_calls = {**old_tool_calls, **new_tool_calls}
        new_meta['tool_calls'] = merged_tool_calls
    elif old_tool_calls:
        new_meta['tool_calls'] = old_tool_calls
    elif new_tool_calls:
        new_meta['tool_calls'] = new_tool_calls
    else:
        new_meta['tool_calls'] = {}

    new_thinking_steps = response_meta.get('thinking_steps') or []
    old_thinking_steps = old_meta.get('thinking_steps', [])

    if old_thinking_steps and new_thinking_steps:
        old_timestamps = {
            step.get('timestamp_start')
            for step in old_thinking_steps
            if isinstance(step, dict) and step.get('timestamp_start')
        }
        unique_new_steps = [
            step for step in new_thinking_steps
            if not isinstance(step, dict) or step.get('timestamp_start') not in old_timestamps
        ]
        new_meta['thinking_steps'] = old_thinking_steps + unique_new_steps
    elif old_thinking_steps:
        new_meta['thinking_steps'] = old_thinking_steps
    else:
        new_meta['thinking_steps'] = new_thinking_steps

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
