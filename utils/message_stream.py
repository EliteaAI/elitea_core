import json

from tools import rpc_tools

from sqlalchemy.orm.attributes import flag_modified

from ..models.message_group import ConversationMessageGroup

from pylon.core.tools import log


# Input keys that grow/echo across HITL replays and must NOT contribute to a
# tool_call's identity. A sub-agent invocation re-fired on each resume carries
# the same logical args (its "task") plus a swelling transient payload
# (accumulated hitl_decisions, echoed state channels). Hashing the whole dict
# makes each replay look unique; dropping these leaves only the stable args.
_TRANSIENT_INPUT_KEYS = {
    'hitl_decisions', 'state_types', 'parallel_tasks',
    'messages', 'chat_history', '_pipeline_blocked', 'hitl_resume',
    'hitl_action', 'hitl_value',
}


# Private key under which a tool_call's computed identity is memoized ON the
# entry itself. It persists through the meta-JSON DB round-trip, so a reloaded
# entry skips re-serialization on the next save (see _tool_call_identity).
# The UI reads named fields off each entry and ignores unknown keys, so this is
# inert to consumers.
_IDENTITY_CACHE_KEY = '_dedup_identity'


def _tool_call_identity(tc: dict) -> tuple:
    """Stable identity for a tool_call across HITL-replay re-fires (#4993).

    A pipeline sub-agent with N sensitive tool calls pauses for HITL N times.
    Each resume makes LangGraph replay the graph from its checkpoint and re-fire
    on_tool_start for the SAME embedded-agent node — a fresh run_id and a fresh
    checkpoint_ns uuid every time (e.g. "Agent1:<uuid-A>" then "Agent1:<uuid-B>").
    Only the final replay ever completes; the rest are empty placeholders. Two
    entries are "the same call" when tool name, sub-agent attribution, bare node
    name, and stable inputs match. Genuinely different calls (read_file on
    another path) keep distinct inputs and are never collapsed.

    Identity is memoized on the entry under `_IDENTITY_CACHE_KEY`. dedup runs on
    EVERY partial save over the whole accumulated tool_calls dict (fan-out
    children persist only via partial_message, never full_message), so without a
    cache each of K entries is json.dumps'd on each of S saves — O(K*S) ≈ O(K^2)
    CPU + transient-dict churn. The stamp persists through the meta-JSON DB
    round-trip; reloaded entries return their cached identity without
    re-serializing, making total work O(K) (one serialize per distinct entry).
    A replacement entry for the same run_id (e.g. on_tool_end adding output /
    parent_agent_name) arrives WITHOUT the stamp, so its identity is recomputed
    from current state — never served stale.
    """
    cached = tc.get(_IDENTITY_CACHE_KEY)
    if cached is not None:
        # Reloaded from JSON: list, not tuple. Coerce so it stays hashable for
        # use as a dict key in _dedupe_replayed_tool_calls.
        return tuple(cached) if isinstance(cached, list) else cached

    meta = tc.get('metadata') or {}
    tool_meta = tc.get('tool_meta') or {}
    tm_meta = tool_meta.get('metadata') if isinstance(tool_meta, dict) else {}
    tm_meta = tm_meta or {}
    name = tool_meta.get('name') or tc.get('tool_name') or ''
    parent = meta.get('parent_agent_name') or tm_meta.get('parent_agent_name') or ''
    raw_ns = meta.get('checkpoint_ns') or ''
    node = raw_ns.split(':', 1)[0] if raw_ns else (meta.get('langgraph_node') or '')
    raw_inputs = tc.get('tool_inputs')
    if isinstance(raw_inputs, dict):
        stable_inputs = {k: v for k, v in raw_inputs.items() if k not in _TRANSIENT_INPUT_KEYS}
    else:
        stable_inputs = raw_inputs
    try:
        inputs = json.dumps(stable_inputs, sort_keys=True, default=str)
    except (TypeError, ValueError):
        inputs = str(stable_inputs)
    identity = (name, parent, node, inputs)
    # Stamp the entry so subsequent saves reuse this without re-serializing.
    # All four components are strings → JSON-safe; stored as a list and coerced
    # back to a tuple on the cached-read path above.
    if isinstance(tc, dict):
        tc[_IDENTITY_CACHE_KEY] = list(identity)
    return identity


def _dedupe_replayed_tool_calls(tool_calls: dict) -> dict:
    """Collapse HITL-replay duplicate tool_calls, keeping the COMPLETED entry.

    Each HITL resume replays the same sub-agent invocation with a fresh run_id;
    most replays are empty placeholders (no tool_output, timestamp_finish unset)
    and only the final one carries the real result. Keep the entry that actually
    completed (has tool_output, else timestamp_finish) per identity; fall back to
    the earliest if none completed. Preserves first-seen insertion order so the
    surviving chip holds its natural position. Returns a dict still keyed by
    run_id; genuinely distinct calls keep separate identities and are not merged.
    """
    if not isinstance(tool_calls, dict) or len(tool_calls) < 2:
        return tool_calls

    def _completeness(tc: dict) -> int:
        if not isinstance(tc, dict):
            return 0
        if tc.get('tool_output'):
            return 2
        if tc.get('timestamp_finish'):
            return 1
        return 0

    best: dict = {}   # identity -> (run_id, tc)
    order: list = []  # identities in first-seen order
    for run_id, tc in tool_calls.items():
        if not isinstance(tc, dict):
            best[run_id] = (run_id, tc)
            order.append(run_id)
            continue
        identity = _tool_call_identity(tc)
        if identity not in best:
            best[identity] = (run_id, tc)
            order.append(identity)
            continue
        # Same logical call replayed: keep whichever entry is more complete.
        _, kept = best[identity]
        if _completeness(tc) > _completeness(kept):
            best[identity] = (run_id, tc)

    deduped: dict = {}
    for identity in order:
        run_id, tc = best[identity]
        deduped[run_id] = tc
    return deduped

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

    # Collapse HITL-replay duplicate chips (#4993). A pipeline sub-agent with N
    # sensitive tool calls pauses N times; each resume replays the graph from its
    # checkpoint and re-fires on_tool_start for the embedded agent node with a
    # fresh run_id + checkpoint uuid, so the parent meta accumulates N near-empty
    # invocation chips, only the last of which completes. Keep the completed entry
    # per identity; real distinct tool calls differ by inputs and are untouched.
    new_meta['tool_calls'] = _dedupe_replayed_tool_calls(new_meta['tool_calls'])

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
