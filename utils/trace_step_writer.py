"""Write tool_calls / thinking_steps as normalized message_trace_step rows (TS-2, Epic #5724).

Under the table-storage flag (see get_legacy_meta_tool_calls_storage_flag) the two heavy
meta keys are no longer written to chat_message_group.meta; each step becomes one row.

Since #5731 the indexer emits DELTAS (one changed entry per event), not the full accumulated
state, so meta can no longer reconstruct a turn. The table is therefore the accumulator: each
save loads the group's existing rows, merges the incoming delta, re-runs the audited HITL dedup,
and rewrites the group's rows. Row counts per group are tiny (p99=20), so delete-reinsert is
cheap and avoids the JSONB read-modify-rewrite that made the meta path slow.
"""
from datetime import datetime

from pylon.core.tools import log

from ..models.message_trace_step import MessageTraceStep
from .tool_call_dedup import _dedupe_replayed_tool_calls

KIND_TOOL_CALL = 'tool_call'
KIND_THINKING_STEP = 'thinking_step'


def _parse_ts(value):
    """ISO-8601 string -> aware datetime, or None. Rows store timestamptz; entries carry strings."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def _tc_parent_name(entry: dict) -> str:
    meta = entry.get('metadata') if isinstance(entry.get('metadata'), dict) else {}
    if meta.get('parent_agent_name'):
        return meta['parent_agent_name']
    tool_meta = entry.get('tool_meta') if isinstance(entry.get('tool_meta'), dict) else {}
    tm_meta = tool_meta.get('metadata') if isinstance(tool_meta.get('metadata'), dict) else {}
    return tm_meta.get('parent_agent_name')


def _parent_call_id(entry: dict) -> str:
    meta = entry.get('metadata') if isinstance(entry.get('metadata'), dict) else {}
    return meta.get('parent_agent_call_id')


def tool_call_to_row(msg_group_id: int, run_id: str, entry: dict) -> MessageTraceStep:
    """Map one accumulated tool_call entry (ToolCallPayload shape) to a row."""
    return MessageTraceStep(
        message_group_id=msg_group_id,
        kind=KIND_TOOL_CALL,
        run_id=entry.get('run_id') or entry.get('tool_run_id') or run_id,
        parent_agent_name=_tc_parent_name(entry),
        parent_agent_call_id=_parent_call_id(entry),
        started_at=_parse_ts(entry.get('timestamp_start')),
        finished_at=_parse_ts(entry.get('timestamp_finish')),
        is_error=bool(entry.get('error')),
        tool_name=entry.get('tool_name') or (
            entry.get('tool_meta', {}).get('name') if isinstance(entry.get('tool_meta'), dict) else None
        ),
        tool_inputs=entry.get('tool_inputs') if isinstance(entry.get('tool_inputs'), (dict, list)) else None,
        tool_output=entry.get('tool_output'),
        finish_reason=entry.get('finish_reason'),
    )


def thinking_step_to_row(msg_group_id: int, entry: dict) -> MessageTraceStep:
    """Map one accumulated thinking_step entry (serialized generation_chunk) to a row."""
    message = entry.get('message') if isinstance(entry.get('message'), dict) else {}
    resp_meta = message.get('response_metadata') if isinstance(message.get('response_metadata'), dict) else {}
    return MessageTraceStep(
        message_group_id=msg_group_id,
        kind=KIND_THINKING_STEP,
        run_id=entry.get('tool_run_id'),
        parent_agent_name=entry.get('parent_agent_name'),
        # thinking steps carry the call id at the top level (see indexer on_llm_end),
        # unlike tool_calls which nest it under metadata.
        parent_agent_call_id=entry.get('parent_agent_call_id'),
        started_at=_parse_ts(entry.get('timestamp_start')),
        finished_at=_parse_ts(entry.get('timestamp_finish')),
        is_error=False,
        step_type=entry.get('type'),
        text=entry.get('text'),
        thinking=entry.get('thinking'),
        model_name=resp_meta.get('model_name'),
    )


def _row_to_tool_call(row: MessageTraceStep) -> dict:
    """Reconstruct a meta-shaped tool_call entry from a row, enough for dedup + re-mapping.

    checkpoint_ns/tool_meta are not persisted, so the reconstructed identity uses
    (tool_name, parent_agent_name, tool_inputs) — the node component of the original heuristic
    is unavailable off-table. See dedup note in sync_trace_steps.
    """
    return {
        'run_id': row.run_id,
        'tool_run_id': row.run_id,
        'tool_name': row.tool_name,
        'metadata': {
            'parent_agent_name': row.parent_agent_name,
            'parent_agent_call_id': row.parent_agent_call_id,
        },
        'tool_inputs': row.tool_inputs,
        'tool_output': row.tool_output,
        'finish_reason': row.finish_reason,
        'error': None if not row.is_error else (row.tool_output or 'error'),
        'timestamp_start': row.started_at.isoformat() if row.started_at else None,
        'timestamp_finish': row.finished_at.isoformat() if row.finished_at else None,
    }


def _row_to_thinking_step(row: MessageTraceStep) -> dict:
    return {
        'tool_run_id': row.run_id,
        'parent_agent_name': row.parent_agent_name,
        'parent_agent_call_id': row.parent_agent_call_id,
        'type': row.step_type,
        'text': row.text,
        'thinking': row.thinking,
        'timestamp_start': row.started_at.isoformat() if row.started_at else None,
        'timestamp_finish': row.finished_at.isoformat() if row.finished_at else None,
        'message': {'response_metadata': {'model_name': row.model_name}},
    }


def load_accumulated_from_rows(session, msg_group_id: int):
    """Reconstruct (tool_calls dict keyed by run_id, thinking_steps list) from existing rows."""
    rows = session.query(MessageTraceStep).filter(
        MessageTraceStep.message_group_id == msg_group_id
    ).all()
    tool_calls = {}
    thinking_steps = []
    for row in rows:
        if row.kind == KIND_TOOL_CALL:
            tool_calls[row.run_id] = _row_to_tool_call(row)
        elif row.kind == KIND_THINKING_STEP:
            thinking_steps.append(_row_to_thinking_step(row))
    return tool_calls, thinking_steps


def _merge_thinking_steps(old_steps: list, new_steps: list) -> list:
    """Append new steps not already present (dedup by timestamp_start, matching the meta path)."""
    if not new_steps:
        return old_steps
    old_timestamps = {
        s.get('timestamp_start') for s in old_steps
        if isinstance(s, dict) and s.get('timestamp_start')
    }
    unique_new = [
        s for s in new_steps
        if not isinstance(s, dict) or s.get('timestamp_start') not in old_timestamps
    ]
    return old_steps + unique_new


def sync_trace_steps(session, msg_group_id: int, delta_tool_calls: dict, delta_thinking_steps: list):
    """Merge a delta into the group's accumulated steps and rewrite its rows.

    The table is the accumulator: load existing rows, merge the incoming delta (run_id keyed for
    tool_calls, timestamp-deduped for thinking_steps), collapse HITL replays with the audited
    _dedupe_replayed_tool_calls, then delete + reinsert the group's rows. No render-order sort:
    reads order by (started_at, id).
    """
    old_tool_calls, old_thinking_steps = load_accumulated_from_rows(session, msg_group_id)

    merged_tool_calls = {**old_tool_calls, **(delta_tool_calls or {})}
    merged_tool_calls = _dedupe_replayed_tool_calls(merged_tool_calls)
    merged_thinking_steps = _merge_thinking_steps(old_thinking_steps, delta_thinking_steps or [])

    session.query(MessageTraceStep).filter(
        MessageTraceStep.message_group_id == msg_group_id
    ).delete(synchronize_session=False)

    rows = [tool_call_to_row(msg_group_id, run_id, tc) for run_id, tc in merged_tool_calls.items()]
    rows.extend(thinking_step_to_row(msg_group_id, step) for step in merged_thinking_steps)
    if rows:
        session.add_all(rows)
    log.debug("sync_trace_steps: group %s -> %s tool_calls, %s thinking_steps",
              msg_group_id, len(merged_tool_calls), len(merged_thinking_steps))
