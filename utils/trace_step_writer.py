"""Write tool_calls / thinking_steps as normalized message_trace_step rows (Epic #5724).

The two heavy meta keys are no longer written to chat_message_group.meta; each step becomes one row.

Since #5731 the indexer emits DELTAS (one changed entry per event), not the full accumulated
state, so meta can no longer reconstruct a turn. The table is therefore the accumulator: each
save loads the group's existing rows, merges the incoming delta, re-runs the audited HITL dedup,
and rewrites the group's rows. Row counts per group are tiny (p99=20), so delete-reinsert is
cheap and avoids the JSONB read-modify-rewrite that made the meta path slow.
"""
from datetime import datetime
import json

from pylon.core.tools import log

from ..models.message_trace_step import MessageTraceStep
from .tool_call_dedup import _dedupe_replayed_tool_calls

KIND_TOOL_CALL = 'tool_call'
KIND_THINKING_STEP = 'thinking_step'

MAX_HIERARCHY_DEPTH = 8
MAX_ATTR_STRING_CHARS = 2048
MAX_ICON_META_BYTES = 8192

_HIERARCHY_KEYS = (
    'parent_agent_name', 'parent_agent_call_id', 'parent_agent_path',
    'sibling_ordinal', 'child_thread_id', 'thread_id',
)
_TOOL_METADATA_KEYS = (
    *_HIERARCHY_KEYS,
    'agent_type', 'checkpoint_ns', 'display_name', 'hitl_deferred',
    'langgraph_node', 'original_name', 'toolkit_name', 'toolkit_type',
)
_TOOL_META_KEYS = ('name', 'display_name', 'model_name')


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


def _bounded_string(value):
    if not isinstance(value, str):
        return value
    return value[:MAX_ATTR_STRING_CHARS]


def _bounded_icon_meta(value):
    if not isinstance(value, dict):
        return None
    try:
        normalized = json.loads(json.dumps(value, ensure_ascii=False, default=str))
        if len(json.dumps(normalized, ensure_ascii=False).encode('utf-8')) > MAX_ICON_META_BYTES:
            return None
    except (TypeError, ValueError):
        return None
    return normalized


def _normalize_parent_path(value):
    if not isinstance(value, list):
        return []
    normalized = []
    for item in value[:MAX_HIERARCHY_DEPTH]:
        if not isinstance(item, dict):
            continue
        name = _bounded_string(item.get('name'))
        call_id = _bounded_string(item.get('call_id'))
        ordinal = item.get('sibling_ordinal')
        if not isinstance(ordinal, int) or ordinal < 1:
            ordinal = None
        if not (name or call_id):
            continue
        normalized.append({
            'name': name,
            'call_id': call_id,
            **({'sibling_ordinal': ordinal} if ordinal is not None else {}),
        })
    return normalized


def _allowlisted_metadata(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in _TOOL_METADATA_KEYS:
        raw = value.get(key)
        if raw is None:
            continue
        if key == 'parent_agent_path':
            raw = _normalize_parent_path(raw)
            if not raw:
                continue
        elif key == 'sibling_ordinal' and (not isinstance(raw, int) or raw < 1):
            continue
        elif key == 'hitl_deferred' and not isinstance(raw, bool):
            continue
        elif key not in {'sibling_ordinal', 'hitl_deferred'}:
            if not isinstance(raw, str):
                continue
            raw = _bounded_string(raw)
        elif not isinstance(raw, (bool, int)):
            continue
        result[key] = raw
    return result


def _hierarchy_metadata(entry):
    """Canonical display lineage from top-level and nested callback metadata."""
    tool_meta = entry.get('tool_meta') if isinstance(entry.get('tool_meta'), dict) else {}
    sources = (
        entry,
        entry.get('metadata') if isinstance(entry.get('metadata'), dict) else {},
        tool_meta.get('metadata') if isinstance(tool_meta.get('metadata'), dict) else {},
        (
            entry.get('message', {}).get('response_metadata', {}).get('metadata', {})
            if isinstance(entry.get('message'), dict)
            else {}
        ),
    )
    result = {}
    for key in _HIERARCHY_KEYS:
        for source in sources:
            if not isinstance(source, dict) or source.get(key) is None:
                continue
            value = source[key]
            if key == 'parent_agent_path':
                value = _normalize_parent_path(value)
                if not value:
                    continue
            elif key == 'sibling_ordinal' and (not isinstance(value, int) or value < 1):
                continue
            elif key != 'sibling_ordinal':
                if not isinstance(value, str):
                    continue
                value = _bounded_string(value)
            result[key] = value
            break
    return result


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


def _tool_call_attrs(entry: dict) -> dict | None:
    """Bounded display-only sidecar; never copy arbitrary callback/tool metadata."""
    attrs = {}
    metadata = _allowlisted_metadata(entry.get('metadata'))
    metadata.update({k: v for k, v in _hierarchy_metadata(entry).items() if k not in metadata})
    if metadata:
        attrs['metadata'] = metadata
    raw_tool_meta = entry.get('tool_meta') if isinstance(entry.get('tool_meta'), dict) else {}
    tool_meta = {
        key: _bounded_string(raw_tool_meta[key])
        for key in _TOOL_META_KEYS
        if isinstance(raw_tool_meta.get(key), str)
    }
    tool_meta_metadata = _allowlisted_metadata(raw_tool_meta.get('metadata'))
    tool_meta_metadata.update({k: v for k, v in metadata.items() if k not in tool_meta_metadata})
    if tool_meta_metadata:
        tool_meta['metadata'] = tool_meta_metadata
    icon_meta = _bounded_icon_meta(raw_tool_meta.get('icon_meta'))
    if icon_meta:
        tool_meta['icon_meta'] = icon_meta
    if tool_meta:
        attrs['tool_meta'] = tool_meta
    return attrs or None


def tool_call_to_row(msg_group_id: int, run_id: str, entry: dict) -> MessageTraceStep:
    """Map one accumulated tool_call entry (ToolCallPayload shape) to a row."""
    hierarchy = _hierarchy_metadata(entry)
    return MessageTraceStep(
        message_group_id=msg_group_id,
        kind=KIND_TOOL_CALL,
        run_id=_bounded_string(entry.get('run_id') or entry.get('tool_run_id') or run_id),
        parent_agent_name=hierarchy.get('parent_agent_name') or _tc_parent_name(entry),
        parent_agent_call_id=hierarchy.get('parent_agent_call_id') or _parent_call_id(entry),
        started_at=_parse_ts(entry.get('timestamp_start')),
        finished_at=_parse_ts(entry.get('timestamp_finish')),
        is_error=bool(entry.get('error')),
        has_visible_content=True,
        tool_name=_bounded_string(entry.get('tool_name') or (
            entry.get('tool_meta', {}).get('name') if isinstance(entry.get('tool_meta'), dict) else None
        )),
        tool_inputs=entry.get('tool_inputs') if isinstance(entry.get('tool_inputs'), (dict, list)) else None,
        tool_output=entry.get('tool_output'),
        finish_reason=_bounded_string(entry.get('finish_reason')),
        attrs=_tool_call_attrs(entry),
    )


def thinking_step_to_row(msg_group_id: int, entry: dict) -> MessageTraceStep:
    """Map one accumulated thinking_step entry (serialized generation_chunk) to a row."""
    message = entry.get('message') if isinstance(entry.get('message'), dict) else {}
    resp_meta = message.get('response_metadata') if isinstance(message.get('response_metadata'), dict) else {}
    hierarchy = _hierarchy_metadata(entry)
    display_response_metadata = {}
    if resp_meta.get('tool_name'):
        display_response_metadata['tool_name'] = _bounded_string(resp_meta['tool_name'])
    if hierarchy:
        display_response_metadata['metadata'] = hierarchy
    attrs = dict(hierarchy)
    if display_response_metadata:
        attrs['response_metadata'] = display_response_metadata
    text = entry.get('text')
    thinking = entry.get('thinking')
    has_visible_content = bool(
        (isinstance(text, str) and text.strip())
        or (isinstance(thinking, str) and thinking.strip())
        or hierarchy.get('parent_agent_name')
        or hierarchy.get('parent_agent_path')
    )
    return MessageTraceStep(
        message_group_id=msg_group_id,
        kind=KIND_THINKING_STEP,
        run_id=_bounded_string(entry.get('tool_run_id') or entry.get('run_id')),
        parent_agent_name=hierarchy.get('parent_agent_name') or entry.get('parent_agent_name'),
        # thinking steps carry the call id at the top level (see indexer on_llm_end),
        # unlike tool_calls which nest it under metadata.
        parent_agent_call_id=hierarchy.get('parent_agent_call_id') or entry.get('parent_agent_call_id'),
        started_at=_parse_ts(entry.get('timestamp_start')),
        finished_at=_parse_ts(entry.get('timestamp_finish')),
        is_error=False,
        has_visible_content=has_visible_content,
        step_type=_bounded_string(entry.get('type')),
        text=text,
        thinking=thinking,
        model_name=_bounded_string(resp_meta.get('model_name')),
        attrs=attrs or None,
    )


def _row_to_tool_call(row: MessageTraceStep) -> dict:
    """Reconstruct a meta-shaped tool_call entry from a row, enough for dedup + re-mapping.

    metadata/tool_meta round-trip through the attrs sidecar, so checkpoint_ns / langgraph_node
    (the node component of the dedup identity) are recovered off-table; promoted columns remain
    the source of truth for the fields they hold.
    """
    attrs = row.attrs if isinstance(row.attrs, dict) else {}
    metadata = dict(attrs.get('metadata') or {})
    metadata['parent_agent_name'] = row.parent_agent_name
    metadata['parent_agent_call_id'] = row.parent_agent_call_id
    entry = {
        'run_id': row.run_id,
        'tool_run_id': row.run_id,
        'tool_name': row.tool_name,
        'metadata': metadata,
        'tool_inputs': row.tool_inputs,
        'tool_output': row.tool_output,
        'finish_reason': row.finish_reason,
        'error': None if not row.is_error else (row.tool_output or 'error'),
        'timestamp_start': row.started_at.isoformat() if row.started_at else None,
        'timestamp_finish': row.finished_at.isoformat() if row.finished_at else None,
    }
    if attrs.get('tool_meta'):
        entry['tool_meta'] = attrs['tool_meta']
    return entry


def _row_to_thinking_step(row: MessageTraceStep) -> dict:
    attrs = row.attrs if isinstance(row.attrs, dict) else {}
    resp_meta = dict(attrs.get('response_metadata') or {})
    resp_meta['model_name'] = row.model_name  # promoted column is source of truth
    return {
        'tool_run_id': row.run_id,
        'parent_agent_name': row.parent_agent_name,
        'parent_agent_call_id': row.parent_agent_call_id,
        'type': row.step_type,
        'text': row.text,
        'thinking': row.thinking,
        'timestamp_start': row.started_at.isoformat() if row.started_at else None,
        'timestamp_finish': row.finished_at.isoformat() if row.finished_at else None,
        'message': {'response_metadata': resp_meta},
        **{key: attrs[key] for key in _HIERARCHY_KEYS if attrs.get(key) is not None},
    }


def _reconstruct(rows) -> tuple[dict, list]:
    """(tool_calls dict keyed by run_id, thinking_steps list) from a set of rows."""
    tool_calls = {}
    thinking_steps = []
    for row in rows:
        if row.kind == KIND_TOOL_CALL:
            tool_calls[row.run_id] = _row_to_tool_call(row)
        elif row.kind == KIND_THINKING_STEP:
            thinking_steps.append(_row_to_thinking_step(row))
    return tool_calls, thinking_steps


def load_accumulated_from_rows(session, msg_group_id: int):
    """Reconstruct (tool_calls dict keyed by run_id, thinking_steps list) from existing rows."""
    rows = session.query(MessageTraceStep).filter(
        MessageTraceStep.message_group_id == msg_group_id
    ).all()
    return _reconstruct(rows)


# Columns synced from a freshly-built row onto an existing one during reconcile (id and
# message_group_id are the identity/parent and never change).
_SYNCED_COLUMNS = (
    'kind', 'run_id', 'parent_agent_name', 'parent_agent_call_id',
    'started_at', 'finished_at', 'is_error', 'has_visible_content', 'tool_name', 'tool_inputs',
    'tool_output', 'finish_reason', 'step_type', 'text', 'thinking',
    'model_name', 'attrs',
)


def _row_key(row: MessageTraceStep):
    """Stable natural key: prefer emitter run_id, then thinking timestamp.

    Legacy thinking steps without a run id fall back to started_at. A step with
    neither signal cannot be matched safely and therefore never collapses.
    """
    if row.run_id:
        return (row.kind, 'run', row.run_id)
    if row.started_at is None:
        return (KIND_THINKING_STEP, 'object', id(row))
    return (KIND_THINKING_STEP, 'timestamp', row.started_at)


def _apply_row_values(target: MessageTraceStep, source: MessageTraceStep) -> None:
    for col in _SYNCED_COLUMNS:
        setattr(target, col, getattr(source, col))


def _merge_thinking_steps(old_steps: list, new_steps: list) -> list:
    """Merge deltas by run id, with timestamp fallback for legacy emitters."""
    if not new_steps:
        return old_steps

    def identity(step):
        if not isinstance(step, dict):
            return None
        run_id = step.get('tool_run_id') or step.get('run_id')
        if run_id:
            return ('run', run_id)
        timestamp = step.get('timestamp_start')
        return ('timestamp', timestamp) if timestamp else None

    merged = list(old_steps)
    positions = {
        key: index
        for index, step in enumerate(merged)
        if (key := identity(step)) is not None
    }
    for step in new_steps:
        key = identity(step)
        if key is not None and key in positions:
            merged[positions[key]] = step
        else:
            if key is not None:
                positions[key] = len(merged)
            merged.append(step)
    return merged


def sync_trace_steps(session, msg_group_id: int, delta_tool_calls: dict, delta_thinking_steps: list):
    """Merge a delta into the group's accumulated steps and reconcile its rows in place.

    The table is the accumulator: load existing rows, merge the incoming delta (run_id keyed for
    tool_calls, timestamp-deduped for thinking_steps), collapse HITL replays with the audited
    _dedupe_replayed_tool_calls, then reconcile by natural key — UPDATE matched rows in place,
    INSERT new ones, DELETE only those that dropped out (e.g. a collapsed HITL replay). Keeping
    ids stable across the on_tool_start -> on_tool_end update avoids 404s on the detail endpoint
    (which reads by id) while a turn streams, and avoids rewriting every row on each partial.
    Render order is derived from (started_at, id) at read time.
    """
    existing = session.query(MessageTraceStep).filter(
        MessageTraceStep.message_group_id == msg_group_id
    ).all()
    old_tool_calls, old_thinking_steps = _reconstruct(existing)

    merged_tool_calls = {**old_tool_calls, **(delta_tool_calls or {})}
    merged_tool_calls = _dedupe_replayed_tool_calls(merged_tool_calls)
    merged_thinking_steps = _merge_thinking_steps(old_thinking_steps, delta_thinking_steps or [])

    desired = [tool_call_to_row(msg_group_id, run_id, tc) for run_id, tc in merged_tool_calls.items()]
    desired.extend(thinking_step_to_row(msg_group_id, step) for step in merged_thinking_steps)

    existing_by_key = {_row_key(r): r for r in existing}
    seen_keys = set()
    for new_row in desired:
        key = _row_key(new_row)
        current = existing_by_key.get(key)
        if current is not None:
            _apply_row_values(current, new_row)  # update in place, id preserved
        else:
            session.add(new_row)
        seen_keys.add(key)

    for key, row in existing_by_key.items():
        if key not in seen_keys:
            session.delete(row)

    log.debug("sync_trace_steps: group %s -> %s tool_calls, %s thinking_steps",
              msg_group_id, len(merged_tool_calls), len(merged_thinking_steps))
