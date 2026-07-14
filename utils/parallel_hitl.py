"""Pure helpers for the durable parallel-child HITL persistence contract."""

from copy import deepcopy


RESOLVED_INTERRUPT_IDS_KEY = 'resolved_hitl_interrupt_ids'
EXECUTION_GENERATION_KEY = 'execution_generation'
MAX_RESOLVED_INTERRUPT_IDS = 256


def pending_interrupts(meta):
    """Return all persisted pending interrupts, including legacy singular metadata."""
    if not isinstance(meta, dict):
        return []
    plural = meta.get('hitl_interrupts')
    if isinstance(plural, list) and plural:
        return [dict(item) for item in plural if isinstance(item, dict)]
    singular = meta.get('hitl_interrupt')
    return [dict(singular)] if isinstance(singular, dict) else []


def begin_execution_generation(meta, generation):
    """Start a fresh execution on a reused message row.

    Interrupt ids are stable across HITL replay and can be reused by regenerate.
    A new generation therefore clears the previous generation's tombstones; old
    callbacks are rejected separately by ``is_current_execution``.
    """
    updated = dict(meta or {})
    updated.pop(RESOLVED_INTERRUPT_IDS_KEY, None)
    updated[EXECUTION_GENERATION_KEY] = generation
    return updated


def is_current_execution(meta, payload):
    """Whether a streamed callback belongs to the message row's active run."""
    current = (meta or {}).get(EXECUTION_GENERATION_KEY)
    if not current:
        return True
    incoming = (payload or {}).get(EXECUTION_GENERATION_KEY)
    if not incoming:
        incoming = ((payload or {}).get('response_metadata') or {}).get(
            EXECUTION_GENERATION_KEY,
        )
    return incoming == current


def normalize_interrupts(response_metadata):
    """Normalize singular/plural pause payloads and attach stable routing identity."""
    response_metadata = response_metadata or {}
    lineage = response_metadata.get('metadata') or {}
    incoming = response_metadata.get('hitl_interrupts')
    if not isinstance(incoming, list) or not incoming:
        singular = response_metadata.get('hitl_interrupt')
        incoming = [singular] if isinstance(singular, dict) else []

    normalized = []
    for item in incoming:
        if not isinstance(item, dict):
            continue
        current = deepcopy(item)
        child_thread_id = (
            current.get('child_thread_id')
            or lineage.get('child_thread_id')
        )
        tool_call_id = current.get('tool_call_id') or lineage.get('tool_call_id')
        if child_thread_id:
            current.setdefault('child_thread_id', child_thread_id)
        if tool_call_id:
            current.setdefault('tool_call_id', tool_call_id)
        for key in ('parent_agent_call_id', 'sibling_ordinal'):
            if lineage.get(key) is not None:
                current.setdefault(key, deepcopy(lineage[key]))
        outer_path = lineage.get('parent_agent_path')
        inner_path = current.get('parent_agent_path')
        if isinstance(outer_path, list) and outer_path:
            inner_path = list(inner_path) if isinstance(inner_path, list) else []
            if inner_path:
                outer_last = outer_path[-1] if isinstance(outer_path[-1], dict) else {}
                inner_first = inner_path[0] if isinstance(inner_path[0], dict) else {}
                if outer_last.get('name') == inner_first.get('name'):
                    inner_path = inner_path[1:]
            current['parent_agent_path'] = deepcopy(outer_path) + deepcopy(inner_path)
        current.setdefault(
            'resume_strategy', 'aggregate_child' if child_thread_id else 'single',
        )
        normalized.append(current)
    return normalized


def merge_interrupts(meta, response_metadata):
    """Merge a pause into message metadata without overwriting paused siblings."""
    merged = {interrupt_identity(item): item for item in pending_interrupts(meta)}
    current_meta = meta if isinstance(meta, dict) else {}
    resolved = set(current_meta.get(RESOLVED_INTERRUPT_IDS_KEY) or [])
    for item in normalize_interrupts(response_metadata):
        tombstone = interrupt_tombstone_identity(item)
        if tombstone and tombstone in resolved:
            continue
        merged[interrupt_identity(item)] = item
    return list(merged.values())


def interrupt_identity(item):
    if item.get('interrupt_id'):
        return item['interrupt_id']
    thread_id = item.get('child_thread_id') or item.get('thread_id')
    tool_call_id = item.get('tool_call_id')
    if not thread_id and not tool_call_id:
        return ''
    return f'{thread_id}:{tool_call_id}'


def interrupt_tombstone_identity(item):
    """Stable identity safe to retain across later runs of the same message.

    Current SDK interrupts carry a UUID. Durable legacy child interrupts can
    safely fall back to their epoch-scoped child thread plus tool call. A root
    legacy interrupt without either signal is deliberately not tombstoned: its
    thread/tool identity can be reused by a legitimate regenerated run.
    """
    if not isinstance(item, dict):
        return None
    if item.get('interrupt_id'):
        return item['interrupt_id']
    if item.get('child_thread_id'):
        return interrupt_identity(item)
    return None


def remember_resolved_interrupts(meta, interrupts):
    """Return metadata with bounded tombstones for resolved interrupt events."""
    updated = dict(meta or {})
    raw_existing = updated.get(RESOLVED_INTERRUPT_IDS_KEY)
    raw_existing = raw_existing if isinstance(raw_existing, list) else []
    existing = [
        value for value in raw_existing
        if isinstance(value, str) and value
    ]
    for item in interrupts or []:
        identity = interrupt_tombstone_identity(item)
        if identity and identity not in existing:
            existing.append(identity)
    if existing:
        updated[RESOLVED_INTERRUPT_IDS_KEY] = existing[-MAX_RESOLVED_INTERRUPT_IDS:]
    return updated


def retire_all_interrupts(meta):
    """Clear every pending card while retaining late-event resurrection guards."""
    updated = remember_resolved_interrupts(meta, pending_interrupts(meta))
    updated.pop('hitl_interrupts', None)
    updated.pop('hitl_interrupt', None)
    return updated


def retire_interrupts(meta, interrupt_ids):
    """Retire exactly the root/in-process interrupts selected for one resume.

    Track-1 resumes run on the parent thread, so they have no durable child
    thread to scope by.  Their stable public interrupt ids are the ownership
    boundary.  Keeping this separate from ``retire_child_interrupts`` avoids
    treating an absent child thread as a wildcard for every root interrupt.
    """
    updated = dict(meta or {})
    interrupt_ids = {
        value for value in (interrupt_ids or [])
        if isinstance(value, str) and value
    }
    remaining = []
    retired = []
    for item in pending_interrupts(updated):
        if interrupt_identity(item) in interrupt_ids:
            retired.append(item)
        else:
            remaining.append(item)
    updated = remember_resolved_interrupts(updated, retired)
    if remaining:
        updated['hitl_interrupts'] = remaining
        updated['hitl_interrupt'] = remaining[0]
    else:
        updated.pop('hitl_interrupts', None)
        updated.pop('hitl_interrupt', None)
    return updated


def requires_plural_persistence(interrupts, response_metadata):
    """Whether reload must use the list protocol instead of legacy scalar resume."""
    raw_plural = (response_metadata or {}).get('hitl_interrupts')
    return bool(
        len(interrupts) > 1
        or (isinstance(raw_plural, list) and len(raw_plural) > 1)
        or any(item.get('child_thread_id') for item in interrupts)
    )


def decisions_for_child(decisions, child_thread_id, tool_call_id=None):
    """Return the complete decision list owned by one durable child."""
    decisions = [dict(item) for item in (decisions or []) if isinstance(item, dict)]
    explicitly_routed = [item for item in decisions if item.get('child_thread_id')]
    if explicitly_routed:
        return [
            item for item in explicitly_routed
            if item.get('child_thread_id') == child_thread_id
        ]
    by_thread = [item for item in decisions if item.get('thread_id') == child_thread_id]
    if by_thread:
        return by_thread
    by_tool = [item for item in decisions if item.get('tool_call_id') == tool_call_id]
    return by_tool or decisions


def validate_child_decisions(pending, decisions):
    """Require an exact, unique decision for every pending durable-child card."""
    pending = [dict(item) for item in (pending or []) if isinstance(item, dict)]
    decisions = [dict(item) for item in (decisions or []) if isinstance(item, dict)]
    expected = [interrupt_identity(item) for item in pending]
    received = [interrupt_identity(item) for item in decisions]
    if not expected or any(not identity for identity in expected):
        raise ValueError('Pending interrupt is missing a stable identity')
    if any(not identity for identity in received):
        raise ValueError('Every decision must include an interrupt identity')
    if len(received) != len(set(received)):
        raise ValueError('Duplicate interrupt decisions are not allowed')
    if set(received) != set(expected):
        raise ValueError('Decisions must exactly match all pending interrupts')

    pending_by_identity = {
        interrupt_identity(item): item for item in pending
    }
    for decision, identity in zip(decisions, received):
        action = decision.get('action')
        available = pending_by_identity[identity].get('available_actions')
        if isinstance(available, list) and available and action not in available:
            raise ValueError(
                f"Action '{action}' is not available for interrupt '{identity}'"
            )


def retire_child_interrupts(meta, child_thread_id, interrupt_ids=None):
    """Return metadata with one resumed child's pending interrupt set retired."""
    updated = dict(meta or {})
    interrupt_ids = set(interrupt_ids or [])
    remaining = []
    retired = []
    for item in pending_interrupts(updated):
        owned_by_child = (
            item.get('child_thread_id') or item.get('thread_id')
        ) == child_thread_id
        explicitly_resolved = item.get('interrupt_id') in interrupt_ids
        if not owned_by_child and not explicitly_resolved:
            remaining.append(item)
        else:
            retired.append(item)
    explicitly_resolved = [
        {'interrupt_id': interrupt_id}
        for interrupt_id in interrupt_ids
        if interrupt_id
    ]
    updated = remember_resolved_interrupts(updated, retired + explicitly_resolved)
    if remaining:
        updated['hitl_interrupts'] = remaining
        updated['hitl_interrupt'] = remaining[0]
    else:
        updated.pop('hitl_interrupts', None)
        updated.pop('hitl_interrupt', None)
    return updated
