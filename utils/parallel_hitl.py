"""Pure helpers for the durable parallel-child HITL persistence contract.

State ownership is intentionally split by meaning, not duplicated:

* PostgreSQL message metadata is the durable source for pending approval cards, accepted-card
  tombstones, and reload/replay behavior. ``retire_*`` means that an approval was accepted or
  invalidated by a message lifecycle transition; it never means that a child execution finished.
* The Redis gate in ``methods.parallel_dispatch`` is the sole source for child execution
  completion and parent reconciliation. Nothing in this module decrements or reconstructs it.

Keeping approval state durable is required because the Redis coordination epoch is transient and
cannot reconstruct the UI after reload or reject a late duplicate pause callback.
"""

from copy import deepcopy


RESOLVED_INTERRUPT_IDS_KEY = 'resolved_hitl_interrupt_ids'
EXECUTION_GENERATION_KEY = 'execution_generation'
MAX_RESOLVED_INTERRUPT_IDS = 256
SUPERVISOR_DECISIONS_KEY = 'parallel_hitl_decisions'
SUPERVISOR_ROSTER_KEY = 'parallel_hitl_roster'
MAX_SUPERVISOR_DECISIONS = 256
INTERNAL_CONTINUE_TOKEN = object()
TRANSIENT_SUPERVISOR_DECISION_KEYS = {'_mcp_tokens'}
PARALLEL_TERMINAL_ERROR_KEYS = (
    'code', 'user_message', 'attempts', 'failure_reason', 'stop_reason',
    'partial_output_available',
)


def normalize_parallel_terminal_error(value):
    """Normalize a bounded error envelope before parent reconciliation."""
    if isinstance(value, dict):
        normalized = {
            key: value[key]
            for key in PARALLEL_TERMINAL_ERROR_KEYS
            if value.get(key) is not None
        }
        message = value.get('user_message') or value.get('error')
        if message and 'user_message' not in normalized:
            normalized['user_message'] = str(message)[:1000]
    else:
        normalized = {'user_message': str(value or 'Parallel child execution failed.')[:1000]}
    normalized.setdefault('code', 'parallel_child_failed')
    normalized.setdefault('user_message', 'Parallel child execution failed.')
    return normalized


def decision_ack_key(message_id, decision_id, phase):
    return f'parallel_hitl_ack:{message_id}:{decision_id}:{phase}'


def pending_supervisor_decisions(meta):
    values = (meta or {}).get(SUPERVISOR_DECISIONS_KEY) or []
    return [dict(item) for item in values if isinstance(item, dict)]


def persist_supervisor_decision(meta, decision, phase='queued'):
    """Durably upsert one invocation-scoped decision before pub/sub delivery."""
    updated = dict(meta or {})
    current = pending_supervisor_decisions(updated)
    decision_id = str((decision or {}).get('decision_id') or '')
    if not decision_id:
        return updated
    incoming = {
        key: value for key, value in dict(decision).items()
        if key not in TRANSIENT_SUPERVISOR_DECISION_KEYS
    }
    incoming['phase'] = phase
    by_id = {
        str(item.get('decision_id')): item
        for item in current if item.get('decision_id')
    }
    existing = by_id.get(decision_id) or {}
    by_id[decision_id] = {**existing, **incoming}
    updated[SUPERVISOR_DECISIONS_KEY] = list(by_id.values())[
        -MAX_SUPERVISOR_DECISIONS:
    ]
    return updated


def update_supervisor_decision_phase(meta, decision_id, phase):
    updated = dict(meta or {})
    decisions = pending_supervisor_decisions(updated)
    for item in decisions:
        if str(item.get('decision_id')) == str(decision_id):
            item['phase'] = phase
    if decisions:
        updated[SUPERVISOR_DECISIONS_KEY] = decisions
    return updated


def claim_supervisor_decision_phase(meta, decision_id, expected_phases, phase):
    """Atomically-shaped pure update used under the message-row lock.

    The caller owns the database lock.  Returning ``claimed=False`` lets a
    competing terminal callback or socket request observe that another path
    already took responsibility for the durable fallback.
    """
    updated = dict(meta or {})
    decisions = pending_supervisor_decisions(updated)
    expected = set(expected_phases or [])
    claimed = False
    for item in decisions:
        if (
            str(item.get('decision_id')) == str(decision_id)
            and item.get('phase') in expected
        ):
            item['phase'] = phase
            claimed = True
            break
    if decisions:
        updated[SUPERVISOR_DECISIONS_KEY] = decisions
    return updated, claimed


def complete_supervisor_decisions(meta, tool_call_id):
    """Mark live decisions settled when their resumed child becomes terminal."""
    updated = dict(meta or {})
    decisions = pending_supervisor_decisions(updated)
    for item in decisions:
        if str(item.get('tool_call_id') or '') == str(tool_call_id or ''):
            item['phase'] = 'completed'
    if decisions:
        updated[SUPERVISOR_DECISIONS_KEY] = decisions
    return updated


def merge_supervisor_roster(meta, state):
    """Persist bounded branch-local supervisor status for crash/reload recovery."""
    updated = dict(meta or {})
    roster = dict(updated.get(SUPERVISOR_ROSTER_KEY) or {})
    root_thread_id = str((state or {}).get('root_thread_id') or '')
    tool_call_id = str((state or {}).get('tool_call_id') or '')
    if root_thread_id:
        roster['root_thread_id'] = root_thread_id
    if tool_call_id:
        children = dict(roster.get('children') or {})
        children[tool_call_id] = {
            **dict(children.get(tool_call_id) or {}),
            **{
                key: value for key, value in dict(state or {}).items()
                if key not in {'chat_project_id'}
            },
        }
        roster['children'] = children
    if roster:
        updated[SUPERVISOR_ROSTER_KEY] = roster
    return updated


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
    for key in ('continuation_error', 'budget_error_code', 'is_error', 'error'):
        updated.pop(key, None)
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
        # The task metadata identifies the durable worker child whose launch
        # payload is stashed in Redis. A nested in-process Application may also
        # put its leaf checkpoint id in ``child_thread_id``; that value is useful
        # for LangGraph routing, but it must not replace the durable worker id or
        # Core will look up a Redis key that can never exist on resume.
        durable_child_thread_id = lineage.get('child_thread_id')
        nested_child_thread_id = current.get('child_thread_id')
        child_thread_id = durable_child_thread_id or nested_child_thread_id
        if (
            durable_child_thread_id
            and nested_child_thread_id
            and nested_child_thread_id != durable_child_thread_id
        ):
            current.setdefault('thread_id', nested_child_thread_id)
        tool_call_id = current.get('tool_call_id') or lineage.get('tool_call_id')
        if child_thread_id:
            current['child_thread_id'] = child_thread_id
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
        # Only the worker task lineage identifies a parked Core fan-out child
        # backed by ``parallel_child_launch:*``. SDK Applications also attach
        # child_thread_id while bubbling an in-process LangGraph interrupt, but
        # those pauses must continue the root worker so the decision reaches
        # the actual nested caller. Never trust an SDK-supplied strategy here.
        if (
            not durable_child_thread_id
            and response_metadata.get('resume_strategy') == 'supervised_child'
        ):
            current['resume_strategy'] = 'supervised_child'
        else:
            current['resume_strategy'] = (
                'aggregate_child' if durable_child_thread_id else 'single'
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
    """Clear every pending card after a terminal message lifecycle transition."""
    updated = remember_resolved_interrupts(meta, pending_interrupts(meta))
    updated.pop('hitl_interrupts', None)
    updated.pop('hitl_interrupt', None)
    updated.pop(SUPERVISOR_DECISIONS_KEY, None)
    updated.pop(SUPERVISOR_ROSTER_KEY, None)
    return updated


def retire_interrupts(meta, interrupt_ids):
    """Retire exactly the root/in-process interrupts selected for one resume.

    Track-1 resumes run on the parent thread, so they have no durable child
    thread to scope by.  Their stable public interrupt ids are the ownership
    boundary.  Keeping this separate from ``retire_child_interrupts`` avoids
    treating an absent child thread as a wildcard for every root interrupt.
    This records accepted approval cards, not child execution completion.
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


def validate_child_decisions(pending, decisions, require_all=True):
    """Validate unique decisions against pending interrupt identities.

    A parked worker child is resumed once and therefore still requires a
    complete decision set.  An in-process root aggregate can be resumed with a
    subset: the SDK checkpoints the completed leaf and returns the remaining
    interrupts with their stable identities.
    """
    pending = [dict(item) for item in (pending or []) if isinstance(item, dict)]
    decisions = [dict(item) for item in (decisions or []) if isinstance(item, dict)]
    expected = [interrupt_identity(item) for item in pending]
    received = [interrupt_identity(item) for item in decisions]
    if not expected or any(not identity for identity in expected):
        raise ValueError('Pending interrupt is missing a stable identity')
    if not received or any(not identity for identity in received):
        raise ValueError('Every decision must include an interrupt identity')
    if len(received) != len(set(received)):
        raise ValueError('Duplicate interrupt decisions are not allowed')
    if require_all and set(received) != set(expected):
        raise ValueError('Decisions must exactly match all pending interrupts')
    if not require_all and not set(received).issubset(set(expected)):
        raise ValueError('Decision does not match a pending interrupt')

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


def partition_root_hitl_decisions(
    pending_hitl, pending_authorizations, decisions,
):
    """Validate and partition one mixed root-checkpoint resume.

    Nested MCP authorization and sensitive-tool pauses can be surfaced in the
    same root aggregate even though Core persists them in separate metadata
    collections. The root LangGraph checkpoint accepts one decision list, so
    validate that list against the union and return the identities to retire
    from each collection.
    """
    pending_hitl = [
        dict(item) for item in (pending_hitl or []) if isinstance(item, dict)
    ]
    pending_authorizations = [
        dict(item) for item in (pending_authorizations or [])
        if isinstance(item, dict)
    ]
    decisions = [
        dict(item) for item in (decisions or []) if isinstance(item, dict)
    ]
    validate_child_decisions(
        pending_hitl + pending_authorizations,
        decisions,
        require_all=False,
    )
    hitl_ids = {interrupt_identity(item) for item in pending_hitl}
    authorization_ids = {
        interrupt_identity(item) for item in pending_authorizations
    }
    received_ids = [interrupt_identity(item) for item in decisions]
    return (
        [identity for identity in received_ids if identity in hitl_ids],
        [identity for identity in received_ids if identity in authorization_ids],
    )


def retire_child_interrupts(meta, child_thread_id, interrupt_ids=None):
    """Retire one resumed child's cards after its replacement task is accepted.

    Sibling cards remain pending. The child itself remains open until the Redis terminal gate in
    ``parallel_dispatch`` settles it; this helper does not represent or infer completion.
    """
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
