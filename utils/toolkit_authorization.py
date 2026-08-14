"""Durable message-meta contract for on-demand toolkit authorization."""

AUTHORIZATION_REQUESTS_KEY = 'authorization_requests'
RESOLVED_AUTHORIZATION_REQUEST_IDS_KEY = 'resolved_authorization_request_ids'


def authorization_identity(request):
    return str(
        (request or {}).get('interrupt_id')
        or (request or {}).get('tool_run_id')
        or (request or {}).get('tool_call_id')
        or ''
    )


def pending_authorization_requests(meta):
    values = (meta or {}).get(AUTHORIZATION_REQUESTS_KEY) or []
    return [dict(item) for item in values if isinstance(item, dict)]


def merge_authorization_request(meta, response_metadata):
    updated = dict(meta or {})
    incoming = dict(response_metadata or {})
    lineage = incoming.get('metadata') or {}
    durable_child_thread_id = lineage.get('child_thread_id')
    nested_child_thread_id = incoming.get('child_thread_id')
    if durable_child_thread_id:
        if (
            nested_child_thread_id
            and nested_child_thread_id != durable_child_thread_id
        ):
            incoming.setdefault('thread_id', nested_child_thread_id)
        incoming['child_thread_id'] = durable_child_thread_id
    request_id = authorization_identity(incoming)
    if not request_id:
        return updated
    resolved = set(updated.get(RESOLVED_AUTHORIZATION_REQUEST_IDS_KEY) or [])
    if request_id in resolved:
        return updated
    incoming['interrupt_id'] = request_id
    incoming['authorization_request_id'] = request_id
    incoming.setdefault('available_actions', ['authorize', 'skip'])
    incoming.setdefault(
        'resume_strategy',
        # child_thread_id alone is not a durable-worker signal: in-process SDK
        # Application nesting uses one too.  The worker stamps aggregate_child
        # only when it owns a parked fan-out child task.
        'root',
    )
    by_id = {
        authorization_identity(item): item
        for item in pending_authorization_requests(updated)
    }
    by_id[request_id] = incoming
    updated[AUTHORIZATION_REQUESTS_KEY] = list(by_id.values())
    return updated


def retire_authorization_requests(meta, request_ids):
    updated = dict(meta or {})
    resolved = [str(value) for value in (request_ids or []) if value]
    resolved_set = set(resolved)
    remaining = [
        item for item in pending_authorization_requests(updated)
        if authorization_identity(item) not in resolved_set
    ]
    if remaining:
        updated[AUTHORIZATION_REQUESTS_KEY] = remaining
    else:
        updated.pop(AUTHORIZATION_REQUESTS_KEY, None)
    tombstones = list(updated.get(RESOLVED_AUTHORIZATION_REQUEST_IDS_KEY) or [])
    for request_id in resolved:
        if request_id not in tombstones:
            tombstones.append(request_id)
    if tombstones:
        updated[RESOLVED_AUTHORIZATION_REQUEST_IDS_KEY] = tombstones[-256:]
    return updated


def retire_all_authorization_requests(meta):
    return retire_authorization_requests(
        meta,
        [authorization_identity(item) for item in pending_authorization_requests(meta)],
    )
