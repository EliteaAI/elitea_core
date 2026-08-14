from utils.toolkit_authorization import (
    authorization_identity,
    merge_authorization_request,
    pending_authorization_requests,
    retire_authorization_requests,
)


def _request(request_id, child_thread_id=None):
    value = {
        'interrupt_id': request_id,
        'guardrail_type': 'mcp_auth',
        'tool_name': f'tool-{request_id}',
        'thread_id': child_thread_id or 'root-thread',
    }
    if child_thread_id:
        value['child_thread_id'] = child_thread_id
        value['resume_strategy'] = 'aggregate_child'
    return value


def test_authorization_requests_merge_and_retire_one_without_losing_siblings():
    meta = merge_authorization_request({}, _request('auth-1', 'child-1'))
    meta = merge_authorization_request(meta, _request('auth-2', 'child-2'))

    pending = pending_authorization_requests(meta)
    assert [authorization_identity(item) for item in pending] == ['auth-1', 'auth-2']
    assert pending[0]['resume_strategy'] == 'root'

    retired = retire_authorization_requests(meta, ['auth-1'])
    assert [authorization_identity(item) for item in pending_authorization_requests(retired)] == ['auth-2']
    assert retired['resolved_authorization_request_ids'] == ['auth-1']


def test_late_duplicate_of_resolved_authorization_is_ignored():
    meta = merge_authorization_request({}, _request('auth-1'))
    meta = retire_authorization_requests(meta, ['auth-1'])

    replayed = merge_authorization_request(meta, _request('auth-1'))

    assert pending_authorization_requests(replayed) == []


def test_child_thread_without_worker_route_defaults_to_root_resume():
    request = _request('auth-in-process', 'sdk-child-thread')
    request.pop('resume_strategy')

    meta = merge_authorization_request({}, request)

    assert pending_authorization_requests(meta)[0]['resume_strategy'] == 'root'


def test_worker_route_overrides_nested_authorization_leaf_thread():
    request = _request('auth-nested', 'leaf-thread')
    request['metadata'] = {'child_thread_id': 'durable-child'}

    meta = merge_authorization_request({}, request)

    persisted = pending_authorization_requests(meta)[0]
    assert persisted['child_thread_id'] == 'durable-child'
    assert persisted['thread_id'] == 'leaf-thread'
    assert persisted['resume_strategy'] == 'aggregate_child'
