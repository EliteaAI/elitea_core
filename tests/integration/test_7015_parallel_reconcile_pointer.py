"""#7015 P2: a reconciled epoch is findable by thread id + epoch, and can be stopped by them.

A batch eval case has only the parked result (thread id + epoch), not the parent's task id. The
reconcile re-invoke records which task it started and which child tasks ran, before the epoch keys
are dropped, so the eval runner can join the reconcile and read each child's result.
"""

import importlib.util
import json
import pathlib


_SPEC = importlib.util.spec_from_file_location(
    'parallel_dispatch_7015',
    pathlib.Path(__file__).resolve().parents[2] / 'methods' / 'parallel_dispatch.py',
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
Method = _MODULE.Method


class _Redis:
    def __init__(self):
        self.values, self.hashes, self.ttl = {}, {}, {}

    def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        self.ttl[key] = ex
        return True

    def get(self, key):
        return self.values.get(key)

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.hashes.pop(key, None)


class _TaskNode:
    def __init__(self):
        self.stopped = []

    def stop_task(self, task_id):
        self.stopped.append(task_id)


class _Harness(Method):
    def __init__(self):
        self.redis = _Redis()
        self.task_node = _TaskNode()
        self.cancelled, self.unstashed, self.cleaned = [], [], []

    def get_redis_client(self):
        return self.redis

    def _parallel_mark_epoch_cancelled(self, parent_thread_id, epoch):
        self.cancelled.append((parent_thread_id, epoch))

    def _parallel_child_unstash(self, child_thread_id):
        self.unstashed.append(child_thread_id)

    def _parallel_epoch_cleanup(self, parent_thread_id, epoch, parent_task_id=None):
        self.cleaned.append((parent_thread_id, epoch, parent_task_id))


def test_record_reconcile_keeps_the_task_and_children_after_the_epoch_keys_go():
    harness = _Harness()
    harness.redis.hashes[_MODULE._k_tasks('T', 'e1')] = {b'T:B:c1': b'child-1', b'T:R:c2': b'child-2'}

    harness._parallel_record_reconcile('T', 'e1', 'reconcile-1')
    harness.redis.hashes.clear()  # what _parallel_epoch_cleanup does next

    pointer = harness.parallel_dispatch_reconcile_pointer('T', 'e1')
    assert pointer == {'task_id': 'reconcile-1', 'epoch': 'e1',
                       'children': {'T:B:c1': 'child-1', 'T:R:c2': 'child-2'}}
    assert harness.redis.ttl[_MODULE._k_reconcile_task('T', 'e1')] == _MODULE._RECONCILE_PAYLOAD_TTL


def test_pointer_is_none_until_reconciled_and_on_garbage():
    harness = _Harness()
    assert harness.parallel_dispatch_reconcile_pointer('T', 'e1') is None
    harness.redis.values[_MODULE._k_reconcile_task('T', 'e1')] = b'not json'
    assert harness.parallel_dispatch_reconcile_pointer('T', 'e1') is None


def test_reinvoke_records_the_pointer_before_cleanup():
    source = (pathlib.Path(_SPEC.origin)).read_text()
    body = source[source.index('def _parallel_reinvoke_parent'):]
    body = body[:body.index('\n    @web.method()')]
    assert body.index('self._parallel_record_reconcile(parent_thread_id, epoch, task_id)') \
        < body.rindex('self._parallel_epoch_cleanup(parent_thread_id, epoch, parent_task_id)')


def test_stop_epoch_stops_its_children_by_thread_and_epoch():
    harness = _Harness()
    harness.redis.hashes[_MODULE._k_tasks('T', 'e1')] = {b'T:B:c1': b'child-1', b'T:R:c2': b'child-2'}

    assert harness.parallel_dispatch_stop_epoch('T', 'e1') == 2
    assert harness.task_node.stopped == ['child-1', 'child-2']
    assert harness.cancelled == [('T', 'e1')]
    assert harness.unstashed == ['T:B:c1', 'T:R:c2']
    assert harness.cleaned == [('T', 'e1', None)]


def test_stop_epoch_needs_both_ids():
    harness = _Harness()
    assert harness.parallel_dispatch_stop_epoch('T', None) == 0
    assert harness.cancelled == []


def test_epoch_children_decodes_the_tasks_hash():
    harness = _Harness()
    harness.redis.hashes[_MODULE._k_tasks('T', 'e1')] = {b'T:B:c1': b'child-1'}
    assert harness.parallel_dispatch_epoch_children('T', 'e1') == {'T:B:c1': 'child-1'}
