"""Launch-once regression tests for durable parallel dispatch."""

from methods.parallel_dispatch import Method


class _Redis:
    def __init__(self):
        self.keys = set()

    def set(self, key, _value, *, nx=False, ex=None):
        assert nx is True
        assert ex
        if key in self.keys:
            return False
        self.keys.add(key)
        return True

    def delete(self, key):
        self.keys.discard(key)


class _TaskNode:
    def __init__(self):
        self.starts = []

    def start_task(self, name, **kwargs):
        self.starts.append((name, kwargs))
        return f"task-{len(self.starts)}"


class _Harness(Method):
    def __init__(self):
        self.redis = _Redis()
        self.task_node = _TaskNode()

    def get_redis_client(self):
        return self.redis

    def _parallel_reconcile_stash(self, *_args, **_kwargs):
        return None

    def _parallel_prime_gate(self, *_args, **_kwargs):
        return None

    def _parallel_child_stash(self, *_args, **_kwargs):
        return None

    def _parallel_set_child_task_ids(self, *_args, **_kwargs):
        return None


def test_duplicate_delivery_of_same_dispatch_epoch_launches_each_child_once():
    harness = _Harness()
    parent_meta = {'task_name': 'indexer_agent', 'project_id': 2}
    parent_result = {
        'thread_id': 'root-thread',
        'dispatch_epoch': 'dispatch-epoch-1',
        'parallel_dispatch': [{
            'dispatch_id': 'dispatch-epoch-1:call-b',
            'tool_call_id': 'call-b',
            'child_thread_id': 'root-thread:B:call-b',
            'child_payload': {'thread_id': 'root-thread:B:call-b'},
            'name': 'B',
            'display_name': 'B',
        }],
    }

    harness.parallel_dispatch_launch_children('parent-task', parent_meta, parent_result)
    harness.parallel_dispatch_launch_children('parent-task-replayed', parent_meta, parent_result)

    assert len(harness.task_node.starts) == 1


def test_same_child_interrupt_set_can_only_be_claimed_once():
    harness = _Harness()

    assert harness.parallel_dispatch_claim_child_resume(
        'child-1', ['interrupt-2', 'interrupt-1'],
    ) is True
    assert harness.parallel_dispatch_claim_child_resume(
        'child-1', ['interrupt-1', 'interrupt-2'],
    ) is False
