"""Unit tests for the pure HITL-replay tool_call dedup logic.

Tests for utils/tool_call_dedup.py - covers issues #4993 and #5386.

Run from tests/ directory:
    pytest unit/utils/test_tool_call_dedup.py -v
"""
import sys
import pytest


@pytest.fixture(scope='module')
def dedup_module(utils_path):
    """Load the tool_call_dedup module."""
    sys.path.insert(0, str(utils_path))
    try:
        import tool_call_dedup
        return tool_call_dedup
    finally:
        sys.path.remove(str(utils_path))


def _wrapper(run_id, ns_uuid, ts, *, tool_output, finished=True,
             name="Name Resolver", node="agent", task="Resolve name Roman",
             hitl_decisions=None):
    """A sub-agent invocation (wrapper) tool_call entry."""
    return {
        'tool_name': name,
        'tool_meta': {'name': name},
        'metadata': {'checkpoint_ns': f'{node}:{ns_uuid}'},
        'tool_inputs': {
            'task': task,
            'hitl_decisions': hitl_decisions if hitl_decisions is not None else [],
        },
        'tool_output': tool_output,
        'timestamp_start': ts,
        'timestamp_finish': ts if finished else None,
    }


def _seq_invocation(run_prefix, start_index, rounds=4):
    """One sequential sub-agent invocation across rounds HITL rounds."""
    entries = []
    for i in range(rounds):
        idx = start_index + i
        is_last = (i == rounds - 1)
        entries.append((
            f'{run_prefix}-r{idx}',
            _wrapper(
                run_id=f'{run_prefix}-r{idx}',
                ns_uuid=f'uuid-{run_prefix}-{idx}',
                ts=f'2024-01-01T00:00:{idx:02d}',
                tool_output=('{"name":"Roman"}' if is_last else None),
                hitl_decisions=list(range(i)),
            ),
        ))
    return entries


def _as_dict(*entry_lists):
    out = {}
    for entries in entry_lists:
        for run_id, tc in entries:
            out[run_id] = tc
    return out


def _survivors(result):
    return list(result.keys())


class TestSequentialInvocations:
    """#5386 - two genuine sequential invocations must stay distinct."""

    def test_two_sequential_invocations_yield_two_survivors(self, dedup_module):
        """Invoking the SAME sub-agent twice in sequence must persist exactly TWO chips."""
        inv1 = _seq_invocation('inv1', start_index=1, rounds=4)
        inv2 = _seq_invocation('inv2', start_index=5, rounds=4)
        tool_calls = _as_dict(inv1, inv2)

        result = dedup_module._dedupe_replayed_tool_calls(tool_calls)

        assert len(result) == 2, f'expected 2 invocations, got {len(result)}: {_survivors(result)}'
        survivors = _survivors(result)
        assert 'inv1-r4' in survivors, survivors
        assert 'inv2-r8' in survivors, survivors
        for run_id in survivors:
            assert result[run_id]['tool_output'], f'survivor {run_id} should be a completion'

    def test_two_sequential_invocations_interleaved_in_dict_order(self, dedup_module):
        """Even if entries are not time-ordered, both completions must survive."""
        inv1 = _seq_invocation('inv1', start_index=1, rounds=3)
        inv2 = _seq_invocation('inv2', start_index=4, rounds=3)
        tool_calls = _as_dict(inv1, inv2)

        result = dedup_module._dedupe_replayed_tool_calls(tool_calls)

        assert len(result) == 2, f'{_survivors(result)}'


class TestSingleInvocationReplays:
    """#4993 - a single invocation's replays must still collapse."""

    def test_single_invocation_replays_collapse_to_one(self, dedup_module):
        """A pipeline sub-agent that pauses N times must collapse to a single chip."""
        inv = _seq_invocation('inv1', start_index=1, rounds=5)
        tool_calls = _as_dict(inv)

        result = dedup_module._dedupe_replayed_tool_calls(tool_calls)

        assert len(result) == 1, f'{_survivors(result)}'
        only = next(iter(result.values()))
        assert only['tool_output'], 'the surviving chip must be the completed one'

    def test_single_invocation_all_placeholders_keeps_one(self, dedup_module):
        """If none of the replays completed, keep exactly one placeholder."""
        entries = []
        for i in range(4):
            entries.append((
                f'p-r{i}',
                _wrapper(run_id=f'p-r{i}', ns_uuid=f'u{i}',
                         ts=f'2024-01-01T00:00:0{i}', tool_output=None),
            ))
        result = dedup_module._dedupe_replayed_tool_calls(dict(entries))
        assert len(result) == 1, f'{_survivors(result)}'


class TestParallelInvocations:
    """Parallel same-args - interleaved completions must both survive."""

    def test_parallel_same_args_two_survivors(self, dedup_module):
        """Two PARALLEL invocations with identical args: both completions must survive."""
        def deferred(run_id, ts):
            tc = _wrapper(run_id=run_id, ns_uuid=run_id, ts=ts, tool_output='')
            tc['metadata']['hitl_deferred'] = True
            return (run_id, tc)

        def completed(run_id, ts):
            return (run_id, _wrapper(run_id=run_id, ns_uuid=run_id, ts=ts,
                                     tool_output='{"name":"Roman"}'))

        tool_calls = _as_dict([
            deferred('A', '2024-01-01T00:00:01'),
            deferred('B', '2024-01-01T00:00:02'),
            completed('A2', '2024-01-01T00:00:03'),
            completed('B2', '2024-01-01T00:00:04'),
        ])

        result = dedup_module._dedupe_replayed_tool_calls(tool_calls)

        assert len(result) == 2, f'{_survivors(result)}'
        for run_id, tc in result.items():
            assert tc['tool_output'], f'survivor {run_id} should be a real completion'

    def test_parallel_same_args_in_different_root_instances_do_not_collapse(self, dedup_module):
        first = _wrapper('A', 'u-a', '2024-01-01T00:00:01', tool_output=None)
        second = _wrapper('B', 'u-b', '2024-01-01T00:00:02', tool_output=None)
        first['metadata']['child_thread_id'] = 'child-B1'
        second['metadata']['child_thread_id'] = 'child-B2'

        result = dedup_module._dedupe_replayed_tool_calls({'A': first, 'B': second})

        assert list(result) == ['A', 'B']


class TestDistinctInputs:
    """Genuinely distinct calls - never merged."""

    def test_distinct_inputs_not_merged(self, dedup_module):
        """Different stable inputs (different task) keep distinct identities."""
        inv_a = _seq_invocation('a', start_index=1, rounds=2)
        inv_b = _seq_invocation('b', start_index=3, rounds=2)
        for _, tc in inv_b:
            tc['tool_inputs']['task'] = 'Resolve name Alice'
        tool_calls = _as_dict(inv_a, inv_b)

        result = dedup_module._dedupe_replayed_tool_calls(tool_calls)
        assert len(result) == 2, f'{_survivors(result)}'


class TestMalformedEntries:
    """Malformed entries must not crash the persist path."""

    def test_identity_tolerates_non_dict_meta_fields(self, dedup_module):
        """Corrupt entries with non-dict metadata must still yield a stable identity."""
        for bad in ('oops', 42, ['x'], None):
            tc = {
                'tool_name': 'Name Resolver',
                'tool_meta': bad,
                'metadata': bad,
                'tool_inputs': {'task': 'Resolve name Roman'},
            }
            identity = dedup_module._tool_call_identity(tc)
            assert isinstance(identity, tuple) and len(identity) == 5, identity
            assert identity[0] == 'Name Resolver', identity

    def test_identity_tolerates_non_dict_nested_tool_meta_metadata(self, dedup_module):
        """tool_meta with non-dict nested metadata must not raise."""
        tc = {
            'tool_name': 'inner_tool',
            'tool_meta': {'name': 'inner_tool', 'metadata': 'not-a-dict'},
            'metadata': {'checkpoint_ns': 'agent:uuid-1'},
            'tool_inputs': {'q': 1},
        }
        identity = dedup_module._tool_call_identity(tc)
        assert isinstance(identity, tuple) and len(identity) == 5, identity
        assert identity[0] == 'inner_tool', identity


class TestEdgeCases:
    """Edge cases - empty and singleton."""

    def test_empty_and_singleton_passthrough(self, dedup_module):
        assert dedup_module._dedupe_replayed_tool_calls({}) == {}
        single = {'x': _wrapper('x', 'u', '2024-01-01T00:00:00', tool_output=None)}
        assert dedup_module._dedupe_replayed_tool_calls(single) == single
