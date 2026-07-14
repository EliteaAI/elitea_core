"""Unit tests for the pure HITL-replay tool_call dedup logic.

Run standalone (no pylon runtime needed) with the project venv:

    source projects/venv/bin/activate
    pytest --rootdir=utils/tests --import-mode=importlib utils/tests/test_tool_call_dedup.py -v

The module under test imports only ``json`` so it loads in isolation; we add the
parent ``utils/`` dir to sys.path and import it directly.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import tool_call_dedup as d  # noqa: E402


# --------------------------------------------------------------------------- #
# Builders mirroring the real ToolCallPayload shape (agent_common.py)
# --------------------------------------------------------------------------- #

def _wrapper(run_id, ns_uuid, ts, *, tool_output, finished=True,
             name="Name Resolver", node="agent", task="Resolve name Roman",
             hitl_decisions=None):
    """A sub-agent *invocation* (wrapper) tool_call entry.

    The wrapper has no ``parent_agent_name`` (it IS the orchestrator's call to a
    sub-agent; only inner chips carry a parent). ``tool_inputs`` carries a stable
    ``task`` plus a swelling transient ``hitl_decisions`` payload that must be
    stripped from identity. ``checkpoint_ns`` gets a fresh uuid each re-fire.
    """
    return {
        'tool_name': name,
        'tool_meta': {'name': name},
        'metadata': {'checkpoint_ns': f'{node}:{ns_uuid}'},
        'tool_inputs': {
            'task': task,
            # Transient payload that grows across HITL replays — stripped by
            # _tool_call_identity so every re-fire shares one identity.
            'hitl_decisions': hitl_decisions if hitl_decisions is not None else [],
        },
        'tool_output': tool_output,
        'timestamp_start': ts,
        'timestamp_finish': ts if finished else None,
    }


def _seq_invocation(run_prefix, start_index, rounds=4):
    """One sequential sub-agent invocation across ``rounds`` HITL rounds.

    The first ``rounds-1`` rounds are HITL pauses (on_tool_error →
    ``tool_output=None``, but ``timestamp_finish`` set); the final round is the
    real completion (``tool_output`` carries JSON). Returns an ordered list of
    ``(run_id, tc)`` pairs.
    """
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
                hitl_decisions=list(range(i)),  # grows each round
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


# --------------------------------------------------------------------------- #
# #5386 — two genuine sequential invocations must stay distinct
# --------------------------------------------------------------------------- #

def test_two_sequential_invocations_yield_two_survivors():
    """The headline #5386 bug: invoking the SAME sub-agent twice in sequence,
    each across several HITL rounds, must persist exactly TWO chips — one per
    completion — not collapse to one."""
    inv1 = _seq_invocation('inv1', start_index=1, rounds=4)
    inv2 = _seq_invocation('inv2', start_index=5, rounds=4)
    tool_calls = _as_dict(inv1, inv2)

    result = d._dedupe_replayed_tool_calls(tool_calls)

    assert len(result) == 2, (
        f'expected 2 invocations to survive, got {len(result)}: {_survivors(result)}'
    )
    # The survivors must be the two real completions (last round of each).
    survivors = _survivors(result)
    assert 'inv1-r4' in survivors, survivors
    assert 'inv2-r8' in survivors, survivors
    for run_id in survivors:
        assert result[run_id]['tool_output'], f'survivor {run_id} should be a completion'


def test_two_sequential_invocations_interleaved_in_dict_order():
    """Even if entries are not perfectly time-ordered in the dict (old+new merge
    can append later), the two completions must still survive as distinct."""
    inv1 = _seq_invocation('inv1', start_index=1, rounds=3)
    inv2 = _seq_invocation('inv2', start_index=4, rounds=3)
    # Simulate a merge where inv1's completion landed before inv2's placeholders.
    tool_calls = _as_dict(inv1, inv2)

    result = d._dedupe_replayed_tool_calls(tool_calls)

    assert len(result) == 2, f'{_survivors(result)}'


# --------------------------------------------------------------------------- #
# #4993 — a single invocation's replays must still collapse (no over-split)
# --------------------------------------------------------------------------- #

def test_single_invocation_replays_collapse_to_one():
    """A pipeline sub-agent that pauses N times within ONE invocation must
    collapse to a single completed chip — epoch logic must not over-split."""
    inv = _seq_invocation('inv1', start_index=1, rounds=5)
    tool_calls = _as_dict(inv)

    result = d._dedupe_replayed_tool_calls(tool_calls)

    assert len(result) == 1, f'{_survivors(result)}'
    only = next(iter(result.values()))
    assert only['tool_output'], 'the surviving chip must be the completed one'


def test_single_invocation_all_placeholders_keeps_one():
    """If none of the replays completed (run still in flight), keep exactly one
    placeholder, not N."""
    entries = []
    for i in range(4):
        entries.append((
            f'p-r{i}',
            _wrapper(run_id=f'p-r{i}', ns_uuid=f'u{i}',
                     ts=f'2024-01-01T00:00:0{i}', tool_output=None),
        ))
    result = d._dedupe_replayed_tool_calls(dict(entries))
    assert len(result) == 1, f'{_survivors(result)}'


# --------------------------------------------------------------------------- #
# Parallel same-args — interleaved completions must both survive
# --------------------------------------------------------------------------- #

def test_parallel_same_args_two_survivors():
    """Two PARALLEL invocations of the same sub-agent with identical args:
    each defers (tool_output="") then completes (tool_output=real), interleaved.
    Both completions must survive so reload matches the live two-chip view."""
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

    result = d._dedupe_replayed_tool_calls(tool_calls)

    assert len(result) == 2, f'{_survivors(result)}'
    for run_id, tc in result.items():
        assert tc['tool_output'], f'survivor {run_id} should be a real completion'


def test_parallel_same_args_in_different_root_instances_do_not_collapse():
    """In-flight B1 and B2 may run the same leaf with identical input.

    Both placeholders must remain visible before either completion; completion
    epochs alone cannot distinguish parallel invocations at that point.
    """
    first = _wrapper('A', 'u-a', '2024-01-01T00:00:01', tool_output=None)
    second = _wrapper('B', 'u-b', '2024-01-01T00:00:02', tool_output=None)
    first['metadata']['child_thread_id'] = 'child-B1'
    second['metadata']['child_thread_id'] = 'child-B2'

    result = d._dedupe_replayed_tool_calls({'A': first, 'B': second})

    assert list(result) == ['A', 'B']


# --------------------------------------------------------------------------- #
# Genuinely distinct calls — never merged
# --------------------------------------------------------------------------- #

def test_distinct_inputs_not_merged():
    """Different stable inputs (different task) keep distinct identities."""
    inv_a = _seq_invocation('a', start_index=1, rounds=2)
    inv_b = _seq_invocation('b', start_index=3, rounds=2)
    # Override task on inv_b so its identity differs.
    for _, tc in inv_b:
        tc['tool_inputs']['task'] = 'Resolve name Alice'
    tool_calls = _as_dict(inv_a, inv_b)

    result = d._dedupe_replayed_tool_calls(tool_calls)
    assert len(result) == 2, f'{_survivors(result)}'


# --------------------------------------------------------------------------- #
# Malformed entries — non-dict meta fields must not crash the persist path
# --------------------------------------------------------------------------- #

def test_identity_tolerates_non_dict_meta_fields():
    """A corrupt/partial entry whose ``metadata`` or ``tool_meta`` round-tripped
    as a non-dict (stray string/number/list/None) must still yield a 4-tuple
        identity instead of raising AttributeError on the ``.get()`` calls — dedup
    runs on every partial save and must not break the persist path."""
    for bad in ('oops', 42, ['x'], None):
        tc = {
            'tool_name': 'Name Resolver',
            'tool_meta': bad,
            'metadata': bad,
            'tool_inputs': {'task': 'Resolve name Roman'},
        }
        identity = d._tool_call_identity(tc)
        assert isinstance(identity, tuple) and len(identity) == 5, identity
        # name falls back to tool_name when tool_meta is unusable.
        assert identity[0] == 'Name Resolver', identity


def test_identity_tolerates_non_dict_nested_tool_meta_metadata():
    """``tool_meta`` is a dict but its nested ``metadata`` is a non-dict — the
    parent lookup (``tm_meta.get('parent_agent_name')``) must not raise."""
    tc = {
        'tool_name': 'inner_tool',
        'tool_meta': {'name': 'inner_tool', 'metadata': 'not-a-dict'},
        'metadata': {'checkpoint_ns': 'agent:uuid-1'},
        'tool_inputs': {'q': 1},
    }
    identity = d._tool_call_identity(tc)
    assert isinstance(identity, tuple) and len(identity) == 5, identity
    assert identity[0] == 'inner_tool', identity


def test_empty_and_singleton_passthrough():
    assert d._dedupe_replayed_tool_calls({}) == {}
    single = {'x': _wrapper('x', 'u', '2024-01-01T00:00:00', tool_output=None)}
    assert d._dedupe_replayed_tool_calls(single) == single
