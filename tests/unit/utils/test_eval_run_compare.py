"""Performance P-4: the run-over-run performance diff (performance design §3.4, §21.7).

Cases align by ``dataset_case_id``; each metric covers only the cases both runs measured; a case
whose frozen input changed is left out; latency changes inside ±15 % / 2 s read as noise; and a
changed time limit withholds the latency and timeout verdicts.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def compare(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    load_utils_module(utils_path, 'evaluation_execution')
    return load_utils_module(utils_path, 'evaluation_run_compare')


def _execution(latency, *, status='ok', tool_calls=2, answer_ms=None, recorded=True, index=0):
    steps = [{'kind': 'llm', 'duration_ms': 100, 'parent_agent': None},
             {'kind': 'llm', 'duration_ms': 9999, 'parent_agent': 'Child'}]
    if answer_ms is not None:
        steps.append({'kind': 'llm', 'duration_ms': answer_ms, 'parent_agent': None})
    return {'case_index': index, 'status': status,
            'trajectory_state': 'recorded' if recorded else 'not_recorded',
            'metrics': {'latency_ms': latency, 'tool_calls': tool_calls},
            'trajectory': {'steps': steps} if recorded else None}


def _usage(tokens=(100, 50), cost=0.01, *, source='runtime:costs-catalog', state='recorded'):
    return {'usage_state': state, 'input_tokens': tokens[0], 'output_tokens': tokens[1],
            'cost': cost, 'cost_source': source}


def _side(compare, cases, *, limit=120, dataset_id=7, inputs=None):
    inputs = inputs or {}
    snapshot = {'cases': [{'id': cid, 'input': inputs.get(cid, f'q{cid}'), 'variables': {}} for cid in cases]}
    return {'snapshot': snapshot, 'dataset_id': dataset_id, 'case_time_limit_seconds': limit,
            'cases': {cid: compare.case_view(*pair) for cid, pair in cases.items()}}


def test_final_answer_is_the_last_top_level_llm_step(compare):
    assert compare.final_answer_ms({'steps': [
        {'kind': 'llm', 'duration_ms': 10}, {'kind': 'tool', 'duration_ms': 5},
        {'kind': 'llm', 'duration_ms': 30}, {'kind': 'llm', 'duration_ms': 99, 'parent_agent': 'X'}]}) == 30
    assert compare.final_answer_ms({'steps': []}) is None
    assert compare.final_answer_ms(None) is None


def test_metrics_over_matched_cases(compare):
    run = _side(compare, {1: (_execution(20_000, answer_ms=500), _usage((200, 100), 0.02)),
                          2: (_execution(40_000, tool_calls=5), _usage())})
    ref = _side(compare, {1: (_execution(10_000, answer_ms=400), _usage()),
                          2: (_execution(12_000, tool_calls=1), _usage())})
    diff = compare.performance_diff(run, ref)
    m = diff['metrics']
    assert m['p50_latency_ms']['run'] == 20_000 and m['p50_latency_ms']['reference'] == 10_000
    assert m['p50_latency_ms']['verdict'] == 'worse'
    assert m['p95_latency_ms']['delta'] == 28_000
    # case 2's last top-level LLM step is the 100 ms one: the sub-agent's 9999 ms step never counts
    assert m['p50_final_answer_ms']['run'] == 100 and m['p50_final_answer_ms']['cases'] == 2
    assert m['tool_calls']['run'] == 7 and m['tool_calls']['reference'] == 3
    assert m['agent_tokens']['run'] == 450 and m['agent_tokens']['reference'] == 300
    assert m['agent_cost']['run'] == pytest.approx(0.03)
    assert diff['slower_than_reference'] is True
    assert diff['not_comparable'] == []
    assert [c['latency_verdict'] for c in diff['cases']] == ['worse', 'worse']


def test_latency_within_noise(compare):
    # 10 s → 11.5 s is 15 %, and 1 s → 2.9 s is under the 2 s floor: both noise.
    run = _side(compare, {1: (_execution(11_500), _usage()), 2: (_execution(2_900), _usage())})
    ref = _side(compare, {1: (_execution(10_000), _usage()), 2: (_execution(1_000), _usage())})
    diff = compare.performance_diff(run, ref)
    assert [c['latency_verdict'] for c in diff['cases']] == ['within_noise', 'within_noise']
    assert diff['metrics']['p95_latency_ms']['verdict'] == 'within_noise'
    assert diff['slower_than_reference'] is False


def test_faster_reads_better(compare):
    run = _side(compare, {1: (_execution(5_000), _usage())})
    ref = _side(compare, {1: (_execution(30_000), _usage())})
    assert compare.performance_diff(run, ref)['metrics']['p50_latency_ms']['verdict'] == 'better'


def test_changed_and_one_sided_cases_are_left_out(compare):
    run = _side(compare, {1: (_execution(1_000), _usage()), 2: (_execution(90_000), _usage()),
                          3: (_execution(1_000), _usage())}, inputs={2: 'new question'})
    ref = _side(compare, {1: (_execution(1_000), _usage()), 2: (_execution(1_000), _usage()),
                          4: (_execution(1_000), _usage())})
    diff = compare.performance_diff(run, ref)
    assert diff['coverage']['changed'] == [2]
    assert diff['coverage']['only_in_run'] == [3] and diff['coverage']['only_in_reference'] == [4]
    assert diff['coverage']['matched'] == 1
    assert diff['metrics']['p95_latency_ms']['run'] == 1_000


def test_timeouts_stay_out_of_percentiles_and_are_listed(compare):
    run = _side(compare, {1: (_execution(120_000, status='timeout', recorded=False), _usage()),
                          2: (_execution(3_000), _usage())})
    ref = _side(compare, {1: (_execution(50_000), _usage()),
                          2: (_execution(120_000, status='timeout', recorded=False), _usage())})
    diff = compare.performance_diff(run, ref)
    assert diff['timeouts']['newly_timed_out'] == [1]
    assert diff['timeouts']['no_longer_timed_out'] == [2]
    assert diff['metrics']['p50_latency_ms']['cases'] == 0
    assert diff['metrics']['p50_latency_ms']['not_comparable'] == 'no_comparable_cases'
    assert diff['metrics']['p50_latency_ms']['verdict'] is None


def test_time_limit_change_withholds_latency_verdicts(compare):
    run = _side(compare, {1: (_execution(50_000), _usage())}, limit=300)
    ref = _side(compare, {1: (_execution(10_000), _usage())}, limit=120)
    diff = compare.performance_diff(run, ref)
    assert 'time_limit_changed' in diff['not_comparable']
    assert diff['metrics']['p50_latency_ms']['verdict'] is None
    assert diff['metrics']['p50_latency_ms']['not_comparable'] == 'time_limit_changed'
    assert diff['timeouts']['not_comparable'] == 'time_limit_changed'
    assert diff['cases'][0]['latency_verdict'] is None
    assert diff['metrics']['agent_tokens']['verdict'] == 'unchanged'
    assert diff['slower_than_reference'] is False


def test_unpriced_or_pending_cost_is_excluded_not_zero(compare):
    run = _side(compare, {1: (_execution(1_000), _usage(cost=None, source='unpriced')),
                          2: (_execution(1_000), _usage(cost=0.5)),
                          3: (_execution(1_000), _usage(cost=0.0, source='pending'))})
    ref = _side(compare, {1: (_execution(1_000), _usage(cost=0.1)),
                          2: (_execution(1_000), _usage(cost=0.4)),
                          3: (_execution(1_000), _usage(cost=0.1))})
    cost = compare.performance_diff(run, ref)['metrics']['agent_cost']
    assert cost['cases'] == 1 and cost['excluded'] == 2
    assert cost['run'] == pytest.approx(0.5) and cost['reference'] == pytest.approx(0.4)


def test_unrecorded_usage_is_excluded_from_tokens(compare):
    run = _side(compare, {1: (_execution(1_000), _usage(state='not_recorded')), 2: (_execution(1_000), _usage())})
    ref = _side(compare, {1: (_execution(1_000), _usage()), 2: (_execution(1_000), _usage())})
    tokens = compare.performance_diff(run, ref)['metrics']['agent_tokens']
    assert tokens['cases'] == 1 and tokens['excluded'] == 1


def test_different_dataset_is_flagged(compare):
    run = _side(compare, {1: (_execution(1_000), _usage())}, dataset_id=1)
    ref = _side(compare, {1: (_execution(1_000), _usage())}, dataset_id=2)
    assert 'different_dataset' in compare.performance_diff(run, ref)['not_comparable']
