"""#7261: a small partial overlap with the reference withholds the latency verdicts.

A grown dataset leaves only a few cases that both runs measured; their p50/p95 are not a sample of
the run, so the verdicts are withheld (``few_matched_cases``) while the figures, tokens, cost and
timeouts stay compared.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

LATENCY = ('p50_latency_ms', 'p95_latency_ms', 'p50_final_answer_ms')


@pytest.fixture(scope='module')
def compare(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    load_utils_module(utils_path, 'evaluation_execution')
    return load_utils_module(utils_path, 'evaluation_run_compare')


def _case(latency, *, status='ok', tokens=(100, 50)):
    recorded = status != 'timeout'
    execution = {'case_index': 0, 'status': status,
                 'trajectory_state': 'recorded' if recorded else 'not_recorded',
                 'metrics': {'latency_ms': latency, 'tool_calls': 2, 'final_answer_ms': latency // 2},
                 'trajectory': None}
    usage = {'usage_state': 'recorded', 'input_tokens': tokens[0], 'output_tokens': tokens[1],
             'cost': 0.01, 'cost_source': 'runtime:costs-catalog'}
    return execution, usage


def _side(compare, cases, *, limit=120):
    snapshot = {'cases': [{'id': cid, 'input': f'q{cid}', 'variables': {}} for cid in cases]}
    return {'snapshot': snapshot, 'dataset_id': 7, 'case_time_limit_seconds': limit,
            'cases': {cid: compare.case_view(*pair) for cid, pair in cases.items()}}


def _diff(compare, run_ids, ref_ids, *, run_latency=30_000, ref_latency=10_000, **kw):
    run = _side(compare, {cid: _case(run_latency) for cid in run_ids}, **kw.get('run', {}))
    ref = _side(compare, {cid: _case(ref_latency) for cid in ref_ids}, **kw.get('ref', {}))
    return compare.performance_diff(run, ref)


def test_three_of_thirty_withholds_latency_verdicts_and_keeps_figures(compare):
    diff = _diff(compare, range(1, 31), range(1, 4))
    assert diff['coverage']['matched'] == 3 and diff['coverage']['run_cases'] == 30
    assert 'few_matched_cases' in diff['not_comparable']
    for key in LATENCY:
        metric = diff['metrics'][key]
        assert metric['verdict'] is None and metric['not_comparable'] == 'few_matched_cases'
    assert diff['metrics']['p50_latency_ms']['run'] == 30_000
    assert diff['metrics']['p50_latency_ms']['reference'] == 10_000
    assert diff['metrics']['p50_latency_ms']['delta'] == 20_000
    assert diff['slower_than_reference'] is False
    assert diff['coverage']['min_matched'] == {'cases': 5, 'share': 0.5}


def test_tokens_cost_and_tool_calls_keep_their_verdicts(compare):
    diff = _diff(compare, range(1, 31), range(1, 4))
    for key in ('agent_tokens', 'agent_cost', 'tool_calls'):
        assert diff['metrics'][key]['verdict'] == 'unchanged'
        assert diff['metrics'][key]['not_comparable'] is None


def test_a_newly_timed_out_case_still_marks_the_run_slower(compare):
    run_cases = {cid: _case(10_000) for cid in range(1, 31)}
    run_cases[2] = _case(120_000, status='timeout')
    run = _side(compare, run_cases)
    ref = _side(compare, {cid: _case(10_000) for cid in range(1, 4)})
    diff = compare.performance_diff(run, ref)
    assert 'few_matched_cases' in diff['not_comparable']
    assert diff['timeouts']['not_comparable'] is None
    assert diff['timeouts']['newly_timed_out'] == [2]
    assert diff['slower_than_reference'] is True


@pytest.mark.parametrize('run_n, ref_n, withheld', [
    (10, 5, False),   # 5 of 10: both thresholds met
    (6, 4, True),     # share 0.67 but fewer than 5 cases
    (12, 5, True),    # 5 cases but under half the run
    (3, 3, False),    # full overlap is always comparable
    (3, 30, False),   # removed cases don't count against the run
])
def test_thresholds(compare, run_n, ref_n, withheld):
    diff = _diff(compare, range(1, run_n + 1), range(1, ref_n + 1))
    assert ('few_matched_cases' in diff['not_comparable']) is withheld
    assert (diff['metrics']['p95_latency_ms']['verdict'] is None) is withheld
    assert diff['slower_than_reference'] is (not withheld)


def test_time_limit_change_keeps_its_reason_on_the_metrics(compare):
    diff = _diff(compare, range(1, 31), range(1, 4), run={'limit': 300}, ref={'limit': 120})
    assert diff['not_comparable'] == ['time_limit_changed', 'few_matched_cases']
    assert diff['metrics']['p50_latency_ms']['not_comparable'] == 'time_limit_changed'
    assert diff['timeouts']['not_comparable'] == 'time_limit_changed'


def test_no_matched_cases_is_not_reported_as_few(compare):
    diff = _diff(compare, range(1, 4), range(10, 13))
    assert diff['not_comparable'] == []
    assert diff['metrics']['p50_latency_ms']['not_comparable'] == 'no_comparable_cases'
