"""Performance P-1: the suite's per-case time limit and how a timed-out case is scored.

``meta.case_time_limit_seconds`` is frozen into the run snapshot (resolved, never null) and becomes
the agent's ``run_agent(timeout=...)``. A case that reaches it is an outcome of the agent under
test, so its bindings are skipped with a time-limit note rather than errored, and the run counts
it (performance design §3.1, §3.2).
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def runner(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    load_utils_module(utils_path, 'evaluation_execution')
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def orchestration(utils_path, runner):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


SNAPSHOT_COMMON = dict(dimensions=[], bindings=[], cases=[], application_id=1, application_version_id=2)

BINDINGS = [
    {'engine': 'ai', 'dimension_id': 1, 'evidence_scope': {}},
    {'engine': 'code', 'dimension_id': 2, 'evidence_scope': {'trajectory': True}},
    {'engine': 'human', 'dimension_id': 3, 'evidence_scope': {}},
]


def _outcome(status, error):
    return lambda case: {'status': status, 'output': None, 'error': error, 'structure': None,
                         'execution': {'trajectory_state': 'not_recorded'}}


def test_default_matches_the_runner_default(orchestration, runner):
    assert orchestration.DEFAULT_CASE_TIME_LIMIT_SECONDS == runner.DEFAULT_AGENT_TIMEOUT


def test_snapshot_freezes_the_resolved_limit(orchestration):
    frozen = orchestration.build_run_snapshot(
        suite={'id': 1, 'name': 's', 'case_time_limit_seconds': 300}, **SNAPSHOT_COMMON)
    assert frozen['suite']['case_time_limit_seconds'] == 300
    for suite in ({'id': 1, 'name': 's'}, {'id': 1, 'name': 's', 'case_time_limit_seconds': None}):
        frozen = orchestration.build_run_snapshot(suite=suite, **SNAPSHOT_COMMON)
        assert frozen['suite']['case_time_limit_seconds'] == 120


def test_snapshot_from_before_the_key_runs_with_the_default(orchestration):
    assert orchestration.case_time_limit({'suite': {'steps_limit': 3}}) == 120
    assert orchestration.case_time_limit({}) == 120
    assert orchestration.case_time_limit({'suite': {'case_time_limit_seconds': 45}}) == 45


def test_agent_runner_passes_the_frozen_limit(orchestration, monkeypatch):
    seen = []
    runner_mod = sys.modules['plugins.elitea_core.utils.evaluation_agent_runner']
    app_utils = type(sys)('plugins.elitea_core.utils.application_utils')
    app_utils.get_application_version_details_expanded = lambda *a: {'agent_type': 'openai'}
    monkeypatch.setitem(sys.modules, app_utils.__name__, app_utils)
    monkeypatch.setattr(runner_mod, 'run_agent',
                        lambda *a, **kw: seen.append(kw['timeout']) or {'status': 'ok'})
    for suite in ({'case_time_limit_seconds': 600}, {}):
        run = orchestration._make_agent_runner(
            1, {'suite': suite, 'application_id': 1, 'application_version_id': 2, 'dataset_id': 3},
            user_id=1)
        run({'input': 'q'})
    assert seen == [600, 120]


def test_run_agent_stops_at_the_limit(runner):
    """The limit is the predict await; reaching it is the ``timeout`` outcome."""
    def predict(**kwargs):
        assert kwargs['await_task_timeout'] == 30
        return {'task_id': 't-1'}  # no 'result': the await ran out

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict, timeout=30)
    assert out['status'] == 'timeout'
    assert out['error'] == 'agent timed out after 30s'


def test_timed_out_case_skips_every_binding(orchestration):
    snapshot = {'suite': {'case_time_limit_seconds': 300}, 'bindings': BINDINGS, 'cases': []}
    case, rows = orchestration.run_one_case(
        {'id': 1, 'input': 'q'}, snapshot, agent_runner=_outcome('timeout', 'agent timed out after 300s'))

    assert case['_agent_timeout'] == 300
    assert case['_execution']['status'] == 'timeout'
    assert [(r['engine'], r['status']) for r in rows] == [
        ('ai', 'skipped'), ('code', 'skipped'), ('human', 'skipped')]
    assert {r['verdict']['note'] for r in rows} == {
        'Skipped: the agent reached the case time limit (300 s).'}
    assert all(r['native_score'] is None for r in rows)


def test_timed_out_case_dispatches_no_scorer(orchestration):
    def boom(*a, **kw):
        raise AssertionError('no judge or code dispatch for a timed-out case')

    snapshot = {'suite': {}, 'bindings': BINDINGS, 'cases': []}
    _case, rows = orchestration.run_one_case(
        {'id': 1, 'input': 'q'}, snapshot, agent_runner=_outcome('timeout', 'agent timed out after 120s'),
        ai_scorer=boom, code_scorer=boom)
    assert {r['status'] for r in rows} == {'skipped'}


@pytest.mark.parametrize('status', ['error', 'predict_error', 'empty', 'unsupported'])
def test_other_failures_still_error(orchestration, status):
    snapshot = {'suite': {}, 'bindings': BINDINGS, 'cases': []}
    case, rows = orchestration.run_one_case({'id': 1, 'input': 'q'}, snapshot,
                                            agent_runner=_outcome(status, f'{status} reason'))
    assert '_agent_timeout' not in case
    assert [r['status'] for r in rows] == ['error', 'error', 'pending_human']


def test_timed_out_case_leaves_the_headline_to_the_others(orchestration):
    """Skipped rows carry no score, so the headline covers the cases that finished."""
    snapshot = {'suite': {}, 'cases': [{'id': 1, 'input': 'slow'}, {'id': 2, 'input': 'fast'}],
                'bindings': [{'engine': 'code', 'dimension_id': 7, 'evidence_scope': {}}],
                'dimensions': {'7': {'name': 'd', 'return_contract': 'bool', 'scale_min': 0,
                                     'scale_max': 1, 'polarity': 'higher_better'}}}

    def agent(case):
        if case['input'] == 'slow':
            return _outcome('timeout', 'agent timed out after 120s')(case)
        return {'status': 'ok', 'output': 'a', 'structure': None, 'execution': None}

    out = orchestration.orchestrate_run(snapshot, agent_runner=agent,
                                        code_scorer=lambda b, ev: {'status': 'scored', 'native_score': 1.0, 'passed': True})
    statuses = {r['dataset_case_id']: r['status'] for r in out['results']}
    assert statuses == {1: 'skipped', 2: 'ok'}
    assert out['headline_score'] is not None


# --- P-2: the latency dimensions score a timed-out case as exceeded ------------------------

def _latency_snapshot(limit):
    dims = {'1': {'name': 'Accuracy'},
            '2': {'name': 'trajectory.tool_errors', 'builtin_check': 'trajectory.tool_errors',
                  'return_contract': 'number', 'scale_min': 0.0, 'scale_max': 10.0,
                  'polarity': 'lower_better'},
            '4': {'name': 'performance.latency', 'builtin_check': 'performance.latency',
                  'return_contract': 'number', 'scale_min': 0.0, 'scale_max': 1.0,
                  'polarity': 'lower_better'},
            '5': {'name': 'performance.final_answer_time',
                  'builtin_check': 'performance.final_answer_time', 'return_contract': 'number',
                  'scale_min': 0.0, 'scale_max': 1.0, 'polarity': 'lower_better'}}
    bindings = [*BINDINGS,
                {'engine': 'code', 'dimension_id': 4, 'evidence_scope': {'trajectory': True}},
                {'engine': 'code', 'dimension_id': 5, 'evidence_scope': {'trajectory': True}}]
    return {'suite': {'case_time_limit_seconds': limit}, 'dimensions': dims,
            'bindings': bindings, 'cases': []}


def test_timed_out_case_is_exceeded_on_the_latency_dimensions(orchestration):
    def boom(*a, **kw):
        raise AssertionError('the exceeded rows are written without a dispatch')

    _case, rows = orchestration.run_one_case(
        {'id': 1, 'input': 'q'}, _latency_snapshot(45),
        agent_runner=_outcome('timeout', 'agent timed out after 45s'), code_scorer=boom)
    by_dim = {r['dimension_id']: r for r in rows}
    for dim_id in (4, 5):
        row = by_dim[dim_id]
        assert (row['status'], row['native_score'], row['normalized_score']) == ('ok', 1.0, 0.0)
        assert row['verdict']['status'] == 'exceeded'
        assert '45 s' in row['verdict']['note']
    assert [by_dim[d]['status'] for d in (1, 2, 3)] == ['skipped'] * 3


def test_the_latency_script_sees_the_frozen_limit(orchestration):
    seen = []
    snapshot = _latency_snapshot(200)
    case = {'id': 1, 'input': 'q', 'output': 'a',
            '_execution': {'trajectory_state': 'recorded', 'trajectory': {'steps': []},
                           'metrics': {'latency_ms': 50000}}}
    orchestration.assemble_case_results(
        case, snapshot,
        code_scorer=lambda b, ev: seen.append(ev['trajectory']['time_limit_seconds'])
        or {'status': 'na'})
    assert seen == [200, 200, 200]


def test_snapshot_keeps_the_builtin_check(orchestration):
    # The exceeded rows key on the frozen dimension's builtin_check, so the snapshot must carry it.
    frozen = orchestration.build_run_snapshot(
        suite={'id': 1, 'name': 's'}, **{**SNAPSHOT_COMMON, 'dimensions': [
            {'id': 4, 'name': 'performance.latency', 'builtin_check': 'performance.latency'},
            {'id': 1, 'name': 'Accuracy'}]})
    assert frozen['dimensions']['4']['builtin_check'] == 'performance.latency'
    assert frozen['dimensions']['1']['builtin_check'] is None
