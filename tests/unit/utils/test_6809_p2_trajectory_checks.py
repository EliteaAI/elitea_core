"""#6809 P2 item 5: the built-in trajectory checks (design §5.2).

The scripts are executed for real here, through the same prelude the sandbox gets, so the
``match`` semantics under test are the shipped ones. They must also pass the author-time code
screen like any user script. A check that needs a missing reference assigns ``result = 'na'``,
which becomes a skipped row.

Run via:
    python tests/run_tests.py unit/utils/test_6809_p2_trajectory_checks.py -v
"""

import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

checks = sys.modules['plugins.elitea_core.utils.evaluation_trajectory_checks']


@pytest.fixture(scope='module')
def cv(utils_path):
    load_utils_module(utils_path, 'evaluation_code_screen')
    return load_utils_module(utils_path, 'code_validation')


@pytest.fixture(scope='module')
def screen(utils_path):
    return load_utils_module(utils_path, 'evaluation_code_screen')


@pytest.fixture(scope='module')
def orch(utils_path, cv):
    load_utils_module(utils_path, 'evaluation_ai_judge')
    load_utils_module(utils_path, 'evaluation_usage')
    load_utils_module(utils_path, 'evaluation_scoring')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


def _tool(name, args=None, status='ok'):
    return {'kind': 'tool', 'tool_name': name, 'tool_inputs': args, 'status': status,
            'is_error': status == 'error'}


def _traj(*steps, **metrics):
    return {'steps': [{'kind': 'llm', 'model': 'm'}, *steps],
            'tool_sequence': [s['tool_name'] for s in steps],
            'truncated': False, 'pause': metrics.pop('pause', None), 'metrics': metrics}


_MISSING = object()


def _local_executor(prelude: str) -> dict:
    """Stand-in for the sandbox: run prelude + script + epilogue, return the result."""
    ns = {}
    lines = prelude.splitlines()
    exec('\n'.join(lines[:-1]), ns)  # noqa: S102 - trusted, shipped test scripts
    return {'status': 'success', 'result': ns.get('result'), 'stdout': '', 'stderr': '',
            'execution_time': 0.0}


def run(cv, key, trajectory=_MISSING, expected=_MISSING):
    kwargs = {}
    if trajectory is not _MISSING:
        kwargs['trajectory'] = trajectory
    if expected is not _MISSING:
        kwargs['expected_trajectory'] = expected
    prelude = cv.build_validation_prelude(checks.script_for(key), output='o', **kwargs)
    return _local_executor(prelude)['result']


def ref(*tools, match='superset', **extra):
    return {'match': match, 'tools': [t if isinstance(t, dict) else {'name': t} for t in tools],
            'forbidden': [], 'allow_repeat': [], **extra}


# --- shipped scripts are ordinary, screen-clean code --------------------------------------

@pytest.mark.parametrize('key', sorted(checks.CHECKS))
def test_every_script_passes_the_code_screen(screen, key):
    assert screen.screen_validation_code(checks.script_for(key)) == []


@pytest.mark.parametrize('key', sorted(checks.CHECKS))
def test_every_check_is_na_without_a_trajectory(cv, key):
    assert run(cv, key) == 'na'
    assert run(cv, key, trajectory=None, expected=ref('a')) == 'na'


# --- tool_match: the match modes ----------------------------------------------------------

AB = _traj(_tool('a'), _tool('b'))


@pytest.mark.parametrize('mode,expected_tools,trajectory,score', [
    # superset (default): every expected call happens; extras are fine
    ('superset', ['a'], AB, 1.0),
    ('superset', ['a', 'c'], AB, 0.5),
    ('superset', ['b', 'a'], AB, 1.0),
    ('superset', ['a', 'a'], _traj(_tool('a')), 0.5),  # one call satisfies one expectation
    # subset: every actual call is an expected one
    ('subset', ['a', 'b', 'c'], AB, 1.0),
    ('subset', ['a'], AB, 0.5),
    ('subset', ['a'], _traj(), 1.0),
    # any_order: F1 of expected vs actual
    ('any_order', ['b', 'a'], AB, 1.0),
    ('any_order', ['a'], AB, round(2 / 3, 4)),
    ('any_order', [], _traj(), 1.0),
    ('any_order', [], AB, 0.0),
    # in_order: expected calls in order, gaps allowed
    ('in_order', ['a', 'b'], _traj(_tool('a'), _tool('x'), _tool('b')), 1.0),
    ('in_order', ['b', 'a'], AB, 0.5),
    # exact: same calls, same order, nothing else
    ('exact', ['a', 'b'], AB, 1.0),
    ('exact', ['b', 'a'], AB, 0.0),
    ('exact', ['a'], AB, 0.0),
    ('exact', [], _traj(), 1.0),
])
def test_tool_match_modes(cv, mode, expected_tools, trajectory, score):
    assert run(cv, 'trajectory.tool_match', trajectory, ref(*expected_tools, match=mode)) == score


@pytest.mark.parametrize('mode', ['superset', 'in_order'])
def test_tool_match_is_na_when_nothing_is_required(cv, mode):
    assert run(cv, 'trajectory.tool_match', AB, ref(match=mode, forbidden=['x'])) == 'na'


def test_tool_match_is_na_without_reference(cv):
    assert run(cv, 'trajectory.tool_match', AB) == 'na'


def test_args_subset_and_exact(cv):
    trajectory = _traj(_tool('jira', {'project': 'EL', 'limit': 5}))
    subset = {'name': 'jira', 'args': {'project': 'EL'}, 'args_match': 'subset'}
    exact = {'name': 'jira', 'args': {'project': 'EL'}, 'args_match': 'exact'}
    wrong = {'name': 'jira', 'args': {'project': 'XX'}, 'args_match': 'subset'}
    assert run(cv, 'trajectory.tool_match', trajectory, ref(subset)) == 1.0
    assert run(cv, 'trajectory.tool_match', trajectory, ref(exact)) == 0.0
    assert run(cv, 'trajectory.tool_match', trajectory, ref(wrong)) == 0.0
    full = {'name': 'jira', 'args': {'limit': 5, 'project': 'EL'}, 'args_match': 'exact'}
    assert run(cv, 'trajectory.tool_match', trajectory, ref(full)) == 1.0


def test_args_compare_against_capped_string_inputs(cv):
    """Inputs over the structured cap are stored as a string: JSON is parsed, anything else fails."""
    spec = {'name': 'jira', 'args': {'project': 'EL'}, 'args_match': 'subset'}
    as_json = _traj(_tool('jira', '{"project": "EL", "q": "x"}'))
    assert run(cv, 'trajectory.tool_match', as_json, ref(spec)) == 1.0
    assert run(cv, 'trajectory.tool_match', _traj(_tool('jira', 'project=EL...')), ref(spec)) == 0.0


def test_args_assignment_finds_the_best_matching(cv):
    """Greedy would spend the 'a' call on the args-free expectation; the matching does not."""
    trajectory = _traj(_tool('a', {'k': 1}), _tool('a', {'k': 2}))
    expected = ref('a', {'name': 'a', 'args': {'k': 1}, 'args_match': 'subset'})
    assert run(cv, 'trajectory.tool_match', trajectory, expected) == 1.0


def test_tool_match_falls_back_to_tool_sequence(cv):
    trajectory = {'steps': [], 'tool_sequence': ['a', 'b'], 'metrics': {}}
    assert run(cv, 'trajectory.tool_match', trajectory, ref('a', 'b', match='exact')) == 1.0


# --- the other checks ---------------------------------------------------------------------

def test_forbidden_tools(cv):
    expected = ref(forbidden=['delete'])
    assert run(cv, 'trajectory.forbidden_tools', AB, expected) is True
    assert run(cv, 'trajectory.forbidden_tools', _traj(_tool('delete')), expected) is False
    assert run(cv, 'trajectory.forbidden_tools', AB, ref('a')) == 'na'
    assert run(cv, 'trajectory.forbidden_tools', AB) == 'na'


def test_tool_errors_reads_the_counter_without_a_reference(cv):
    assert run(cv, 'trajectory.tool_errors', _traj(_tool('a'), tool_errors=3)) == 3
    assert run(cv, 'trajectory.tool_errors', _traj(_tool('a', status='error'))) == 1


def test_redundant_calls(cv):
    trajectory = _traj(_tool('poll', {'id': 1}), _tool('poll', {'id': 1}), _tool('get', {'x': 1}),
                       _tool('get', {'x': 1}), redundant_calls=2)
    assert run(cv, 'trajectory.redundant_calls', trajectory) == 2
    assert run(cv, 'trajectory.redundant_calls', trajectory, ref('a')) == 2
    assert run(cv, 'trajectory.redundant_calls', trajectory, ref(allow_repeat=['poll'])) == 1


def test_redundant_recount_treats_a_repeat_after_failure_as_retry(cv):
    trajectory = _traj(_tool('get', {'x': 1}, status='error'), _tool('get', {'x': 1}),
                       _tool('get', {'x': 1}), _tool('poll'), _tool('poll'))
    assert run(cv, 'trajectory.redundant_calls', trajectory, ref(allow_repeat=['poll'])) == 1


def test_step_budget(cv):
    trajectory = _traj(_tool('a'), _tool('b'), tool_calls=2)
    assert run(cv, 'trajectory.step_budget', trajectory, ref(max_tool_calls=2)) is True
    assert run(cv, 'trajectory.step_budget', trajectory, ref(max_tool_calls=1)) is False
    assert run(cv, 'trajectory.step_budget', trajectory, ref('a')) == 'na'


def test_step_limit_hit(cv):
    assert run(cv, 'trajectory.step_limit_hit', _traj(step_limit_hit=False)) is True
    assert run(cv, 'trajectory.step_limit_hit', _traj(step_limit_hit=True)) is False
    assert run(cv, 'trajectory.step_limit_hit', _traj()) == 'na'


def test_guardrail_events(cv):
    assert run(cv, 'trajectory.guardrail_events', _traj(guardrail_events=2)) == 2
    assert run(cv, 'trajectory.guardrail_events',
               _traj(_tool('a', status='blocked'), pause={'pause_type': 'hitl'})) == 2


# --- the 'na' contract --------------------------------------------------------------------

def test_na_result_is_a_skip_not_an_error(cv):
    for contract in ('bool', 'number'):
        verdict = cv.map_execution_result({'status': 'success', 'result': 'na', 'stdout': 'why'},
                                          dimension_id=1, name='d', return_contract=contract)
        assert verdict['status'] == 'na'
        assert verdict['stdout'] == 'why'
        assert 'reference' in verdict['error']


def test_other_strings_are_still_errors(cv):
    verdict = cv.map_execution_result({'status': 'success', 'result': 'NA'},
                                      dimension_id=1, name='d', return_contract='bool')
    assert verdict['status'] == 'error'


def test_legacy_na_verdict_message_unchanged(cv):
    assert 'expected_output' in cv.na_verdict(1, 'd')['error']


# --- through the orchestrator -------------------------------------------------------------

def _snapshot():
    dims, bindings = {}, []
    for i, (key, check) in enumerate(sorted(checks.CHECKS.items()), start=1):
        code, contract = checks.builtin_code({'builtin_check': key})
        dims[str(i)] = {'name': key, 'scale_type': check['scale_type'],
                        'scale_min': check['scale_min'], 'scale_max': check['scale_max'],
                        'polarity': check['polarity'], 'code': code, 'return_contract': contract}
        bindings.append({'dimension_id': i, 'engine': 'code', 'weight': check['default_weight'],
                         'evidence_scope': dict(checks.TRAJECTORY_SCOPE)})
    return {'dimensions': dims, 'bindings': bindings}


def _case(expected=None):
    trajectory = _traj(_tool('a'), _tool('b'), _tool('b'), tool_calls=3, tool_errors=0,
                       redundant_calls=1, step_limit_hit=False, guardrail_events=0)
    metrics = trajectory.pop('metrics')
    case = {'id': 7, 'input': 'i', 'output': 'o',
            '_execution': {'trajectory_state': 'recorded', 'trajectory': trajectory,
                           'metrics': metrics, 'case_index': 0}}
    if expected:
        case['expected_trajectory'] = expected
    return case


def test_orchestrated_rows_score_and_skip(orch):
    snapshot = _snapshot()
    scorer = orch._make_code_scorer(snapshot, _local_executor)
    names = {b['dimension_id']: snapshot['dimensions'][str(b['dimension_id'])]['name']
             for b in snapshot['bindings']}

    rows = orch.assemble_case_results(_case(), snapshot, code_scorer=scorer)
    by_name = {names[r['dimension_id']]: r for r in rows}
    # Reference-free checks score; reference-based ones skip on a case without one.
    assert by_name['trajectory.tool_errors']['normalized_score'] == 100.0
    assert by_name['trajectory.redundant_calls']['normalized_score'] == 90.0
    assert by_name['trajectory.step_limit_hit']['normalized_score'] == 100.0
    for key in ('trajectory.tool_match', 'trajectory.forbidden_tools', 'trajectory.step_budget'):
        assert by_name[key]['status'] == 'skipped', key

    expected = ref('a', 'c', forbidden=['b'], max_tool_calls=5, allow_repeat=['b'])
    rows = orch.assemble_case_results(_case(expected), snapshot, code_scorer=scorer)
    by_name = {names[r['dimension_id']]: r for r in rows}
    assert by_name['trajectory.tool_match']['native_score'] == 0.5
    assert by_name['trajectory.tool_match']['normalized_score'] == 50.0
    assert by_name['trajectory.forbidden_tools']['normalized_score'] == 0.0
    assert by_name['trajectory.step_budget']['normalized_score'] == 100.0
    assert by_name['trajectory.redundant_calls']['native_score'] == 0  # 'b' may repeat
    assert all(r['status'] == 'ok' for r in rows)


# --- registry seed ------------------------------------------------------------------------

def test_registry_seed_shape():
    rows = checks.registry_seed()
    assert [r['name'] for r in rows] == list(checks.CHECKS)
    weighted = {r['name'] for r in rows if r['default_weight']}
    assert weighted == {'trajectory.tool_match', 'trajectory.forbidden_tools'}
    for row in rows:
        assert row['allowed_engines'] == ['code']
        assert checks.builtin_key(row['meta']) == row['name']
        assert row['meta']['default_evidence_scope']['trajectory'] is True
        if not row['default_weight']:
            assert row['default_target'] is not None and row['default_target_operator']


def test_builtin_lookup_ignores_unknown_keys():
    assert checks.builtin_code(None) is None
    assert checks.builtin_code({'builtin_check': 'trajectory.nope'}) is None
    code, contract = checks.builtin_code({'builtin_check': 'trajectory.tool_match'})
    assert contract == 'number' and 'result' in code
