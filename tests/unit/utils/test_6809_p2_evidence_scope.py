"""#6809 P2 item 3: the ``trajectory`` and ``usage`` evidence scopes (design §5.1).

A binding that turns either scope on sees the case's recorded trajectory / agent tokens: code
scripts get ``trajectory`` / ``expected_trajectory`` / ``usage`` variables, the judge gets a compact
rendering. A case with nothing recorded skips the binding instead of scoring empty data. The result
row stores a reference and digest of the trajectory, not a second copy. Bindings that leave both
scopes off see exactly what they saw before.
"""

import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def judge(utils_path):
    return load_utils_module(utils_path, 'evaluation_ai_judge')


@pytest.fixture(scope='module')
def cv(utils_path):
    load_utils_module(utils_path, 'evaluation_code_screen')
    return load_utils_module(utils_path, 'code_validation')


@pytest.fixture(scope='module')
def orch(utils_path, judge, cv):
    load_utils_module(utils_path, 'evaluation_usage')
    load_utils_module(utils_path, 'evaluation_scoring')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


TRAJECTORY = {
    'steps': [
        {'kind': 'llm', 'model': 'gpt-4o', 'tokens': {'in': 120, 'out': 14},
         'planned_tools': ['jira_search']},
        {'kind': 'tool', 'tool_name': 'jira_search', 'tool_inputs': {'jql': 'project = EL'},
         'tool_output': 'x' * 2000, 'status': 'ok', 'is_error': False},
        {'kind': 'tool', 'tool_name': 'post_comment', 'parent_agent': 'Writer',
         'tool_inputs': {'id': 1}, 'tool_output': None, 'status': 'error', 'is_error': True,
         'error': 'forbidden'},
    ],
    'tool_sequence': ['jira_search', 'post_comment'],
    'truncated': False,
    'pause': None,
    'source': 'predict_sio',
}
METRICS = {'llm_calls': 1, 'tool_calls': 2, 'tool_errors': 1, 'step_limit_hit': False}
AGENT_USAGE = {
    'usage_state': 'recorded', 'usage_state_reason': None, 'token_source': 'provider',
    'model_name': 'gpt-4o', 'models': {'gpt-4o': {'input_tokens': 120, 'output_tokens': 14}},
    'input_tokens': 120, 'output_tokens': 14, 'cache_read_tokens': 0,
    'cache_creation_tokens': 0, 'reasoning_tokens': 0,
}


def _case(recorded=True, usage=True, **extra):
    case = {'id': 7, 'input': 'find EL bugs', 'output': 'done', **extra}
    if recorded is not None:
        case['_execution'] = {
            'trajectory_state': 'recorded' if recorded else 'not_recorded',
            'trajectory_state_reason': None if recorded else 'no_tool_calls_field',
            'trajectory': TRAJECTORY if recorded else None,
            'metrics': METRICS if recorded else {}, 'status': 'ok', 'case_index': 3,
        }
    if usage:
        case['_usage'] = {'agent': AGENT_USAGE}
    return case


def _snapshot(*bindings):
    dims = {str(b['dimension_id']): {'name': f"d{b['dimension_id']}", 'description': 'rubric',
                                     'scale_type': 'binary', 'return_contract': 'bool',
                                     'code': 'result = True'}
            for b in bindings}
    return {'bindings': list(bindings), 'dimensions': dims}


# --- select_evidence -------------------------------------------------------------------------

def test_scopes_off_leave_evidence_unchanged(orch):
    """Back-compat: a pre-P2 binding's evidence is byte-identical with a trajectory on the case."""
    case = _case(expected_output='e', expected_trajectory={'match': 'in_order'})
    assert orch.select_evidence(case, {'input': True, 'output': True}) == {
        'output': 'done', 'expected_output': 'e', 'input': 'find EL bugs'}
    assert orch.select_evidence(case, {'structure': True, 'trajectory': False, 'usage': False}) == {
        'output': 'done', 'expected_output': 'e', 'input': 'find EL bugs', 'structure': None}


def test_trajectory_scope_attaches_recorded_trajectory(orch):
    evidence = orch.select_evidence(_case(), {'trajectory': True, 'output': False, 'input': False})
    assert list(evidence) == ['trajectory']
    traj = evidence['trajectory']
    assert traj['steps'] == TRAJECTORY['steps']
    assert traj['tool_sequence'] == ['jira_search', 'post_comment']
    assert traj['metrics'] == METRICS
    assert traj['case_index'] == 3


def test_expected_trajectory_rides_with_trajectory_scope(orch):
    expected = {'match': 'in_order', 'tools': [{'name': 'jira_search'}]}
    evidence = orch.select_evidence(_case(expected_trajectory=expected), {'trajectory': True})
    assert evidence['expected_trajectory'] == expected
    assert 'expected_trajectory' not in orch.select_evidence(
        _case(expected_trajectory=expected), {'output': True})


@pytest.mark.parametrize('recorded', [False, None])
def test_trajectory_is_none_when_not_recorded(orch, recorded):
    """``not_recorded`` runs and on-demand cases (no ``_execution``) have nothing to score."""
    assert orch.select_evidence(_case(recorded=recorded), {'trajectory': True})['trajectory'] is None


def test_usage_scope_attaches_agent_tokens_without_cost(orch):
    case = _case()
    case['_usage'] = {'agent': {**AGENT_USAGE, 'reasoning_tokens': 6}}
    usage = orch.select_evidence(case, {'usage': True})['usage']
    assert usage['input_tokens'] == 120 and usage['output_tokens'] == 14
    assert usage['total_tokens'] == 140  # input + output + reasoning, as the Consumption card
    assert usage['model_name'] == 'gpt-4o' and usage['token_source'] == 'provider'
    assert 'usage_state' not in usage and 'cost' not in usage


def test_usage_is_none_when_not_recorded(orch):
    assert orch.select_evidence(_case(usage=False), {'usage': True})['usage'] is None
    case = _case(usage=False)
    case['_usage'] = {'agent': {**AGENT_USAGE, 'usage_state': 'not_recorded'}}
    assert orch.select_evidence(case, {'usage': True})['usage'] is None


# --- stored evidence -------------------------------------------------------------------------

def test_result_row_stores_trajectory_reference_and_digest(orch):
    evidence = orch.select_evidence(_case(), {'trajectory': True})
    stored = orch.stored_evidence(evidence)['trajectory']
    assert stored['ref'] == 'eval_case_execution' and stored['case_index'] == 3
    assert stored['steps'] == 3
    assert len(stored['digest']) == 64
    assert 'x' * 2000 not in json.dumps(stored)
    assert orch.stored_evidence(orch.select_evidence(_case(), {'trajectory': True}))['trajectory'][
        'digest'] == stored['digest']


def test_stored_evidence_passes_other_evidence_through(orch):
    evidence = {'output': 'o', 'input': 'i', 'trajectory': None}
    assert orch.stored_evidence(evidence) is evidence


# --- assemble_case_results -------------------------------------------------------------------

def test_code_binding_skipped_without_trajectory(orch):
    snapshot = _snapshot({'dimension_id': 1, 'engine': 'code', 'evidence_scope': {'trajectory': True}})
    calls = []
    rows = orch.assemble_case_results(_case(recorded=False), snapshot,
                                      code_scorer=lambda b, e: calls.append(e) or {'status': 'scored'})
    assert calls == []
    assert rows[0]['status'] == 'skipped'
    assert 'no trajectory' in rows[0]['verdict']['note']


def test_ai_group_skipped_without_usage(orch):
    snapshot = _snapshot({'dimension_id': 1, 'evidence_scope': {'usage': True}},
                         {'dimension_id': 2, 'evidence_scope': {'usage': True}})
    calls = []
    rows = orch.assemble_case_results(_case(usage=False), snapshot,
                                      ai_scorer=lambda e, d: calls.append(e) or [])
    assert calls == []
    assert [r['status'] for r in rows] == ['skipped', 'skipped']
    assert 'token usage' in rows[0]['verdict']['note']


def test_ai_scorer_gets_rendered_trajectory_and_row_keeps_reference(orch):
    snapshot = _snapshot({'dimension_id': 1, 'evidence_scope': {'trajectory': True, 'output': True}})
    seen = []

    def ai_scorer(evidence, dims):
        seen.append(evidence)
        return [{'dimension_id': 1, 'native_score': 1, 'rationale': 'r', 'status': 'scored'}]

    rows = orch.assemble_case_results(_case(), snapshot, ai_scorer=ai_scorer)
    assert isinstance(seen[0]['trajectory'], str)
    assert 'TOOL jira_search [ok]' in seen[0]['trajectory']
    assert rows[0]['status'] == 'ok'
    assert rows[0]['evidence']['trajectory']['ref'] == 'eval_case_execution'


def test_unscoped_bindings_still_score_when_nothing_recorded(orch):
    snapshot = _snapshot({'dimension_id': 1, 'engine': 'code', 'evidence_scope': {'output': True}})
    rows = orch.assemble_case_results(
        _case(recorded=None, usage=False), snapshot,
        code_scorer=lambda b, e: {'status': 'scored', 'native_score': 1.0, 'passed': True})
    assert rows[0]['status'] == 'ok'
    assert rows[0]['evidence'] == {'output': 'done', 'input': 'find EL bugs'}


def test_run_one_case_exposes_agent_usage_while_scoring(orch):
    snapshot = _snapshot({'dimension_id': 1, 'engine': 'code', 'evidence_scope': {'usage': True}})
    seen = []

    def runner(case):
        return {'status': 'ok', 'output': 'done', 'usage': AGENT_USAGE,
                'execution': {'trajectory_state': 'recorded', 'trajectory': TRAJECTORY,
                              'metrics': METRICS}}

    def code_scorer(binding, evidence):
        seen.append(evidence)
        return {'status': 'scored', 'native_score': 1.0, 'passed': True}

    case, rows = orch.run_one_case({'id': 7, 'input': 'i'}, snapshot, agent_runner=runner,
                                   code_scorer=code_scorer, case_index=5)
    assert seen[0]['usage']['total_tokens'] == 134
    assert case['_usage']['agent'] is AGENT_USAGE
    assert case['_execution']['case_index'] == 5
    assert rows[0]['status'] == 'ok'


# --- code prelude ----------------------------------------------------------------------------

def test_prelude_injects_trajectory_variables(cv):
    prelude = cv.build_validation_prelude(
        'result = True', output='o', trajectory={'tool_sequence': ['a']},
        expected_trajectory={'match': 'exact'}, usage={'total_tokens': 3})
    assert "trajectory = {'tool_sequence': ['a']}" in prelude
    assert "expected_trajectory = {'match': 'exact'}" in prelude
    assert "usage = {'total_tokens': 3}" in prelude


def test_prelude_unchanged_without_new_scopes(cv):
    prelude = cv.build_validation_prelude('result = True', output='o', input='i')
    assert 'trajectory' not in prelude and 'usage' not in prelude


def test_code_scorer_threads_trajectory_into_prelude(orch):
    snapshot = _snapshot({'dimension_id': 1, 'engine': 'code', 'evidence_scope': {'trajectory': True}})
    preludes = []

    def executor(prelude):
        preludes.append(prelude)
        return {'status': 'ok', 'result': True, 'stdout': '', 'stderr': '', 'execution_time': 0.1}

    scorer = orch._make_code_scorer(snapshot, executor)
    evidence = orch.select_evidence(_case(expected_trajectory={'match': 'subset'}), {'trajectory': True})
    scorer(snapshot['bindings'][0], evidence)
    assert "'tool_sequence': ['jira_search', 'post_comment']" in preludes[0]
    assert "expected_trajectory = {'match': 'subset'}" in preludes[0]
    assert 'usage = ' not in preludes[0]


# --- judge -----------------------------------------------------------------------------------

def test_render_trajectory_is_compact(judge):
    text = judge.render_trajectory({**TRAJECTORY, 'metrics': METRICS})
    assert text.splitlines()[0] == '1. LLM gpt-4o (in 120 / out 14 tokens) → plans: jira_search'
    assert '2. TOOL jira_search [ok]' in text
    assert '3. [in Writer] TOOL post_comment [error]' in text
    assert 'error: forbidden' in text
    assert 'chars omitted' in text and len(text) < 2000
    assert 'Counters: llm_calls=1' in text


def test_render_trajectory_notes_truncation_and_pause(judge):
    text = judge.render_trajectory({'steps': [], 'truncated': True,
                                    'pause': {'pause_type': 'hitl', 'tool_name': 'deploy'}})
    assert '(no steps recorded)' in text
    assert 'truncated when recorded' in text
    assert 'Run paused: hitl on deploy' in text


def test_judge_prompt_describes_new_fields_only_when_present(judge):
    dims = [{'id': 1, 'name': 'n', 'definition': 'd', 'scale_type': 'binary'}]
    plain = judge.build_judge_system_prompt(dims)
    assert plain == judge.build_judge_system_prompt(dims, ('output', 'input'))
    assert '`trajectory`' not in plain
    with_traj = judge.build_judge_system_prompt(dims, ('trajectory', 'usage'))
    assert '`trajectory`' in with_traj and '`usage`' in with_traj
    assert '`expected_trajectory`' not in with_traj


def test_case_payload_carries_new_fields(judge):
    dims = [{'id': 1}]
    payload = json.loads(judge.build_case_payload(
        {'trajectory': '1. TOOL a [ok]', 'expected_trajectory': {'match': 'exact'},
         'usage': {'total_tokens': 3}}, dims))
    assert payload == {'trajectory': '1. TOOL a [ok]', 'expected_trajectory': {'match': 'exact'},
                       'usage': {'total_tokens': 3}, 'dimension_ids': [1]}


def test_budget_truncation_trims_trajectory_first(judge, orch):
    dims = [{'id': 1, 'name': 'n', 'definition': 'd', 'scale_type': 'binary'}]
    evidence = {'trajectory': 'step\n' * 4000, 'output': 'short'}
    shrunk, _ = judge._truncate_evidence_for_budget(evidence, dims, budget_tokens=1500)
    assert len(shrunk['trajectory']) < len(evidence['trajectory'])
    assert shrunk['output'] == 'short'
