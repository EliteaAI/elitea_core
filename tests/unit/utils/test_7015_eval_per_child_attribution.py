"""#7015 P3: per-child attribution of a case whose agent calls sub-agents.

* ``agent_breakdown`` splits a case's steps by the agent that made them, into ``metrics.agents``.
* ``ledger_agents`` / ``settle_agents`` replace the split's tokens with the ledger's per-entity
  figures when the sub-agents were dispatched (their calls carry their own application).
* ``trajectory.tool_match`` scores the agent's own calls only, so a reference that names the
  sub-agents is not marked down for what they did inside. ``forbidden_tools`` still sees every call.

The figures are those of local run #219 (dispatched) and #217 (in-process).

Run via:
    python tests/run_tests.py unit/utils/test_7015_eval_per_child_attribution.py -v
"""

import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

checks = sys.modules['plugins.elitea_core.utils.evaluation_trajectory_checks']

PRID = '8b98ac42-c7fb-4fe1-b86b-50b76798c3ee'
ORDER, DECISION = 'RD-OrderReader', 'RD-DecisionReader'


@pytest.fixture(scope='module')
def usage(utils_path):
    return load_utils_module(utils_path, 'evaluation_usage')


@pytest.fixture(scope='module')
def execution(utils_path, usage):
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def cv(utils_path):
    load_utils_module(utils_path, 'evaluation_code_screen')
    return load_utils_module(utils_path, 'code_validation')


def _llm(tokens=(10, 1), agent=None):
    return {'kind': 'llm', 'model': 'haiku', 'parent_agent': agent,
            'tokens': None if tokens is None else {'in': tokens[0], 'out': tokens[1]}}


def _tool(name, agent=None, status='ok'):
    return {'kind': 'tool', 'tool_name': name, 'tool_inputs': {}, 'status': status,
            'is_error': status == 'error', 'parent_agent': agent}


def _fanout():
    """Case 0 of run #217: the parent fans out, each child reads a file, the parent answers."""
    return {'steps': [
        _llm((1700, 120)),
        _llm((3600, 35), DECISION), _tool('read_file', DECISION), _llm((3649, 35), DECISION),
        _llm((3900, 60), ORDER), _tool('list_files', ORDER), _llm((4000, 70), ORDER),
        _tool('read_file', ORDER), _llm((4074, 78), ORDER),
        _tool(DECISION), _tool(ORDER),
        _llm((1697, 60)),
    ]}


# --- agent_breakdown --------------------------------------------------------------------------

def test_steps_split_by_agent_parent_first(execution):
    agents = execution.agent_breakdown(_fanout())
    assert [a['agent'] for a in agents] == [None, DECISION, ORDER]
    parent, decision, order = agents
    assert (parent['llm_calls'], parent['tool_calls'], parent['input_tokens'], parent['output_tokens']) == \
        (2, 2, 3397, 180)
    assert (decision['llm_calls'], decision['tool_calls'], decision['input_tokens']) == (2, 1, 7249)
    assert (order['llm_calls'], order['tool_calls'], order['input_tokens'], order['output_tokens']) == \
        (3, 2, 11974, 208)
    assert {a['source'] for a in agents} == {'trajectory'} and {a['cost'] for a in agents} == {None}


def test_the_split_adds_up_to_the_case_counters(execution):
    trajectory = _fanout()
    metrics = execution.trajectory_metrics(trajectory)
    agents = metrics['agents']
    assert sum(a['llm_calls'] for a in agents) == metrics['llm_calls']
    assert sum(a['tool_calls'] for a in agents) == metrics['tool_calls']


def test_a_plain_agent_gets_no_split(execution):
    trajectory = {'steps': [_llm(), _tool('read_file'), _llm()]}
    assert execution.agent_breakdown(trajectory) is None
    assert 'agents' not in execution.trajectory_metrics(trajectory)
    assert execution.agent_breakdown(None) is None


def test_an_agent_with_an_unreported_llm_call_has_unknown_tokens(execution):
    trajectory = {'steps': [_llm(), _llm((5, 1), ORDER), _llm(None, ORDER)]}
    _, order = execution.agent_breakdown(trajectory)
    assert (order['llm_calls'], order['input_tokens'], order['output_tokens']) == (2, None, None)


def test_a_sub_agent_that_only_called_tools_is_listed(execution):
    trajectory = {'steps': [_llm(), _tool('read_file', ORDER)]}
    _, order = execution.agent_breakdown(trajectory)
    assert (order['agent'], order['llm_calls'], order['tool_calls'], order['input_tokens']) == (ORDER, 0, 1, 0)


# --- ledger_agents / settle_agents ------------------------------------------------------------

def _row(case, entity, inp, out, nano, calls, role='agent'):
    base = 'eval_agent' if role == 'agent' else 'eval_judge'
    return {'conversation_id': f'{base}_{PRID}_{case}_0925568232e0', 'llm_calls': calls,
            'input_tokens': inp, 'output_tokens': out, 'cache_read_tokens': 0,
            'cache_creation_tokens': 0, 'reasoning_tokens': 0, 'cost_nano_usd': nano,
            'unpriced_calls': 0, 'entity_type': entity[0], 'entity_id': entity[1], 'entity_name': entity[2]}


EVAL = ('evaluation', 219, None)
ORDER_APP = ('application', 53, ORDER)
DECISION_APP = ('application', 54, DECISION)

# Run #219, case 0, as the usage ledger holds it.
LEDGER_219 = [
    _row(0, ORDER_APP, 11299, 191, 12_254_000, 3),
    _row(0, DECISION_APP, 7249, 70, 7_599_000, 2),
    _row(0, EVAL, 3358, 179, 4_253_000, 2),
    _row(0, EVAL, 900, 40, 1_000_000, 2, role='judge'),
]


def test_ledger_rows_key_by_the_sub_agent_application(usage):
    ledger = usage.ledger_agents(LEDGER_219, PRID)
    assert set(ledger) == {0}
    assert set(ledger[0]) == {None, ORDER, DECISION}
    assert (ledger[0][ORDER]['input_tokens'], ledger[0][ORDER]['llm_calls']) == (11299, 3)
    assert ledger[0][None]['input_tokens'] == 3358  # the judge's rows are not the agent's


def test_an_in_process_run_has_no_ledger_split(usage):
    # Run #217: the sub-agents ran inside the parent, so every row carries the evaluation.
    assert usage.ledger_agents([_row(0, EVAL, 21970, 458, 24_000_000, 7)], PRID) == {}


def test_settled_agents_take_the_ledger_tokens_and_cost(usage, execution):
    agents = execution.agent_breakdown(_fanout())
    settled = usage.settle_agents(agents, usage.ledger_agents(LEDGER_219, PRID)[0])
    by_name = {a['agent']: a for a in settled}
    assert (by_name[ORDER]['input_tokens'], by_name[ORDER]['output_tokens'], by_name[ORDER]['cost']) == \
        (11299, 191, 0.012254)
    assert (by_name[None]['cost'], by_name[DECISION]['cost']) == (0.004253, 0.007599)
    assert {a['source'] for a in settled} == {'ledger'}
    assert (by_name[ORDER]['llm_calls'], by_name[ORDER]['ledger_calls']) == (3, 3)


def test_an_unpriced_agent_keeps_its_cost_unknown(usage):
    rows = [{**_row(0, ORDER_APP, 10, 1, 0, 1), 'unpriced_calls': 1}, _row(0, EVAL, 10, 1, 5, 1)]
    agents = [{'agent': None, 'source': 'trajectory'}, {'agent': ORDER, 'source': 'trajectory'}]
    settled = usage.settle_agents(agents, usage.ledger_agents(rows, PRID)[0])
    assert (settled[1]['input_tokens'], settled[1]['cost']) == (10, None)


def test_a_ledger_agent_missing_from_the_trajectory_is_appended(usage):
    agents = [{'agent': None, 'llm_calls': 2, 'tool_calls': 2, 'source': 'trajectory'}]
    settled = usage.settle_agents(agents, usage.ledger_agents(LEDGER_219, PRID)[0])
    assert [a['agent'] for a in settled] == [None, ORDER, DECISION]
    assert (settled[1]['llm_calls'], settled[1]['input_tokens']) == (None, 11299)


def test_settle_agents_without_a_ledger_is_unchanged(usage):
    agents = [{'agent': None, 'source': 'trajectory'}]
    assert usage.settle_agents(agents, None) is agents
    assert usage.settle_agents(None, {None: {}}) is None


# --- tool_match scores the agent's own calls --------------------------------------------------

def _local_executor(prelude: str):
    ns = {}
    exec('\n'.join(prelude.splitlines()[:-1]), ns)  # noqa: S102 - trusted, shipped test scripts
    return ns.get('result')


def run(cv, key, trajectory, expected):
    return _local_executor(cv.build_validation_prelude(
        checks.script_for(key), output='o', trajectory=trajectory, expected_trajectory=expected))


def _ref(*tools, match='any_order', **extra):
    return {'match': match, 'tools': [{'name': t} for t in tools], 'forbidden': [], 'allow_repeat': [],
            **extra}


def test_tool_match_ignores_the_sub_agents_internal_calls(cv):
    # Before P3 this scored 0.5714 (57): 2 matched of 2 expected + 5 actual.
    assert run(cv, 'trajectory.tool_match', _fanout(), _ref(ORDER, DECISION)) == 1.0


@pytest.mark.parametrize('match', ['exact', 'in_order', 'subset', 'superset'])
def test_every_match_mode_sees_only_the_agents_calls(cv, match):
    assert run(cv, 'trajectory.tool_match', _fanout(), _ref(DECISION, ORDER, match=match)) == 1.0


def test_a_missing_sub_agent_call_is_still_marked_down(cv):
    trajectory = {'steps': [_llm(), _llm(agent=ORDER), _tool('read_file', ORDER), _tool(ORDER), _llm()]}
    assert run(cv, 'trajectory.tool_match', trajectory, _ref(ORDER, DECISION, match='superset')) == 0.5


def test_a_reference_naming_a_childs_tool_does_not_match_it(cv):
    # The reference describes the agent under test; read_file was the child's call, not its own.
    assert run(cv, 'trajectory.tool_match', _fanout(), _ref('read_file', match='superset')) == 0.0


def test_forbidden_tools_still_sees_every_agent(cv):
    expected = {**_ref(ORDER), 'forbidden': ['list_files']}
    assert run(cv, 'trajectory.forbidden_tools', _fanout(), expected) is False


def test_tool_match_on_a_plain_agent_is_unchanged(cv):
    trajectory = {'steps': [_llm(), _tool('jira_search'), _tool('jira_get'), _llm()]}
    assert run(cv, 'trajectory.tool_match', trajectory, _ref('jira_search', 'jira_get', match='exact')) == 1.0


def test_the_description_says_so():
    assert "sub-agent's internal tool calls are not matched" in \
        checks.CHECKS['trajectory.tool_match']['description']
