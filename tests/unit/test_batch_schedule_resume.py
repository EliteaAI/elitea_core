"""One answer on a scheduled run with several pending tools becomes one decision for each tool.

Contract under test: rpc/chat_all.py `_schedule_decisions` and `_schedule_batch_kwargs`, and the places where
`continue_predict_sio` uses them. Nothing loads rpc/chat_all.py in the unit env, so the helpers are compiled from
the file's source (fixtures/cut_source.py) next to the real `interrupt_identity`.
"""
import ast
import pathlib
import types

import pytest
from fixtures.cut_source import load_cut

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = (PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text()
PARALLEL_HITL = (PLUGIN_ROOT / 'utils' / 'parallel_hitl.py').read_text()
APPLICATION_RPC = 'applications_predict_sio'
PENDING = [
    {'interrupt_id': 'hitl_a', 'tool_call_id': 'c1', 'toolkit_name': 'Artifact', 'tool_name': 'create_file'},
    {'interrupt_id': 'hitl_b', 'tool_call_id': 'c2', 'toolkit_name': 'Artifact', 'tool_name': 'delete_file'},
    {'interrupt_id': 'hitl_c', 'tool_call_id': 'c3', 'toolkit_name': 'ADO', 'tool_name': 'list_indexes'},
]
SCHEDULE_DECISION = (9, 'approve')


def _nodes(source, names):
    return [node for node in ast.parse(source).body if getattr(node, 'name', None) in names]


@pytest.fixture(scope='module')
def helpers():
    identity = load_cut(_nodes(PARALLEL_HITL, {'interrupt_identity'}), {}, 'parallel_hitl.py')
    application = types.SimpleNamespace(application='application')
    namespace = {
        'interrupt_identity': identity['interrupt_identity'],
        'ParticipantTypes': application,
        'CHAT_PREDICT_MAPPER': {'application': APPLICATION_RPC, 'toolkit': 'applications_test_toolkit_tool_sio',
                                'llm': 'applications_predict_sio_llm'},
    }
    return load_cut(_nodes(CHAT_ALL, {'_schedule_decisions', '_schedule_batch_kwargs'}), namespace, 'chat_all.py')


def _parsed(hitl_action='approve', hitl_value=None, hitl_decisions=None):
    return types.SimpleNamespace(hitl_action=hitl_action, hitl_value=hitl_value, hitl_decisions=hitl_decisions)


@pytest.mark.parametrize('action', ['approve', 'reject'])
def test_one_action_on_a_schedule_run_becomes_a_decision_for_every_pending_tool(helpers, action):
    decisions = helpers['_schedule_decisions'](_parsed(hitl_action=action), PENDING, SCHEDULE_DECISION)

    assert decisions == [{'interrupt_id': name, 'action': action, 'value': ''}
                         for name in ('hitl_a', 'hitl_b', 'hitl_c')]


def test_each_decision_has_exactly_the_keys_of_the_resume(helpers):
    decisions = helpers['_schedule_decisions'](_parsed(), PENDING, SCHEDULE_DECISION)

    assert all(set(decision) == {'interrupt_id', 'action', 'value'} for decision in decisions)


def test_the_value_of_the_action_goes_to_every_decision(helpers):
    decisions = helpers['_schedule_decisions'](_parsed(hitl_value='checked'), PENDING, SCHEDULE_DECISION)

    assert [decision['value'] for decision in decisions] == ['checked'] * 3


def test_a_plain_chat_has_no_schedule_decision_and_keeps_its_rules(helpers):
    assert helpers['_schedule_decisions'](_parsed(), PENDING, None) == []


def test_the_own_decision_list_of_the_caller_is_kept(helpers):
    own = [{'interrupt_id': 'hitl_a', 'action': 'approve', 'value': ''}]

    assert helpers['_schedule_decisions'](_parsed(hitl_decisions=own), PENDING, SCHEDULE_DECISION) == []


@pytest.mark.parametrize('pending', [[], PENDING[:1]], ids=['nothing pending', 'one tool pending'])
def test_a_pause_with_less_than_two_tools_is_not_changed(helpers, pending):
    assert helpers['_schedule_decisions'](_parsed(), pending, SCHEDULE_DECISION) == []


def test_a_pending_tool_without_an_interrupt_id_gets_its_thread_identity(helpers):
    pending = [{'thread_id': 't1', 'tool_call_id': 'c1'}, {'interrupt_id': 'hitl_b'}]

    decisions = helpers['_schedule_decisions'](_parsed(), pending, SCHEDULE_DECISION)

    assert [decision['interrupt_id'] for decision in decisions] == ['t1:c1', 'hitl_b']


def test_the_batch_flag_goes_to_the_application_predict_of_a_schedule_run_only(helpers):
    batch_kwargs = helpers['_schedule_batch_kwargs']

    assert batch_kwargs(SCHEDULE_DECISION, APPLICATION_RPC) == {'batch_sensitive_approvals': True}
    assert batch_kwargs(None, APPLICATION_RPC) == {}
    assert batch_kwargs(SCHEDULE_DECISION, 'applications_test_toolkit_tool_sio') == {}
    assert batch_kwargs(SCHEDULE_DECISION, 'applications_predict_sio_llm') == {}


def _continue_predict_sio():
    return next(node for node in ast.walk(ast.parse(CHAT_ALL))
                if isinstance(node, ast.FunctionDef) and node.name == 'continue_predict_sio')


def _calls(tree, name):
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == name]


def test_the_resume_makes_the_decisions_before_it_checks_them_against_the_pending_tools():
    resume = _continue_predict_sio()

    (batch,) = _calls(resume, '_schedule_decisions')
    (partition,) = _calls(resume, 'partition_root_hitl_decisions')

    assert [arg.id for arg in batch.args] == ['parsed', 'pending', 'schedule_decision']
    assert batch.lineno < partition.lineno


def test_the_resume_passes_the_batch_flag_to_the_predict_rpc_call():
    resume = _continue_predict_sio()

    (batch,) = _calls(resume, '_schedule_batch_kwargs')

    assert [arg.id for arg in batch.args] == ['schedule_decision', 'rpc_func']
