"""One answer on a scheduled run with several pending tools becomes one decision for each tool.

Contract under test: rpc/chat_all.py `_schedule_decisions`, `_schedule_batch_kwargs`, `_pause_changed` and the
409 of `_check_schedule_decision`. Nothing loads rpc/chat_all.py in the unit env, so the helpers are compiled from
the file's source (fixtures/cut_source.py) next to the real `interrupt_identity` and `pending_interrupts`.
`continue_predict_sio` itself is too large to load here; the helpers carry its logic.
"""
import ast
import pathlib
import types
from contextlib import contextmanager

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
IDS = ['hitl_a', 'hitl_b', 'hitl_c']


PAIR_LIMIT = 50
SENSITIVE = 'sensitive_tool'
PENDING = [{**item, 'guardrail_type': SENSITIVE, 'available_actions': ['approve', 'reject', 'block_with_comment']}
           for item in PENDING]


def _nodes(source, names):
    found = []
    for node in ast.parse(source).body:
        targets = [target.id for target in getattr(node, 'targets', []) if isinstance(target, ast.Name)]
        if getattr(node, 'name', None) in names or set(targets) & names:
            found.append(node)
    return found


class _Session:
    """A session whose queries answer the given values in order; each `where` takes the next one."""

    def __init__(self, answers):
        self.answers = answers

    def query(self, *_columns):
        return self

    def where(self, *_conditions):
        return self

    def scalar(self):
        return self.answers.pop(0)


@pytest.fixture(scope='module')
def helpers():
    parallel = load_cut(_nodes(PARALLEL_HITL, {'interrupt_identity', 'pending_interrupts'}), {}, 'parallel_hitl.py')
    application = types.SimpleNamespace(application='application')
    answers = []

    @contextmanager
    def get_session(_project_id):
        yield _Session(answers)

    refusals = types.SimpleNamespace(
        forbidden=type('ScheduleDecisionForbidden', (Exception,), {}),
        invalid=type('ScheduleDecisionInvalid', (Exception,), {}),
        not_waiting=type('ScheduleRunNotWaiting', (Exception,), {}))
    def check_schedule_decision(project_id, run_id, caller_id, actions, fan_out=False, sign_in=False):
        return 'reject' if 'reject' in actions else 'approve'

    namespace = {
        'interrupt_identity': parallel['interrupt_identity'],
        'pending_interrupts': parallel['pending_interrupts'],
        'ParticipantTypes': application,
        'CHAT_PREDICT_MAPPER': {'application': APPLICATION_RPC, 'toolkit': 'applications_test_toolkit_tool_sio',
                                'llm': 'applications_predict_sio_llm'},
        'PAIR_LIMIT': PAIR_LIMIT,
        'db': types.SimpleNamespace(get_session=get_session),
        'Conversation': types.SimpleNamespace(meta='meta', uuid='uuid'),
        'ConversationMessageGroup': types.SimpleNamespace(meta='meta', uuid='uuid'),
        'ScheduleDecisionForbidden': refusals.forbidden,
        'ScheduleDecisionInvalid': refusals.invalid,
        'ScheduleRunNotWaiting': refusals.not_waiting,
        'check_schedule_decision': check_schedule_decision,
    }
    cut = load_cut(_nodes(CHAT_ALL, {
        '_schedule_decisions', '_schedule_batch_kwargs', '_pause_changed', '_check_schedule_decision',
        '_SCHEDULE_REFUSALS', '_SIGN_IN_REFUSAL', '_PAUSE_CHANGED_REFUSAL', 'SCHEDULE_VALUE_LIMIT'}),
        namespace, 'chat_all.py')
    cut['answers'] = answers
    return cut


def _parsed(hitl_action='approve', hitl_value=None, hitl_decisions=None, interrupt_ids=None, hitl_resume=True):
    return types.SimpleNamespace(
        hitl_action=hitl_action, hitl_value=hitl_value, hitl_decisions=hitl_decisions, interrupt_ids=interrupt_ids,
        hitl_resume=hitl_resume, project_id=1, conversation_uuid='c-1', message_id='m-1', thread_id=None)

@pytest.mark.parametrize('action', ['approve', 'reject'])
def test_one_action_on_a_schedule_run_becomes_a_decision_for_every_pending_tool(helpers, action):
    decisions = helpers['_schedule_decisions'](_parsed(hitl_action=action, interrupt_ids=IDS), PENDING, SCHEDULE_DECISION)

    assert decisions == [{'interrupt_id': name, 'action': action, 'value': ''}
                         for name in ('hitl_a', 'hitl_b', 'hitl_c')]


def test_each_decision_has_exactly_the_keys_of_the_resume(helpers):
    decisions = helpers['_schedule_decisions'](_parsed(interrupt_ids=IDS), PENDING, SCHEDULE_DECISION)

    assert all(set(decision) == {'interrupt_id', 'action', 'value'} for decision in decisions)


def test_the_value_of_the_action_goes_to_every_decision(helpers):
    decisions = helpers['_schedule_decisions'](_parsed(hitl_value='checked', interrupt_ids=IDS), PENDING, SCHEDULE_DECISION)

    assert [decision['value'] for decision in decisions] == ['checked'] * 3


def test_a_plain_chat_has_no_schedule_decision_and_keeps_its_rules(helpers):
    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=IDS), PENDING, None) == []


def test_the_own_decision_list_of_the_caller_is_kept(helpers):
    own = [{'interrupt_id': 'hitl_a', 'action': 'approve', 'value': ''}]

    assert helpers['_schedule_decisions'](_parsed(hitl_decisions=own, interrupt_ids=IDS), PENDING, SCHEDULE_DECISION) == []


@pytest.mark.parametrize('pending', [[], PENDING[:1]], ids=['nothing pending', 'one tool pending'])
def test_a_pause_with_less_than_two_tools_is_not_changed(helpers, pending):
    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=IDS[:len(pending)]), pending, SCHEDULE_DECISION) == []


def test_a_pending_tool_without_an_interrupt_id_gets_its_thread_identity(helpers):
    pending = [{'guardrail_type': SENSITIVE, 'thread_id': 't1', 'tool_call_id': 'c1'},
               {'guardrail_type': SENSITIVE, 'interrupt_id': 'hitl_b'}]

    decisions = helpers['_schedule_decisions'](_parsed(interrupt_ids=['t1:c1', 'hitl_b']), pending, SCHEDULE_DECISION)

    assert [decision['interrupt_id'] for decision in decisions] == ['t1:c1', 'hitl_b']


def test_the_batch_flag_goes_to_the_application_predict_of_a_schedule_run_only(helpers):
    batch_kwargs = helpers['_schedule_batch_kwargs']

    assert batch_kwargs(SCHEDULE_DECISION, APPLICATION_RPC) == {'batch_sensitive_approvals': True}
    assert batch_kwargs(None, APPLICATION_RPC) == {}
    assert batch_kwargs(SCHEDULE_DECISION, 'applications_test_toolkit_tool_sio') == {}
    assert batch_kwargs(SCHEDULE_DECISION, 'applications_predict_sio_llm') == {}


def test_a_pause_that_holds_another_kind_of_item_is_not_answered_by_one_click(helpers):
    pending = [*PENDING, {'interrupt_id': 'hitl_q', 'guardrail_type': 'clarifying_question'}]

    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=[*IDS, 'hitl_q']), pending, SCHEDULE_DECISION) == []


@pytest.mark.parametrize('action', ['edit', 'answer'])
def test_an_action_that_a_pending_tool_does_not_offer_gets_no_decisions(helpers, action):
    assert helpers['_schedule_decisions'](_parsed(hitl_action=action, hitl_value='x', interrupt_ids=IDS), PENDING, SCHEDULE_DECISION) == []


def test_one_tool_that_does_not_offer_the_action_stops_the_whole_batch(helpers):
    pending = [PENDING[0], {**PENDING[1], 'available_actions': ['approve']}, PENDING[2]]

    assert helpers['_schedule_decisions'](_parsed(hitl_action='reject', interrupt_ids=IDS), pending, SCHEDULE_DECISION) == []


def test_a_tool_without_a_list_of_actions_accepts_the_action_as_the_single_answer_rule_does(helpers):
    pending = [{key: value for key, value in item.items() if key != 'available_actions'} for item in PENDING]

    decisions = helpers['_schedule_decisions'](_parsed(interrupt_ids=IDS), pending, SCHEDULE_DECISION)

    assert len(decisions) == 3


def test_an_answer_object_is_not_copied_to_the_tools(helpers):
    assert helpers['_schedule_decisions'](_parsed(hitl_value={'a': 'b'}, interrupt_ids=IDS), PENDING, SCHEDULE_DECISION) == []


def test_a_long_note_is_cut_before_it_is_copied_to_every_tool(helpers):
    limit = helpers['SCHEDULE_VALUE_LIMIT']

    decisions = helpers['_schedule_decisions'](
        _parsed(hitl_action='block_with_comment', hitl_value='n' * (limit + 500), interrupt_ids=IDS), PENDING, SCHEDULE_DECISION)

    assert [len(decision['value']) for decision in decisions] == [limit] * 3


def test_an_answer_without_ids_gets_no_decisions_so_the_single_answer_rule_applies(helpers):
    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=None), PENDING, SCHEDULE_DECISION) == []


@pytest.mark.parametrize('shown', [IDS[:2], [*IDS, 'hitl_x'], ['hitl_a', 'hitl_b', 'hitl_x'], []],
                         ids=['fewer', 'more', 'other', 'none'])
def test_ids_that_differ_from_the_pending_tools_get_no_decisions(helpers, shown):
    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=shown), PENDING, SCHEDULE_DECISION) == []


def test_ids_in_another_order_still_cover_every_pending_tool(helpers):
    decisions = helpers['_schedule_decisions'](_parsed(interrupt_ids=IDS[::-1]), PENDING, SCHEDULE_DECISION)

    assert [decision['interrupt_id'] for decision in decisions] == IDS


def test_a_pause_longer_than_the_id_list_limit_gets_no_decisions(helpers):
    names = [f'hitl_{number}' for number in range(PAIR_LIMIT + 3)]
    pending = [{'interrupt_id': name, 'guardrail_type': SENSITIVE} for name in names]

    assert helpers['_schedule_decisions'](_parsed(interrupt_ids=names[:PAIR_LIMIT]), pending, SCHEDULE_DECISION) == []


def _answer(helpers, message_meta, interrupt_ids, hitl_action='approve'):
    """Run the owner check of a schedule chat whose paused message holds `message_meta`."""
    helpers['answers'][:] = [{'schedule_run_id': 9}, message_meta]
    return helpers['_check_schedule_decision'](_parsed(hitl_action=hitl_action, interrupt_ids=interrupt_ids), 20)


def _meta(*names):
    return {'hitl_interrupts': [{'interrupt_id': name, 'guardrail_type': SENSITIVE} for name in names]}


def test_an_answer_for_the_pause_the_banner_showed_is_accepted(helpers):
    decision, denied = _answer(helpers, _meta('hitl_a', 'hitl_b'), ['hitl_b', 'hitl_a'])

    assert (decision, denied) == ((9, 'approve'), None)


def test_an_answer_from_an_old_banner_is_refused_with_409_and_changes_nothing(helpers):
    decision, denied = _answer(helpers, _meta('hitl_c', 'hitl_d'), ['hitl_a', 'hitl_b'])

    assert (decision, denied['status']) == (None, 409)
    assert 'changed' in denied['error']


@pytest.mark.parametrize('action', ['approve', 'reject'])
def test_a_reject_from_an_old_banner_is_refused_too(helpers, action):
    decision, denied = _answer(helpers, _meta('hitl_c'), ['hitl_a'], hitl_action=action)

    assert (decision, denied['status']) == (None, 409)


@pytest.mark.parametrize('shown', [['hitl_a'], ['hitl_a', 'hitl_b', 'hitl_x'], []], ids=['fewer', 'more', 'none'])
def test_a_banner_list_that_differs_from_the_pending_set_is_refused(helpers, shown):
    decision, denied = _answer(helpers, _meta('hitl_a', 'hitl_b'), shown)

    assert (decision, denied['status']) == (None, 409)


def test_an_answer_without_ids_keeps_the_old_behavior(helpers):
    helpers['answers'][:] = [{'schedule_run_id': 9}]

    decision, denied = helpers['_check_schedule_decision'](_parsed(interrupt_ids=None), 20)

    assert (decision, denied) == ((9, 'approve'), None)
    assert helpers['answers'] == []  # the paused message was not read


def test_only_the_sensitive_tools_count_as_the_pending_set(helpers):
    meta = {'hitl_interrupts': [*_meta('hitl_a')['hitl_interrupts'], {'interrupt_id': 'q1', 'guardrail_type': 'x'}]}

    decision, denied = _answer(helpers, meta, ['hitl_a'])

    assert (decision, denied) == ((9, 'approve'), None)


def test_a_banner_that_showed_the_limit_matches_a_longer_pending_set(helpers):
    names = [f'hitl_{number}' for number in range(PAIR_LIMIT + 3)]

    decision, denied = _answer(helpers, _meta(*names), names[:PAIR_LIMIT])

    assert (decision, denied) == ((9, 'approve'), None)


def test_a_chat_that_is_not_a_schedule_run_has_no_pause_check(helpers):
    helpers['answers'][:] = [{}]

    assert helpers['_check_schedule_decision'](_parsed(interrupt_ids=['hitl_a']), 20) == (None, None)
