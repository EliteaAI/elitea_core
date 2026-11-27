"""EL-7011 US-18: the owner check runs before a schedule run resumes, and REST answers 400, 403 or 409.

Contract under test: rpc/chat_all.py `_check_schedule_decision`, `_apply_schedule_decision` and their
places in `continue_predict_sio`, and api/v2/continue_predict.py `PromptLibAPI.post`.
Nothing loads rpc/chat_all.py in the unit env, so the helpers are compiled from the file's source
with fakes around them. Cases: TC-US18-05, TC-US18-08, TC-US18-09.

Run via:
    python tests/run_tests.py unit/test_7011_continue_predict_owner.py -v
"""
import ast
import builtins
import pathlib
import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from fixtures.cut_source import load_cut
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = (PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text()
PROJECT_ID, RUN_ID, OWNER, CONVERSATION_ID = 7, 9, 12, 31
ACTOR = 55
REFUSED_TEXT = 'Only the owner of this schedule can approve or reject this run'
HELPERS = {
    '_SCHEDULE_REFUSALS', '_SIGN_IN_REFUSAL', '_check_schedule_decision', '_apply_schedule_decision', '_resume_user_id',
    '_approve_all_kwargs',
}


class Forbidden(Exception):
    pass


class Invalid(Exception):
    pass


class NotWaiting(Exception):
    pass


class NotActor(Exception):
    pass


class SettingOff(Exception):
    pass


class WebhookRun(Exception):
    pass


def _helpers(meta, error=None, decision='approve', apply_error=None, actor=ACTOR, resume=False):
    """The real helpers on a fake conversation meta; `calls` records each schedule_runs call."""
    calls = []

    def check_schedule_decision(project_id, run_id, caller_id, actions, fan_out=False, sign_in=False,
                                conversation_id=None, approve_all=False):
        calls.append(('check', project_id, run_id, caller_id, actions, {
            'fan_out': fan_out, 'sign_in': sign_in, 'conversation_id': conversation_id, 'approve_all': approve_all}))
        if error:
            raise error
        return decision

    def apply_schedule_decision(project_id, run_id, decision, approve_all=False):
        calls.append(('apply', project_id, run_id, decision, approve_all))
        if apply_error:
            raise apply_error

    def schedule_run_actor_id(project_id, run_id):
        calls.append(('actor', project_id, run_id))
        return actor

    chat = None if meta is None else types.SimpleNamespace(id=CONVERSATION_ID, meta=meta)
    query = types.SimpleNamespace(where=lambda *_: types.SimpleNamespace(first=lambda: chat))

    @contextmanager
    def get_session(project_id):
        yield types.SimpleNamespace(query=lambda *_: query)

    nodes = [
        node for node in ast.parse(CHAT_ALL).body
        if getattr(node, 'name', None) in HELPERS
        or any(getattr(target, 'id', None) in HELPERS for target in getattr(node, 'targets', []))
    ]
    namespace = {
        'db': types.SimpleNamespace(get_session=get_session),
        'Conversation': types.SimpleNamespace(id='id', meta='meta', uuid='uuid'),
        'check_schedule_decision': check_schedule_decision,
        'apply_schedule_decision': apply_schedule_decision,
        'schedule_run_actor_id': schedule_run_actor_id,
        'DECISION_REFUSED': REFUSED_TEXT,
        'ScheduleDecisionForbidden': Forbidden,
        'ScheduleDecisionInvalid': Invalid,
        'ScheduleRunNotWaiting': NotWaiting,
        'ScheduleApproveAllForbidden': NotActor,
        'ScheduleApproveAllWebhook': WebhookRun,
        'ScheduleApproveAllOff': SettingOff,
    }
    namespace = load_cut(nodes, namespace, 'chat_all.py')  # the real helpers cut out of rpc/chat_all.py
    helpers = namespace['_check_schedule_decision'], namespace['_apply_schedule_decision'], calls
    return helpers + (namespace['_resume_user_id'],) if resume else helpers


BARE_NAMES = {'_BARE_HITL_ACTIONS', '_BARE_SIGN_IN_ACTIONS', '_BARE_ITEM_KEYS', '_BARE_PAYLOAD_FIELDS', '_BARE_REFUSAL',
              '_bare_decision_refusal', '_drop_caller_inputs'}


def _bare():
    """The plain-answer guard cut out of rpc/chat_all.py: (the refusal check, the clearing of the caller's inputs)."""
    nodes = [
        node for node in ast.parse(CHAT_ALL).body
        if getattr(node, 'name', None) in BARE_NAMES
        or any(getattr(target, 'id', None) in BARE_NAMES for target in getattr(node, 'targets', []))
    ]
    namespace = load_cut(nodes, {}, 'chat_all.py')
    return namespace['_bare_decision_refusal'], namespace['_drop_caller_inputs']


def _parsed(**values):
    fields = {
        'project_id': PROJECT_ID, 'conversation_uuid': 'c-1', 'hitl_resume': True,
        'hitl_action': 'approve', 'hitl_decisions': None, 'thread_id': None,
        'mcp_auth_resume': False, 'mcp_auth_action': None, 'hitl_value': None, 'user_input': None,
        'mcp_auth_decisions': None, 'llm_settings': None, 'attachments_info': None, 'mcp_tokens': {},
        'ignored_mcp_servers': [], 'user_declined_mcp_servers': None,
        **values,
    }
    # Like a pydantic model, the payload knows which fields the client sent
    return types.SimpleNamespace(model_fields_set=set(fields), **fields)


def test_el_7011_r2_1_the_check_changes_nothing_and_the_apply_writes_the_decision():
    check, apply, calls = _helpers({'schedule_run_id': RUN_ID})

    pending, denied = check(_parsed(), OWNER)
    assert (pending, denied) == ((RUN_ID, 'approve', False), None)
    assert calls == [('check', PROJECT_ID, RUN_ID, OWNER, ['approve'], {
        'fan_out': False, 'sign_in': False, 'conversation_id': CONVERSATION_ID, 'approve_all': False})]

    assert apply(PROJECT_ID, pending) is None
    assert calls[-1] == ('apply', PROJECT_ID, RUN_ID, 'approve', False)


def test_el_7011_r2_2_a_decision_list_passes_every_action():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_action='approve', hitl_decisions=[{'action': 'approve'}, {'action': 'reject'}]), OWNER)

    assert calls[0][4] == ['approve', 'reject']


def test_el_7011_r2_4_a_fan_out_child_on_a_resumed_run_has_nothing_to_apply():
    check, apply, calls = _helpers({'schedule_run_id': RUN_ID}, decision=None)

    pending, denied = check(_parsed(thread_id='child-1'), OWNER)

    assert (pending, denied) == ((RUN_ID, None, False), None)  # passed the check, nothing to write
    assert calls[0][-1] == {
        'fan_out': True, 'sign_in': False, 'conversation_id': CONVERSATION_ID, 'approve_all': False}
    assert apply(PROJECT_ID, pending) is None
    assert [call[0] for call in calls] == ['check']


@pytest.mark.parametrize('error, status', [(Forbidden(), 403), (Invalid(), 400), (NotWaiting(), 409)])
def test_el_7011_r2_3_r2_4_a_refused_check_answers_its_status(error, status):
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, error)

    pending, denied = check(_parsed(), 1)

    assert (pending, denied['status']) == (None, status)


def test_a_refused_decision_by_a_user_who_is_not_the_schedule_owner_names_the_owner_in_the_text():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, Forbidden())

    pending, denied = check(_parsed(), 1)

    assert pending is None
    assert denied == {'error': REFUSED_TEXT, 'status': 403}


def test_a_decided_schedule_run_resumes_as_the_user_it_acts_as():
    _, _, calls, resume_user_id = _helpers({'schedule_run_id': RUN_ID}, resume=True)

    assert resume_user_id(PROJECT_ID, (RUN_ID, 'approve'), OWNER) == ACTOR
    assert calls == [('actor', PROJECT_ID, RUN_ID)]


def test_a_fan_out_child_of_a_resumed_run_also_resumes_as_the_run_actor():
    _, _, _, resume_user_id = _helpers({'schedule_run_id': RUN_ID}, resume=True)

    assert resume_user_id(PROJECT_ID, (RUN_ID, None), OWNER) == ACTOR


def test_a_chat_without_a_decided_run_resumes_as_its_caller():
    _, _, calls, resume_user_id = _helpers({'schedule_run_id': RUN_ID}, resume=True)

    assert resume_user_id(PROJECT_ID, None, OWNER) == OWNER
    assert calls == []


def test_a_run_without_an_actor_resumes_as_its_caller():
    _, _, _, resume_user_id = _helpers({'schedule_run_id': RUN_ID}, actor=None, resume=True)

    assert resume_user_id(PROJECT_ID, (RUN_ID, 'approve'), OWNER) == OWNER


def test_the_resume_runs_as_the_actor_and_the_child_card_accepts_the_decided_owner():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    body = body[:body.index('\n    def ', 1)]

    assert 'user_id=resume_user_id,' in body
    assert "user_id=current_user['id']," not in body
    assert 'decided=schedule_decision is not None' in body
    assert 'and not decided' in CHAT_ALL[CHAT_ALL.index('def _continue_child_resume('):]


@pytest.mark.parametrize('answer', ['authorize', 'skip'])
def test_a_sign_in_answer_of_another_user_is_refused(answer):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID}, Forbidden())

    pending, denied = check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action=answer), 1)

    assert (pending, denied['status']) == (None, 403)
    assert calls[0][3] == 1 and calls[0][-1]['sign_in'] is True


def test_a_chat_without_the_conversation_row_is_left_alone():
    check, _, calls = _helpers(None)

    assert check(_parsed(), OWNER) == (None, None)
    assert calls == []


def test_el_7011_r2_1_an_unknown_action_names_the_field():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, Invalid())

    assert check(_parsed(hitl_action='maybe'), OWNER)[1]['field'] == 'hitl_action'


def test_el_7011_r2_1_a_non_dict_decision_item_is_passed_as_no_action():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_decisions=['approve', {'action': 'approve'}]), OWNER)

    assert calls[0][4] == [None, 'approve']


def test_el_7011_r2_4_an_apply_that_lost_a_race_gets_409():
    _, apply, _ = _helpers({'schedule_run_id': RUN_ID}, apply_error=NotWaiting())

    assert apply(PROJECT_ID, (RUN_ID, 'approve'))['status'] == 409


@pytest.mark.parametrize('meta, parsed', [
    (None, _parsed()),
    ({'schedule_id': 3}, _parsed()),
    ({'schedule_run_id': RUN_ID}, _parsed(hitl_resume=False)),
])
def test_el_7011_r2_1_other_conversations_and_resumes_skip_the_check(meta, parsed):
    check, _, calls = _helpers(meta)

    assert check(parsed, OWNER) == (None, None)
    assert calls == []


def test_el_7011_r2_3_the_check_runs_before_any_resume_return():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    check_index = body.index('_check_schedule_decision(parsed, current_user')

    assert body.index('current_user = auth.current_user()') < check_index
    assert body.index('if _internal_token is not INTERNAL_CONTINUE_TOKEN:') < check_index  # a replay was checked
    assert check_index < body.index('self.is_chat_run_stopped(parsed.message_id)')
    assert check_index < body.index('self._continue_child_resume(')


def test_el_7011_r2_1_a_refused_later_guard_leaves_the_run_waiting():
    # The write follows the stopped-run, child-resume and expired-child guards, before the parent resume
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    body = body[:body.index('\n    def ', 1)]
    child_index = body.index('self._continue_child_resume(')
    child_apply, parent_apply = (
        index for index in range(len(body)) if body.startswith('_apply_schedule_decision(', index))

    assert body.index('self.is_chat_run_stopped(parsed.message_id)') < child_index
    assert child_index < child_apply < body.index('return child_resume')
    assert body.index('if expired_child:') < parent_apply < body.index('Conversation.uuid == parsed.conversation_uuid')


@pytest.fixture
def post(monkeypatch):
    """`PromptLibAPI.post` on stubbed plugin modules; set `rpc.result` to what the RPC returns."""
    def decorator(*_, **__):
        return lambda func: func

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=list, endpoint_metrics=lambda func: func)
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=decorator))
    tools.db = types.SimpleNamespace()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.register_openapi = decorator
    rpc = types.SimpleNamespace(result=None, approve_all=False, sent=[])
    payload = types.SimpleNamespace(model_validate=lambda raw: types.SimpleNamespace(
        conversation_uuid='c-1', message_id='m-1', mcp_auth_resume=False, hitl_resume=True,
        hitl_action='approve', hitl_value=None, hitl_decisions=None, mcp_auth_action=None,
        approve_all=rpc.approve_all,
        mcp_auth_decisions=None, authorization_request_id=None, user_input=None, thread_id=None,
        await_task_timeout=-1))
    stubs = {
        'flask': types.SimpleNamespace(request=types.SimpleNamespace(json={})),
        'tools': tools,
        'pylon.core.tools': types.SimpleNamespace(log=types.SimpleNamespace(error=print)),
        'plugins.elitea_core.models.conversation': types.SimpleNamespace(Conversation=None),
        'plugins.elitea_core.models.message_group': types.SimpleNamespace(ConversationMessageGroup=None),
        'plugins.elitea_core.models.pd.continue_predict': types.SimpleNamespace(ContinuePredictPayload=payload),
        'plugins.elitea_core.utils.conversation_utils': types.SimpleNamespace(
            _message_group_columns=None, fetch_guarded_message_groups=None, _serialize_guarded_groups=None),
        'plugins.elitea_core.utils.constants': types.SimpleNamespace(PROMPT_LIB_MODE='prompt_lib'),
        'plugins.elitea_core.utils.sio_utils': types.SimpleNamespace(SioValidationError=Forbidden),
        'plugins.elitea_core.utils.exceptions': types.SimpleNamespace(
            PoolSaturationError=NotWaiting, BudgetDoorClosedError=NotWaiting,
            InputGuardrailBlockedError=NotWaiting),
    }
    for name in ('pylon', 'pylon.core', 'plugins', 'plugins.elitea_core', 'plugins.elitea_core.api',
                 'plugins.elitea_core.api.v2'):
        package = types.ModuleType(name)
        package.__path__ = []
        stubs[name] = package
    stubs['plugins.elitea_core.api.v2.continue_predict'] = None  # monkeypatch drops the loaded module after
    for name, stub in stubs.items():
        monkeypatch.setitem(sys.modules, name, stub)
    module = load_module_with_stubs(
        PLUGIN_ROOT / 'api' / 'v2' / 'continue_predict.py', 'plugins.elitea_core.api.v2.continue_predict')
    handler = module.PromptLibAPI()
    handler.module = types.SimpleNamespace(
        chat_continue_predict_sio=lambda **kwargs: (rpc.sent.append(kwargs), rpc.result)[1])
    return rpc, lambda: handler.post(PROJECT_ID, 'c-1')


@pytest.mark.parametrize('status', [400, 403, 409])
def test_el_7011_r2_3_r2_4_rest_answers_the_status_of_the_decision(post, status):
    rpc, call = post
    rpc.result = {'error': 'denied', 'status': status}

    assert call() == ({'error': 'denied'}, status)


def test_el_7011_r2_4_rest_keeps_400_for_other_errors(post):
    rpc, call = post
    rpc.result = {'error': 'Redis connection error'}

    assert call() == ({'error': 'Redis connection error'}, 400)


def test_rest_carries_the_code_and_field_of_a_refusal(post):
    rpc, call = post
    rpc.result = {
        'error': 'This project does not allow "Run without asking", so all tools cannot be approved',
        'field': 'approve_all', 'code': 'approve_all_off', 'status': 409,
    }

    body, status = call()

    assert status == 409
    assert body == {
        'error': 'This project does not allow "Run without asking", so all tools cannot be approved',
        'code': 'approve_all_off', 'field': 'approve_all',
    }


def test_a_sign_in_answer_is_checked_as_a_schedule_decision():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    pending, denied = check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action='authorize'), OWNER)

    assert (pending, denied) == ((RUN_ID, 'approve', False), None)
    assert calls == [('check', PROJECT_ID, RUN_ID, OWNER, ['authorize'], {
        'fan_out': False, 'sign_in': True, 'conversation_id': CONVERSATION_ID, 'approve_all': False})]


def test_one_skip_among_the_sign_in_answers_rejects_the_run():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})
    answers = [{'interrupt_id': 'a', 'action': 'authorize'}, {'interrupt_id': 'b', 'action': 'skip'}]

    pending, denied = check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action='authorize',
                                    mcp_auth_decisions=answers), OWNER)

    assert denied is None
    assert calls[0][4] == ['authorize', 'skip']


def test_an_invalid_sign_in_answer_names_the_sign_in_field():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, error=Invalid())

    pending, denied = check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action='maybe'), OWNER)

    assert pending is None and denied['field'] == 'mcp_auth_action' and denied['status'] == 400


def test_a_sign_in_answer_in_a_chat_without_a_schedule_run_is_left_alone():
    check, _, calls = _helpers({})

    assert check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action='skip'), OWNER) == (None, None)
    assert calls == []


@pytest.mark.parametrize('approve_all', [True, False])
def test_rest_forwards_approve_all(post, approve_all):
    rpc, call = post
    rpc.result = {'stopped': True}
    rpc.approve_all = approve_all

    call()

    assert rpc.sent[0]['data']['approve_all'] is approve_all


@pytest.mark.parametrize('decision, approve_all, expected', [
    ('approve', True, True), ('approve', False, False), ('reject', True, False)])
def test_the_check_carries_approve_all_only_with_an_approve(decision, approve_all, expected):
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, decision=decision)

    pending, _ = check(_parsed(approve_all=approve_all), OWNER)

    assert pending == (RUN_ID, decision, expected)


@pytest.mark.parametrize('error, status, field', [
    (NotActor(), 403, 'approve_all'),
    (WebhookRun(), 403, 'approve_all'),
    (SettingOff(), 409, 'approve_all'),
])
def test_a_refused_approve_all_answers_its_status_and_names_the_field(error, status, field):
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, error)

    pending, denied = check(_parsed(approve_all=True), OWNER)

    assert pending is None
    assert (denied['status'], denied['field']) == (status, field)


def test_the_webhook_refusal_has_its_own_code():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, WebhookRun())

    _, denied = check(_parsed(approve_all=True), OWNER)

    assert denied['code'] == 'approve_all_webhook'


def test_the_approve_all_refusal_of_the_setting_has_a_code_that_tells_it_from_a_run_that_does_not_wait():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, SettingOff())

    _, denied = check(_parsed(approve_all=True), OWNER)

    assert denied == {
        'error': 'This project does not allow "Run without asking", so all tools cannot be approved',
        'field': 'approve_all', 'code': 'approve_all_off', 'status': 409,
    }
    _, not_waiting = _helpers({'schedule_run_id': RUN_ID}, NotWaiting())[0](_parsed(approve_all=True), OWNER)
    assert (not_waiting['status'], 'code' in not_waiting) == (409, False)


def test_the_approve_all_refusal_of_a_webhook_run_is_a_403_with_a_code_and_the_field():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, WebhookRun())

    pending, denied = check(_parsed(approve_all=True), OWNER)

    assert pending is None
    assert denied == {
        'error': 'A run started by a webhook cannot approve all its tools', 'field': 'approve_all',
        'code': 'approve_all_webhook', 'status': 403,
    }


def test_rest_carries_the_code_and_field_of_the_webhook_refusal(post):
    rpc, call = post
    rpc.result = {
        'error': 'A run started by a webhook cannot approve all its tools', 'field': 'approve_all',
        'code': 'approve_all_webhook', 'status': 403,
    }

    assert call() == (
        {'error': 'A run started by a webhook cannot approve all its tools', 'code': 'approve_all_webhook',
         'field': 'approve_all'}, 403)


@pytest.mark.parametrize('request_values, asked', [
    ({'approve_all': True}, True),
    ({'approve_all': False}, False),
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'mcp_auth_action': 'authorize', 'approve_all': True}, False),
], ids=['asked', 'plain approve', 'sign-in answer'])
def test_the_check_hands_the_approve_all_request_to_the_run_check(request_values, asked):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(**request_values), OWNER)

    assert calls[0][-1]['approve_all'] is asked


def test_the_check_never_carries_approve_all_on_a_sign_in_answer():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID})

    pending, _ = check(_parsed(hitl_resume=False, mcp_auth_resume=True, mcp_auth_action='authorize',
                               approve_all=True), OWNER)

    assert pending == (RUN_ID, 'approve', False)


def test_the_apply_passes_approve_all_to_the_run():
    _, apply, calls = _helpers({'schedule_run_id': RUN_ID})

    assert apply(PROJECT_ID, (RUN_ID, 'approve', True)) is None

    assert calls == [('apply', PROJECT_ID, RUN_ID, 'approve', True)]


def _kwargs_helper(approved_runs, allowed=True):
    """`_approve_all_kwargs` cut out of rpc/chat_all.py; `asked` records the run ids it looked up.

    `allowed` is the project setting "Allow Run without asking".
    """
    asked = []

    def approves_all(session, run_id):
        asked.append((session, run_id))
        return run_id in approved_runs

    nodes = [node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == '_approve_all_kwargs']
    namespace = load_cut(nodes, {
        'approves_all': approves_all, 'is_run_without_asking_allowed': lambda project_id: allowed,
        'log': types.SimpleNamespace(debug=lambda *_: None)}, 'chat_all.py')
    return namespace['_approve_all_kwargs'], asked


def test_a_resume_of_an_approve_all_run_passes_the_auto_approve_keyword():
    helper, asked = _kwargs_helper({RUN_ID})
    conversation = types.SimpleNamespace(meta={'schedule_run_id': RUN_ID})

    result = helper('session', conversation, 'applications_predict_sio', PROJECT_ID)
    assert result == {'auto_approve_sensitive_actions': True}
    assert asked == [('session', RUN_ID)]


def test_a_resume_after_the_setting_was_switched_off_passes_no_auto_approve_keyword():
    helper, _ = _kwargs_helper({RUN_ID}, allowed=False)
    conversation = types.SimpleNamespace(meta={'schedule_run_id': RUN_ID})

    assert helper('session', conversation, 'applications_predict_sio', PROJECT_ID) == {}


def test_a_resume_without_approve_all_passes_no_auto_approve_keyword():
    helper, _ = _kwargs_helper(set())
    conversation = types.SimpleNamespace(meta={'schedule_run_id': RUN_ID})

    assert helper('session', conversation, 'applications_predict_sio', PROJECT_ID) == {}


@pytest.mark.parametrize('rpc_func', ['llm_predict_sio', 'toolkits_predict_sio'])
def test_the_other_predict_rpcs_never_get_the_auto_approve_keyword(rpc_func):
    helper, asked = _kwargs_helper({RUN_ID})
    conversation = types.SimpleNamespace(meta={'schedule_run_id': RUN_ID})

    assert helper('session', conversation, rpc_func, PROJECT_ID) == {}
    assert asked == []


@pytest.mark.parametrize('meta', [None, {}, {'schedule_id': 3}])
def test_a_chat_that_is_not_a_schedule_run_never_gets_the_auto_approve_keyword(meta):
    helper, asked = _kwargs_helper({RUN_ID})

    assert helper('session', types.SimpleNamespace(meta=meta), 'applications_predict_sio', PROJECT_ID) == {}
    assert asked == []


def test_the_resume_call_adds_the_keyword_through_the_helper_for_every_resume_kind():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    call = body[body.index('getattr(self.context.rpc_manager.call, rpc_func)('):]
    call = call[:call.index('self.finalize_timed_out_response(')]

    assert '**_approve_all_kwargs(session, conversation, rpc_func, parsed.project_id)' in call
    assert body.count('getattr(self.context.rpc_manager.call, rpc_func)(') == 1  # HITL and sign-in resumes share it


class _Rows:
    """A fake query: every chained call returns itself, `first()` hands out the next row."""

    def __init__(self, rows):
        self._rows = rows

    def __getattr__(self, _name):
        return lambda *_, **__: self

    def first(self):
        return self._rows.pop(0)


class _PredictCalls:
    """`rpc_manager.call`: any attribute is a predict RPC that records its name and keywords."""

    def __init__(self):
        self.sent = []

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.sent.append((name, kwargs))
            return {'queued': True}
        return call


def _continue_predict(schedule_meta, events, allowed=True, **request):
    """Run the real `continue_predict_sio` of rpc/chat_all.py; return the predict calls it made.

    Every name the method reads from the module is a MagicMock unless it is set here.
    """
    cls = next(node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == 'RPC')
    method = next(node for node in cls.body if getattr(node, 'name', None) == 'continue_predict_sio')
    method.decorator_list, method.returns = [], None
    for arg in method.args.args:
        arg.annotation = None
    used = {node.id for node in ast.walk(method) if isinstance(node, ast.Name)} - set(dir(builtins))
    helpers = [node for node in ast.parse(CHAT_ALL).body
               if getattr(node, 'name', None) in {'_chat_trigger_kwargs', '_approve_all_kwargs'}]
    calls = _PredictCalls()
    resumed = types.SimpleNamespace(author_participant=None, reply_to_id=None, uuid='m-1')
    conversation = types.SimpleNamespace(meta=schedule_meta, uuid='c-1')
    response = types.SimpleNamespace(
        meta={'generation': 'g'}, message_items=[], author_participant=types.SimpleNamespace(entity_name='agent'),
        author_participant_id=3, uuid='m-1', reply_to_id=5, is_streaming=False, id=6)
    question = types.SimpleNamespace(uuid='q-1')
    rows = _Rows([resumed, conversation, response, question])  # in the order the method reads them
    session = types.SimpleNamespace(
        query=lambda *_: rows, commit=lambda: None,
        refresh=lambda *_: None, add=lambda *_: None, flush=lambda: None)

    @contextmanager
    def get_session(_project_id):
        yield session

    parsed = types.SimpleNamespace(**{
        'project_id': PROJECT_ID, 'message_id': 'm-1', 'conversation_uuid': 'c-1', 'thread_id': None,
        'hitl_resume': False, 'mcp_auth_resume': False, 'hitl_decisions': None, 'hitl_action': 'approve',
        'hitl_value': None, 'authorization_request_id': 'a-1', 'mcp_auth_action': 'authorize',
        'token_limit_continuation': False, **request})
    owner = types.SimpleNamespace(
        is_chat_run_stopped=lambda _id: False, _continue_child_resume=lambda *_, **__: None,
        _offer_supervised_decision=lambda *_: None, finalize_timed_out_response=lambda *_, **__: None,
        context=types.SimpleNamespace(sio=MagicMock(), rpc_manager=types.SimpleNamespace(call=calls)))
    namespace = {name: MagicMock() for name in used}
    namespace.update({
        'db': types.SimpleNamespace(get_session=get_session),
        'SioValidationError': lambda **kwargs: Invalid(kwargs['error']),
        'SioContinuePredictModel': types.SimpleNamespace(model_validate=lambda _data: parsed),
        'auth': types.SimpleNamespace(current_user=lambda **_: {'id': OWNER}),
        '_check_schedule_decision': lambda *_: (None, None),
        '_resume_user_id': lambda *_: OWNER,  # the run acts as the caller, so no plain-answer guard
        '_apply_schedule_decision': lambda *_: None,
        'CHAT_PREDICT_MAPPER': {'agent': 'applications_predict_sio'},
        'TriggerType': types.SimpleNamespace(CHAT='chat'),
        'is_run_without_asking_allowed': lambda _project_id: allowed,
        'approves_all': lambda _session, run_id: any(
            item.get('kind') == 'decision' and item.get('approve_all') is True for item in events.get(run_id, [])),
        'EXECUTION_GENERATION_KEY': 'generation', 'INTERNAL_CONTINUE_TOKEN': object(),
        'pending_interrupts': lambda _meta: [{'id': 'i-1'}],
        'pending_authorization_requests': lambda _meta: [{'id': 'a-1'}],
        'interrupt_identity': lambda item: item['id'], 'authorization_identity': lambda item: item['id'],
        'get_direct_pipeline_hitl_interrupt': lambda *_: None,
        'retire_interrupts': lambda meta, _ids: meta, 'retire_authorization_requests': lambda meta, _ids: meta,
        'generate_payload': lambda *_, **__: {'thread_id': 't-1'},
        'prepare_conversation_history': lambda *_: ([], [], True),
        'generate_payload_chat_history': lambda *_, **__: ([], []),
        'get_context_manager_feature_flag': lambda _project_id: False,
        'is_token_limit_continuation': lambda **_: False,
        'pop_skill_dispatch': lambda *_: ({}, None), 'pop_chat_skills': lambda _payload: {},
        'authoring_kwargs': lambda *_: {},
    })
    namespace = load_cut([*helpers, method], namespace, 'chat_all.py')
    result = namespace['continue_predict_sio'](owner, None, **{'data': {}})
    assert result == {'queued': True}
    return calls.sent


APPROVE_ALL_EVENTS = {RUN_ID: [{'kind': 'paused'}, {'kind': 'decision', 'approve_all': True}]}
PLAIN_APPROVE_EVENTS = {RUN_ID: [{'kind': 'paused'}, {'kind': 'decision', 'text': 'Approved by the schedule owner'}]}


@pytest.mark.parametrize('request_values', [
    {'hitl_resume': True},
    {'mcp_auth_resume': True},
], ids=['sensitive-tool approval', 'sign-in resume'])
def test_a_resumed_approve_all_schedule_run_predicts_with_the_auto_approve_keyword(request_values):
    ((rpc_func, kwargs),) = _continue_predict({'schedule_run_id': RUN_ID}, APPROVE_ALL_EVENTS, **request_values)

    assert rpc_func == 'applications_predict_sio'
    assert kwargs['auto_approve_sensitive_actions'] is True


@pytest.mark.parametrize('request_values', [
    {'hitl_resume': True},
    {'mcp_auth_resume': True},
], ids=['sensitive-tool approval', 'sign-in resume'])
def test_a_resumed_schedule_run_without_approve_all_predicts_without_the_keyword(request_values):
    ((_, kwargs),) = _continue_predict({'schedule_run_id': RUN_ID}, PLAIN_APPROVE_EVENTS, **request_values)

    assert 'auto_approve_sensitive_actions' not in kwargs
    assert kwargs['trigger_source'] == 'chat'


@pytest.mark.parametrize('meta', [None, {}, {'schedule_id': 3}])
def test_a_resumed_chat_that_is_not_a_schedule_run_predicts_without_the_keyword(meta):
    ((_, kwargs),) = _continue_predict(meta, APPROVE_ALL_EVENTS, hitl_resume=True)

    assert 'auto_approve_sensitive_actions' not in kwargs


def test_a_resumed_approve_all_run_predicts_without_the_keyword_once_the_setting_is_off():
    ((_, kwargs),) = _continue_predict({'schedule_run_id': RUN_ID}, APPROVE_ALL_EVENTS, allowed=False, hitl_resume=True)

    assert 'auto_approve_sensitive_actions' not in kwargs


def test_a_plain_approve_or_reject_passes_when_the_run_acts_as_another_user():
    refuse, _ = _bare()

    assert refuse(_parsed(hitl_action='approve')) is None
    assert refuse(_parsed(hitl_action='reject', hitl_value='')) is None
    assert refuse(_parsed(hitl_action=None, hitl_decisions=[
        {'tool_call_id': 't1', 'interrupt_id': 'i1', 'action': 'approve', 'value': ''}, {'action': 'reject'}])) is None


@pytest.mark.parametrize('values', [
    {'hitl_action': 'edit'},
    {'hitl_action': 'answer'},
    {'hitl_action': 'block_with_comment'},
    {'hitl_action': 'approve', 'hitl_value': 'delete everything'},
    {'hitl_action': 'approve', 'hitl_value': {'answer': 'yes'}},
    {'hitl_action': 'approve', 'user_input': 'run this instead'},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': 'edit', 'value': 'x'}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': 'approve', 'value': 'x'}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': 'approve', 'edited_action': {'args': {}}}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': 'approve', 'text': 'x'}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': ['approve']}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{'action': 5}]},
    {'hitl_action': 'approve', 'hitl_decisions': [{}]},
    {'hitl_action': 'approve', 'hitl_decisions': ['approve']},
    {'hitl_action': ['approve']},
    {'hitl_action': 'approve', 'mcp_auth_decisions': [{'action': 'authorize', 'token': 't'}]},
    {'hitl_action': 'approve', 'mcp_auth_action': 'approve'},
])
def test_an_edit_or_a_typed_answer_is_refused_when_the_run_acts_as_another_user(values):
    refuse, _ = _bare()

    denied = refuse(_parsed(**values))

    assert denied['status'] == 400 and denied['field'] == 'hitl_action'


@pytest.mark.parametrize('values, refused', [
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'mcp_auth_action': 'authorize', 'hitl_action': None}, False),
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'mcp_auth_action': 'skip', 'hitl_action': None}, False),
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'mcp_auth_action': 'approve', 'hitl_action': None}, True),
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'hitl_action': None,
      'mcp_auth_decisions': [{'action': 'authorize', 'authorization_request_id': 'a1'}]}, False),
    ({'hitl_resume': False, 'mcp_auth_resume': True, 'hitl_action': None,
      'mcp_auth_decisions': [{'action': 'authorize', 'access_token': 'secret'}]}, True),
])
def test_a_sign_in_answer_is_plain_only_when_the_run_acts_as_another_user(values, refused):
    refuse, _ = _bare()

    assert (refuse(_parsed(**values)) is not None) is refused


@pytest.mark.parametrize('field, value', [
    ('approve_all', True),
    ('approve_all', 'yes'),
    ('approve_all', 1),
    ('approve_all', [['github', '*']]),
    ('token_limit_continuation', True),
    ('a_field_added_later', {'x': 1}),
])
def test_a_field_outside_the_plain_answer_is_refused_when_the_run_acts_as_another_user(field, value):
    refuse, _ = _bare()

    denied = refuse(_parsed(**{field: value}))

    assert denied['status'] == 400 and denied['field'] == 'hitl_action'


@pytest.mark.parametrize('field, value', [
    ('approve_all', False),
    ('approve_all', None),
    ('token_limit_continuation', False),
    ('should_continue', True),
    ('question_id', 'q-1'),
])
def test_an_unset_or_identity_field_does_not_refuse_a_plain_answer(field, value):
    refuse, _ = _bare()

    assert refuse(_parsed(**{field: value})) is None


def test_a_field_the_client_did_not_send_is_not_checked():
    refuse, _ = _bare()
    parsed = _parsed()
    parsed.approve_all = True  # a default on the model, not sent by the client

    assert refuse(parsed) is None


def test_a_run_started_by_its_owner_resumes_as_the_owner_and_gets_no_refusal_check():
    _, _, _, resume_user_id = _helpers({'schedule_run_id': RUN_ID}, actor=OWNER, resume=True)

    assert resume_user_id(PROJECT_ID, (RUN_ID, 'approve'), OWNER) == OWNER  # the guard runs only when this differs


def test_the_resume_of_another_users_run_gets_neither_the_decider_socket_nor_the_decider_as_user():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    call = body[body.index('rpc_func)('):]
    call = call[:call.index('user_id=resume_user_id')]

    assert "sid if resume_user_id == current_user['id'] else None, payload" in call


def test_the_inputs_of_the_deciding_user_are_dropped_before_the_resume():
    _, drop = _bare()
    parsed = _parsed(mcp_tokens={'https://mcp': 'owner-token'}, ignored_mcp_servers=['https://x'],
                     user_declined_mcp_servers=[{'url': 'https://y'}], user_input='hello',
                     llm_settings={'model_name': 'other'}, attachments_info=[{'name': 'a.txt'}])

    drop(parsed)

    assert (parsed.mcp_tokens, parsed.ignored_mcp_servers, parsed.user_declined_mcp_servers, parsed.user_input,
            parsed.llm_settings, parsed.attachments_info) == ({}, [], None, None, None, None)


def test_the_plain_answer_guard_runs_before_the_child_resume_and_every_write():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    guard = body.index('_bare_decision_refusal(parsed)')

    assert body.index('resume_user_id = _resume_user_id(') < guard
    assert 'if resume_user_id != current_user[\'id\']:' in body[:guard]
    assert body.index('_drop_caller_inputs(parsed)') < body.index('self.is_chat_run_stopped(parsed.message_id)')
    assert guard < body.index('self._continue_child_resume(') < body.index('_apply_schedule_decision(parsed.project_id')
