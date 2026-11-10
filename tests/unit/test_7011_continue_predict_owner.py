"""EL-7011 US-18: the owner check runs before a schedule run resumes, and REST answers 400, 403 or 409.

Contract under test: rpc/chat_all.py `_check_schedule_decision`, `_apply_schedule_decision` and their
places in `continue_predict_sio`, and api/v2/continue_predict.py `PromptLibAPI.post`.
Nothing loads rpc/chat_all.py in the unit env, so the helpers are compiled from the file's source
with fakes around them. Cases: TC-US18-05, TC-US18-08, TC-US18-09.

Run via:
    python tests/run_tests.py unit/test_7011_continue_predict_owner.py -v
"""
import ast
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest
from fixtures.cut_source import load_cut
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = (PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text()
PROJECT_ID, RUN_ID, OWNER, CONVERSATION_ID = 7, 9, 12, 55
HELPERS = {'_SCHEDULE_REFUSALS', '_SIGN_IN_REFUSAL', '_check_schedule_decision', '_apply_schedule_decision',
           '_DECISION_FIELDS', '_DECISION_ITEM_KEYS', '_filled', '_item_sends_more_than_a_decision',
           '_sends_more_than_a_decision'}
ACTOR = 31  # the user a schedule run acts as, when it is not the caller


class Forbidden(Exception):
    pass


class EditForbidden(Forbidden):
    pass


class Invalid(Exception):
    pass


class NotWaiting(Exception):
    pass


def _helpers(meta, error=None, decision='approve', apply_error=None):
    """The real helpers on a fake conversation meta; `calls` records each schedule_runs call."""
    calls = []

    def check_schedule_decision(project_id, run_id, caller_id, actions, fan_out=False, sign_in=False, is_admin=None,
                                conversation_id=None, recheck_actor=False, has_text=False):
        calls.append(('check', project_id, run_id, caller_id, actions,
                      {'fan_out': fan_out, 'sign_in': sign_in, 'is_admin': is_admin,
                       'conversation_id': conversation_id, 'recheck_actor': recheck_actor, 'has_text': has_text}))
        if error:
            raise error
        return decision

    def apply_schedule_decision(project_id, run_id, decision, decided_by=None):
        calls.append(('apply', project_id, run_id, decision, decided_by))
        if apply_error:
            raise apply_error

    query = types.SimpleNamespace(where=lambda *_: types.SimpleNamespace(
        first=lambda: types.SimpleNamespace(id=CONVERSATION_ID, meta=meta)))

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
        'ScheduleDecisionForbidden': Forbidden,
        'ScheduleEditForbidden': EditForbidden,
        'ScheduleDecisionInvalid': Invalid,
        'ScheduleRunNotWaiting': NotWaiting,
    }
    namespace = load_cut(nodes, namespace, 'chat_all.py')  # the real helpers cut out of rpc/chat_all.py
    return namespace['_check_schedule_decision'], namespace['_apply_schedule_decision'], calls


def _parsed(**values):
    return types.SimpleNamespace(**{
        'project_id': PROJECT_ID, 'conversation_uuid': 'c-1', 'hitl_resume': True,
        'hitl_action': 'approve', 'hitl_value': None, 'hitl_decisions': None, 'thread_id': None,
        'mcp_auth_resume': False, 'mcp_auth_action': None, 'mcp_auth_decisions': None, **values,
    })


def test_el_7011_r2_1_the_check_changes_nothing_and_the_apply_writes_the_decision():
    check, apply, calls = _helpers({'schedule_run_id': RUN_ID})

    pending, denied = check(_parsed(), OWNER)
    assert (pending, denied) == ((RUN_ID, 'approve', OWNER), None)
    assert calls == [('check', PROJECT_ID, RUN_ID, OWNER, ['approve'], {'fan_out': False, 'sign_in': False, 'is_admin': None,
                                                                  'conversation_id': CONVERSATION_ID,
                                                                  'recheck_actor': True, 'has_text': False})]

    assert apply(PROJECT_ID, pending) is None
    assert calls[-1] == ('apply', PROJECT_ID, RUN_ID, 'approve', OWNER)


def test_the_check_gets_the_id_of_the_conversation_that_holds_the_run_id():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(), OWNER)

    assert calls[0][-1]['conversation_id'] == CONVERSATION_ID


def test_el_7011_r2_2_a_decision_list_passes_every_action():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_action='approve', hitl_decisions=[{'action': 'approve'}, {'action': 'reject'}]), OWNER)

    assert calls[0][4] == ['approve', 'reject', 'approve']


def test_el_7011_r2_4_a_fan_out_child_on_a_resumed_run_has_nothing_to_apply():
    check, apply, calls = _helpers({'schedule_run_id': RUN_ID}, decision=None)

    pending, denied = check(_parsed(thread_id='child-1'), OWNER)

    assert (pending, denied) == ((RUN_ID, None, OWNER), None)  # the rights check passed: a child card may be answered
    assert calls[0][-1] == {'fan_out': True, 'sign_in': False, 'is_admin': None, 'conversation_id': CONVERSATION_ID,
                            'recheck_actor': True, 'has_text': False}
    assert apply(PROJECT_ID, pending) is None
    assert [call[0] for call in calls] == ['check']


@pytest.mark.parametrize('error, status', [(Forbidden(), 403), (Invalid(), 400), (NotWaiting(), 409)])
def test_el_7011_r2_3_r2_4_a_refused_check_answers_its_status(error, status):
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, error)

    pending, denied = check(_parsed(), 1)

    assert (pending, denied['status']) == (None, status)


def test_el_7011_r2_1_an_unknown_action_names_the_field():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, Invalid())

    assert check(_parsed(hitl_action='maybe'), OWNER)[1]['field'] == 'hitl_action'


def test_el_7011_r2_1_a_non_dict_decision_item_is_passed_as_no_action():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_decisions=['approve', {'action': 'approve'}]), OWNER)

    assert calls[0][4] == [None, 'approve', 'approve']


def test_el_7011_r2_4_an_apply_that_lost_a_race_gets_409():
    _, apply, _ = _helpers({'schedule_run_id': RUN_ID}, apply_error=NotWaiting())

    assert apply(PROJECT_ID, (RUN_ID, 'approve', OWNER))['status'] == 409


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


def _resume_user_id(actors):
    """The real `_resume_user_id` cut out of rpc/chat_all.py; `actors` maps a run id to the user it acts as."""
    nodes = [node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == '_resume_user_id']
    namespace = load_cut(nodes, {'schedule_run_actor_id': lambda project_id, run_id: actors.get(run_id)}, 'chat_all.py')
    return namespace['_resume_user_id']


@pytest.mark.parametrize('pending, caller, expected', [
    ((RUN_ID, 'approve', 1), 1, ACTOR),           # an admin decides: the run goes on as the user it acts as
    ((RUN_ID, 'approve', OWNER), OWNER, ACTOR),   # the user it acts as is the one stored, whoever the caller is
    ((RUN_ID, None, 1), 1, ACTOR),                # a later fan-out child of an already resumed pause
    ((RUN_ID + 1, 'approve', 1), 1, 1),           # the run is gone: the caller stays
    (None, OWNER, OWNER),                         # no schedule run in this chat
])
def test_a_decided_run_resumes_as_the_user_it_acts_as_not_as_the_deciding_admin(pending, caller, expected):
    assert _resume_user_id({RUN_ID: ACTOR})(PROJECT_ID, pending, caller) == expected


def _chat_helper(name):
    nodes = [node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == name]
    return load_cut(nodes, {}, 'chat_all.py')[name]


@pytest.mark.parametrize('author, user, participant, decided, allowed', [
    (OWNER, OWNER, False, False, True),    # the author
    (OWNER, 1, True, False, True),         # a participant
    (OWNER, 1, False, True, True),         # a schedules admin whose decision check passed
    (OWNER, 1, False, False, False),       # a stranger
    (OWNER, 1, None, False, False),        # no conversation row was read
])
def test_a_child_card_is_answered_by_the_author_a_participant_or_a_decided_caller(
        author, user, participant, decided, allowed):
    assert _chat_helper('_may_answer_child_card')(author, user, participant, decided) is allowed


def test_the_child_card_check_gets_the_decision_of_the_caller():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    assert 'self._continue_child_resume(sid, parsed, current_user, decided=schedule_decision is not None)' in body


def test_the_resume_user_of_the_helper_reaches_the_chat_entity_and_the_predict_call():
    # A wiring pin: the helper above is tested on its values; the method needs the whole chat stack to run
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    body = body[:body.index('\n    def ', 1)]

    assert "resume_user_id = _resume_user_id(parsed.project_id, schedule_decision, current_user['id'])" in body
    assert 'ParticipantEntityUser(id=resume_user_id)' in body and 'user_id=resume_user_id' in body
    assert 'ParticipantEntityUser(id=current_user' not in body and "user_id=current_user['id']" not in body


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
    payload = types.SimpleNamespace(model_validate=lambda raw: types.SimpleNamespace(
        conversation_uuid='c-1', message_id='m-1', mcp_auth_resume=False, hitl_resume=True,
        hitl_action='approve', hitl_value=None, hitl_decisions=None, mcp_auth_action=None,
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
    rpc = types.SimpleNamespace(result=None)
    handler = module.PromptLibAPI()
    handler.module = types.SimpleNamespace(chat_continue_predict_sio=lambda **_: rpc.result)
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


def test_a_sign_in_answer_is_checked_as_a_schedule_decision():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    pending, denied = check(
        _parsed(hitl_resume=False, hitl_action=None, mcp_auth_resume=True, mcp_auth_action='authorize'), OWNER)

    assert (pending, denied) == ((RUN_ID, 'approve', OWNER), None)
    assert calls == [('check', PROJECT_ID, RUN_ID, OWNER, ['authorize'], {'fan_out': False, 'sign_in': True, 'is_admin': None,
                                                                       'conversation_id': CONVERSATION_ID,
                                                                       'recheck_actor': True, 'has_text': False})]


def test_one_skip_among_the_sign_in_answers_rejects_the_run():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})
    answers = [{'interrupt_id': 'a', 'action': 'authorize'}, {'interrupt_id': 'b', 'action': 'skip'}]

    pending, denied = check(_parsed(hitl_resume=False, hitl_action=None, mcp_auth_resume=True,
                                    mcp_auth_action='authorize', mcp_auth_decisions=answers), OWNER)

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


def test_a_top_level_edit_next_to_a_decision_list_is_checked():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_decisions=[{'action': 'approve'}], hitl_action='edit', hitl_value='x'), OWNER)

    assert calls[0][4] == ['approve', 'edit']


def test_a_top_level_action_alone_is_the_whole_list():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_decisions=[], hitl_action='reject'), OWNER)

    assert calls[0][4] == ['reject']


def test_a_sign_in_answer_with_a_top_level_edit_is_checked():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})
    answers = [{'interrupt_id': 'a', 'action': 'authorize'}]

    check(_parsed(hitl_resume=False, hitl_action='edit', mcp_auth_resume=True, mcp_auth_action='authorize',
                  mcp_auth_decisions=answers), OWNER)

    assert calls[0][4] == ['authorize', 'edit']


def test_a_top_level_value_counts_as_text():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_value='edited text'), OWNER)

    assert calls[0][-1]['has_text'] is True


@pytest.mark.parametrize('values', [
    {'hitl_decisions': [{'action': 'approve', 'value': 'note'}]},
    {'hitl_resume': False, 'mcp_auth_resume': True, 'hitl_action': None, 'mcp_auth_action': 'authorize',
     'mcp_auth_decisions': [{'action': 'authorize', 'value': 'answer'}]},
], ids=['decision item', 'sign-in answer'])
def test_an_item_value_counts_as_text(values):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(**values), OWNER)

    assert calls[0][-1]['has_text'] is True


@pytest.mark.parametrize('values', [
    {'hitl_value': ''},
    {'hitl_value': {}},
    {'hitl_value': None},
    {'hitl_decisions': [{'action': 'approve', 'value': ''}, {'action': 'approve', 'value': None},
                        {'action': 'approve', 'value': {}}, {'action': 'approve'}]},
], ids=['empty string', 'empty dict', 'none', 'empty item values'])
def test_an_empty_value_is_not_text(values):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(**values), OWNER)

    assert calls[0][-1]['has_text'] is False


def test_the_edit_refusal_answers_its_own_403_text():
    check, _, _ = _helpers({'schedule_run_id': RUN_ID}, EditForbidden())
    rights_check, _, _ = _helpers({'schedule_run_id': RUN_ID}, Forbidden())

    pending, denied = check(_parsed(hitl_action='edit', hitl_value='x'), 1)
    _, rights_denied = rights_check(_parsed(), 1)

    assert pending is None
    assert (denied['status'], denied['error']) == (
        403, 'Only the user this run acts as can edit or answer in its chat')
    assert rights_denied['error'] == 'Only the schedule owner or a project admin can approve or reject this run'


def _unlisted_field(name='surprise'):
    """A request whose model reports `name` as set, as a field added to the model later would be."""
    return {'model_fields_set': {name}, name: 'x'}


@pytest.mark.parametrize('values', [
    {'hitl_decisions': [{'action': 'reject', 'user_feedback': 'text for the model'}]},
    {'hitl_decisions': [{'action': 'reject', 'surprise': 'x'}]},
    {'hitl_resume': False, 'mcp_auth_resume': True, 'hitl_action': None, 'mcp_auth_action': 'skip',
     'mcp_auth_decisions': [{'action': 'skip', 'user_feedback': 'text'}]},
    {'user_input': 'run this instead'},
    {'user_input': 'reject'},   # the action word is allowed only when the request carries that action
    {'llm_settings': {'model': 'other'}},
    {'attachments_info': [{'filepath': 'a.txt'}]},
    {'token_limit_continuation': True},
    _unlisted_field(),
], ids=['user_feedback item key', 'unknown item key', 'user_feedback sign-in key', 'custom user_input',
        'action word of another action', 'llm_settings', 'attachments_info', 'token_limit_continuation',
        'field the allowlist does not know'])
def test_a_request_with_more_than_a_decision_counts_as_text(values):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(**values), OWNER)

    assert calls[0][-1]['has_text'] is True


@pytest.mark.parametrize('values', [
    {'user_input': 'approve'},   # the chat box sends the action word
    {'user_input': ''},
    {'hitl_decisions': [{'action': 'approve', 'interrupt_id': 'i', 'tool_call_id': 't', 'child_thread_id': 'c',
                         'thread_id': 'th', 'value': ''}]},
    {'thread_id': 'th', 'interaction_uuid': 'u', 'question_id': 'q', 'should_continue': False,
     'mcp_tokens': {'https://mcp': {'access_token': 'x'}}, 'ignored_mcp_servers': ['https://other'],
     'user_declined_mcp_servers': [{'server_url': 'https://other'}], 'llm_settings': None, 'attachments_info': []},
    {'model_fields_set': {'project_id', 'hitl_action', 'mcp_tokens', 'surprise'}, 'surprise': None},
], ids=['action word', 'empty user_input', 'decision with its ids', 'routing fields and browser state',
        'unlisted field without a value'])
def test_a_decision_with_its_routing_ids_is_no_text(values):
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(**values), OWNER)

    assert calls[0][-1]['has_text'] is False


def test_the_action_word_of_a_sign_in_answer_is_no_text():
    check, _, calls = _helpers({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_resume=False, hitl_action=None, mcp_auth_resume=True, mcp_auth_action='skip',
                  user_input='skip'), OWNER)

    assert calls[0][-1]['has_text'] is False


def _model_fields(class_name):
    """The field names of a pydantic class in models/pd/predict.py, read from the source."""
    tree = ast.parse((PLUGIN_ROOT / 'models' / 'pd' / 'predict.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    return {item.target.id for item in node.body if isinstance(item, ast.AnnAssign)}


def test_every_field_of_the_continue_request_is_decided_for_a_user_who_is_not_the_actor():
    # A field is either allowed in `_DECISION_FIELDS` or refused by name in `_sends_more_than_a_decision`
    refused = {'llm_settings', 'attachments_info', 'token_limit_continuation'}
    nodes = [node for node in ast.parse(CHAT_ALL).body
             if any(getattr(target, 'id', None) == '_DECISION_FIELDS' for target in getattr(node, 'targets', []))]
    allowed = set(load_cut(nodes, {}, 'chat_all.py')['_DECISION_FIELDS'])

    fields = _model_fields('SioContinuePredictModel')

    assert fields <= allowed | refused
    assert not refused & allowed
    helper = CHAT_ALL[CHAT_ALL.index('def _sends_more_than_a_decision('):CHAT_ALL.index('def _drop_other_users_input(')]
    assert all(f"'{name}'" in helper for name in refused)


def _drop_helper():
    nodes = [node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == '_drop_other_users_input']
    return load_cut(nodes, {}, 'chat_all.py')['_drop_other_users_input']


def test_the_browser_credentials_and_input_of_a_non_actor_are_cleared():
    parsed = types.SimpleNamespace(
        mcp_tokens={'https://mcp': {'access_token': 'admin-token'}}, ignored_mcp_servers=['https://x'],
        user_declined_mcp_servers=[{'server_url': 'https://y'}], user_input='approve', hitl_action='approve')

    _drop_helper()(parsed)

    assert (parsed.mcp_tokens, parsed.ignored_mcp_servers, parsed.user_declined_mcp_servers, parsed.user_input) == (
        {}, [], None, None)
    assert parsed.hitl_action == 'approve'


def test_the_clear_runs_right_after_the_actor_is_known_and_before_any_token_use():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    body = body[:body.index('\n    def ', 1)]
    resume_index = body.index("resume_user_id = _resume_user_id(parsed.project_id, schedule_decision, current_user['id'])")

    assert ("        if schedule_decision is not None and resume_user_id != current_user['id']:\n"
            "            _drop_other_users_input(parsed)") in body
    assert resume_index < body.index('_drop_other_users_input(parsed)') < body.index('check_resume_input(')
    assert body.index('_drop_other_users_input(parsed)') < body.index('self._continue_child_resume(')
