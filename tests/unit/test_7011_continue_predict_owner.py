"""EL-7011 US-18: the owner check runs before a schedule run resumes, and REST answers 403 or 409.

Contract under test: rpc/chat_all.py `_schedule_decision_error` and its place in
`continue_predict_sio`, and api/v2/continue_predict.py `PromptLibAPI.post`.
Nothing loads rpc/chat_all.py in the unit env, so the helper is compiled from the file's source
with fakes around it. Cases: TC-US18-05, TC-US18-08, TC-US18-09.

Run via:
    python tests/run_tests.py unit/test_7011_continue_predict_owner.py -v
"""
import ast
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = (PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text()
PROJECT_ID, RUN_ID, OWNER = 7, 9, 12


class Forbidden(Exception):
    pass


class NotWaiting(Exception):
    pass


def _helper(meta, error=None):
    """The real helper on a fake conversation meta; `calls` records each resume_schedule_run call."""
    calls = []

    def resume_schedule_run(*args):
        calls.append(args)
        if error:
            raise error

    query = types.SimpleNamespace(where=lambda *_: types.SimpleNamespace(scalar=lambda: meta))

    @contextmanager
    def get_session(project_id):
        yield types.SimpleNamespace(query=lambda *_: query)

    tree = ast.parse(CHAT_ALL)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_schedule_decision_error')
    namespace = {
        'db': types.SimpleNamespace(get_session=get_session),
        'Conversation': types.SimpleNamespace(meta='meta', uuid='uuid'),
        'resume_schedule_run': resume_schedule_run,
        'ScheduleDecisionForbidden': Forbidden,
        'ScheduleRunNotWaiting': NotWaiting,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'chat_all.py', 'exec'), namespace)  # noqa: S102 - runs the real helper cut out of rpc/chat_all.py
    return namespace['_schedule_decision_error'], calls


def _parsed(**values):
    return types.SimpleNamespace(**{
        'project_id': PROJECT_ID, 'conversation_uuid': 'c-1', 'hitl_resume': True,
        'hitl_action': 'approve', 'hitl_decisions': None, **values,
    })


def test_el_7011_r2_1_an_owner_approve_resumes_the_schedule_run():
    check, calls = _helper({'schedule_run_id': RUN_ID})

    assert check(_parsed(), OWNER) is None
    assert calls == [(PROJECT_ID, RUN_ID, OWNER, 'approve')]


def test_el_7011_r2_1_a_parallel_decision_passes_its_first_action():
    check, calls = _helper({'schedule_run_id': RUN_ID})

    check(_parsed(hitl_action=None, hitl_decisions=[{'action': 'reject'}]), OWNER)

    assert calls == [(PROJECT_ID, RUN_ID, OWNER, 'reject')]


def test_el_7011_r2_3_a_caller_other_than_the_owner_gets_403():
    check, _ = _helper({'schedule_run_id': RUN_ID}, Forbidden())

    assert check(_parsed(), 1)['status'] == 403


def test_el_7011_r2_4_a_run_not_waiting_gets_409():
    check, _ = _helper({'schedule_run_id': RUN_ID}, NotWaiting())

    assert check(_parsed(), OWNER)['status'] == 409


@pytest.mark.parametrize('meta, parsed', [
    (None, _parsed()),
    ({'schedule_id': 3}, _parsed()),
    ({'schedule_run_id': RUN_ID}, _parsed(hitl_resume=False)),
])
def test_el_7011_r2_1_other_conversations_and_resumes_skip_the_check(meta, parsed):
    check, calls = _helper(meta)

    assert check(parsed, OWNER) is None
    assert calls == []


def test_el_7011_r2_3_the_check_runs_before_any_resume_return():
    body = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio('):]
    check_index = body.index('_schedule_decision_error(parsed, current_user')

    assert body.index('current_user = auth.current_user()') < check_index
    assert body.index('if _internal_token is not INTERNAL_CONTINUE_TOKEN:') < check_index  # a replay was checked
    assert check_index < body.index('self.is_chat_run_stopped(parsed.message_id)')
    assert check_index < body.index('self._continue_child_resume(')


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
            PoolSaturationError=NotWaiting, BudgetDoorClosedError=NotWaiting),
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


@pytest.mark.parametrize('status', [403, 409])
def test_el_7011_r2_3_r2_4_rest_answers_the_status_of_the_decision(post, status):
    rpc, call = post
    rpc.result = {'error': 'denied', 'status': status}

    assert call() == ({'error': 'denied'}, status)


def test_el_7011_r2_4_rest_keeps_400_for_other_errors(post):
    rpc, call = post
    rpc.result = {'error': 'Redis connection error'}

    assert call() == ({'error': 'Redis connection error'}, 400)
