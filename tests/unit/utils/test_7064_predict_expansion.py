"""Issue #7064 - every predict path sends the expanded agent, so the runtime never PATCHes it back.

Before #7064, REST / MCP / webhook predicts and scheduled pipelines named a version by id but sent
an empty ``version_details``. The SDK then fetched the expanded version over
``PATCH elitea_core/version``. Scheduled pipelines also skipped the sub-agent prefetch, because
``is_system_user`` implies ``skip_expansion``.

``generate_predict_payload`` now expands the named version itself, with ``expand_version_for_runtime``
(exactly what PATCH returned), and always attaches the sub-agent prefetch. A top-level expansion
error still stops the run, as the runtime's own PATCH did; sub-agents stay best effort. System-token runs expand
as the project system user, which is the identity the runtime's own PATCH used: owner-bound
scheduled runs are #7011's job, so this deliberately preserves today's behaviour.

The builder is executed from source with its collaborators replaced, the same seam
``test_auto_routing_predict_payload.py`` uses. The two new helpers are executed for real.

Run via:
    python tests/run_tests.py unit/utils/test_7064_predict_expansion.py -v
"""
import ast
import copy
from types import SimpleNamespace as NS
from typing import Optional, Union
from unittest.mock import Mock

import pytest

from fixtures.helpers import load_module_with_stubs


class Chat(NS):
    def __getattr__(self, key):
        return None


class Agent(Chat):
    pass


class PayloadError(Exception):
    pass


EXPANDED = {
    'agent_type': 'openai',
    'instructions': 'expanded instructions',
    'llm_settings': {'model_name': 'stored-model', 'max_tokens': 1000},
    'tools': [{'type': 'github', 'name': 'gh', 'settings': {'access_token': 'unsecreted'}}],
    'attached_skills': [{'name': 'reviewer'}],
}

PREDICT_FUNCTIONS = ('generate_predict_payload', 'get_expansion_user_id', 'expand_top_level_version')


@pytest.fixture
def env(models_path):
    llm = load_module_with_stubs(models_path / 'pd/llm.py', 'p7064_llm')
    path = models_path.parent / 'utils/predict_utils.py'
    functions = [n for n in ast.parse(path.read_text()).body
                 if isinstance(n, ast.FunctionDef) and n.name in PREDICT_FUNCTIONS]

    class DropImports(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            return None

    functions = [DropImports().visit(f) for f in functions]
    calls = NS(expand=[], prefetch=[], resolve=[], system_user=[], token=[])
    env_flags = {}   # 'run_token': enforce with a signing key (C9-SEC-47)

    rpc = Mock()
    rpc.configurations_get_configuration_model.return_value = {'supports_reasoning': False}

    def admin_get_project_system_user(project_id):
        calls.system_user.append(project_id)
        return {'id': 900 + project_id}

    rpc.admin_get_project_system_user.side_effect = admin_get_project_system_user

    def expand_version_for_runtime(project_id, application_id, version_id, user_id):
        calls.expand.append((project_id, application_id, version_id, user_id))
        return copy.deepcopy(env.expanded)

    def attach_subagent_prefetch(payload, project_id, user_id):
        calls.prefetch.append((project_id, user_id))
        payload['application']['subagent_version_details'] = {}

    def get_system_token(project_id):
        calls.token.append(('system', project_id))
        return 'system-token'

    def get_predict_token_and_session(project_id, user_id, sid, run_identity=False, mandate_run=False):
        calls.token.append(('user', user_id))
        return 'user-token', None

    namespace = {
        'Union': Union, 'Optional': Optional, 'LLMChatRequest': Chat, 'ApplicationChatRequest': Agent,
        '_normalize_llm_settings_family': llm._normalize_llm_settings_family,
        'PredictPayloadError': PayloadError,
        'VaultClient': lambda project: NS(get_all_secrets=lambda: {}),
        'rpc_tools': NS(RpcMixin=lambda: NS(rpc=NS(call=rpc, timeout=lambda seconds: rpc))),
        'get_predict_token_and_session': get_predict_token_and_session,
        'get_system_token': get_system_token,
        'run_identity_applies': lambda sid=None, mandate=False: mandate and env_flags.get('run_token', False),
        'get_session_cookie_name': lambda: None,
        'get_predict_base_url': lambda *a: 'https://unit.invalid',
        'get_public_base_url': lambda: 'https://public.invalid',
        'normalize_runtime_max_tokens': lambda value: value,
        'next_input_suggestion_config': lambda *a: {'enabled': False},
        'serialize': lambda value: value,
        'AgentTypes': NS(pipeline=NS(value='pipeline')), 'resolve_application_name': lambda p: 'fixture',
        'resolve_runtime_skills': lambda version: [], 'consume_message_skills': lambda text, skills: (text, []),
        'merge_mention_candidates': lambda attached, chat, project=None: [*attached, *chat, *(project or [])],
        'merge_disclosable_skills': lambda own, chat: [*own, *chat],
        'expand_version_for_runtime': expand_version_for_runtime,
        'attach_subagent_prefetch': attach_subagent_prefetch,
        'inject_stored_logins': lambda payload, project_id, user_id: None,
        'dedupe_internal_mcp_tools': lambda tools: None,
        'resolve_internal_mcp_tools': lambda tools, user_id, project_id: calls.resolve.append(user_id),
        'log': NS(warning=lambda *a, **k: None, debug=lambda *a, **k: None, info=lambda *a, **k: None),
    }
    exec(compile(ast.Module(functions, []), str(path), 'exec'), namespace)

    env = NS(build=namespace['generate_predict_payload'], llm=llm, calls=calls,
             expanded=copy.deepcopy(EXPANDED), flags=env_flags)
    return env


def agent_request(env, version_details=None, tools=None):
    """An id-only request, as do_predict / predict_sio build it from the ORM row."""
    return Agent(
        project_id=7, application_id=3, version_id=4, user_input='go', chat_history=[], thread_id='t',
        llm_settings=env.llm.LLMSettingsModel(model_name='request-model', max_tokens=500),
        tools=tools if tools is not None else [{'type': 'github', 'name': 'raw orm row'}],
        version_details=version_details,
    )


def test_an_id_only_request_carries_the_expanded_version(env):
    result = env.build(agent_request(env), 2)

    details = result['application']['version_details']
    assert details['instructions'] == 'expanded instructions'
    assert details['tools'][0]['settings'] == {'access_token': 'unsecreted'}
    assert env.calls.expand == [(7, 3, 4, 2)]


def test_the_request_model_still_overrides_the_stored_one(env):
    # Same override the chat path applies to its own expanded details
    llm_settings = env.build(agent_request(env), 2)['application']['version_details']['llm_settings']

    assert llm_settings['model_name'] == 'request-model'
    assert llm_settings['max_tokens'] == 500


def test_the_raw_orm_tools_are_not_sent_next_to_the_expanded_ones(env):
    # The runtime reads payload['tools'] for attachments; raw rows would duplicate every toolkit
    result = env.build(agent_request(env), 2)

    assert result['tools'] == EXPANDED['tools']


def test_details_already_present_are_not_expanded_again(env):
    # Chat and evaluation runs bring their own expanded version_details
    own = {'agent_type': 'openai', 'llm_settings': {'model_name': 'x'}, 'tools': []}
    result = env.build(agent_request(env, version_details=own, tools=[]), 2)

    assert env.calls.expand == []
    assert result['application']['version_details']['agent_type'] == 'openai'


def test_a_request_without_a_version_is_left_alone(env):
    request = agent_request(env)
    request.version_id = None

    env.build(request, 2)

    assert env.calls.expand == []


def test_an_expansion_error_stops_the_predict(env):
    env.expanded = {'error': "Configuration 'gh' not found"}

    with pytest.raises(PayloadError, match="Configuration 'gh' not found"):
        env.build(agent_request(env), 2)


def test_system_token_runs_expand_as_the_project_system_user(env):
    # Same identity the runtime's PATCH used; the schedule owner is #7011's scope
    env.build(agent_request(env), 2, is_system_user=True)

    assert env.calls.system_user == [7]
    assert env.calls.expand == [(7, 3, 4, 907)]
    assert env.calls.prefetch == [(7, 907)]
    assert env.calls.token == [('system', 7)]


def test_a_missing_system_user_stops_the_predict(env, monkeypatch):
    request = agent_request(env)
    rpc = env.build.__globals__['rpc_tools'].RpcMixin().rpc.call
    rpc.admin_get_project_system_user.side_effect = lambda project_id: None

    with pytest.raises(PayloadError, match='no system user'):
        env.build(request, 2, is_system_user=True)


def test_real_user_runs_expand_and_prefetch_as_that_user(env):
    env.build(agent_request(env), 2)

    assert env.calls.system_user == []
    assert env.calls.prefetch == [(7, 2)]
    assert env.calls.token == [('user', 2)]


def test_skip_expansion_still_prefetches_sub_agents(env):
    # Evaluation runs pass skip_expansion; their sub-agents used to be fetched over PATCH
    own = {'agent_type': 'openai', 'llm_settings': {'model_name': 'x'}, 'tools': []}
    env.build(agent_request(env, version_details=own, tools=[]), 2, skip_expansion=True)

    assert env.calls.prefetch == [(7, 2)]
    assert env.calls.resolve == []


def test_without_skip_expansion_internal_mcp_tools_are_still_resolved(env):
    env.build(agent_request(env), 2)

    assert env.calls.resolve


def test_plain_llm_chat_neither_expands_nor_prefetches(env):
    request = Chat(project_id=7, user_input='hi', chat_history=[], thread_id='t', tools=[],
                   llm_settings=env.llm.LLMSettingsModel(model_name='m', max_tokens=10))

    env.build(request, 2, is_system_user=True)

    assert env.calls.expand == [] and env.calls.prefetch == [] and env.calls.system_user == []


def test_inline_agents_without_an_application_skip_the_system_user_lookup(env):
    # Draft generators and the judge send inline version_details with no application id
    own = {'agent_type': 'openai', 'llm_settings': {'model_name': 'x'}, 'tools': []}
    request = agent_request(env, version_details=own, tools=[])
    request.application_id = None
    request.version_id = None

    env.build(request, 2, is_system_user=True)

    assert env.calls.system_user == [] and env.calls.expand == []
    assert env.calls.prefetch == [(7, 2)]


def test_a_mandate_trigger_run_under_enforce_reads_no_system_token(env):
    """C9-SEC-47: a scheduled or webhook run admitted under a mandate runs on its run token; the project system token is
    neither read nor created for it. Off, audit only or no signing key: as before."""
    env.flags['run_token'] = True
    result = env.build(agent_request(env), 2, is_system_user=True, mandate_run=True)
    assert ('system', 7) not in env.calls.token and not result['llm']['kwargs'].get('api_key')
    env.flags['run_token'] = False
    result = env.build(agent_request(env), 2, is_system_user=True, mandate_run=True)
    assert ('system', 7) in env.calls.token and result['llm']['kwargs']['api_key'] == 'system-token'


def test_a_trigger_run_without_a_mandate_keeps_its_system_token(env):
    env.flags['run_token'] = True
    result = env.build(agent_request(env), 2, is_system_user=True)
    assert ('system', 7) in env.calls.token and result['llm']['kwargs']['api_key'] == 'system-token'
