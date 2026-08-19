import importlib
import pathlib
import sys
import types
from enum import Enum
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
MODELS_ROOT = PLUGIN_ROOT / 'models'
UTILS_ROOT = PLUGIN_ROOT / 'utils'


def _package(name, path):
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module
    return module


def _module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class _PermissiveModel(BaseModel):
    model_config = ConfigDict(extra='allow')


class _AgentTypes(Enum):
    pipeline = 'pipeline'


_package('models', MODELS_ROOT)
_package('models.pd', MODELS_ROOT / 'pd')
_package('models.enums', MODELS_ROOT / 'enums')
_module('models.pd.tool', ToolChatModel=_PermissiveModel, ToolDetails=_PermissiveModel)
_module('models.pd.utils', MergeUpdateBase=_PermissiveModel)
_module(
    'models.pd.version',
    ApplicationVariableModel=_PermissiveModel,
    LLMSettingsModel=_PermissiveModel,
)
_module('models.enums.all', AgentTypes=_AgentTypes)
_module('models.elitea_tools', EliteATool=object)
_module(
    'jinja2',
    Environment=object,
    DebugUndefined=object,
)

chat = importlib.import_module('models.pd.chat')
ApplicationChatRequest = chat.ApplicationChatRequest
LLMChatRequest = chat.LLMChatRequest

_package('utils', UTILS_ROOT)
_module('flask')
_module('pylon')
_package('pylon.core', PLUGIN_ROOT)
_module(
    'pylon.core.tools',
    log=SimpleNamespace(
        debug=lambda *_args, **_kwargs: None,
        info=lambda *_args, **_kwargs: None,
        warning=lambda *_args, **_kwargs: None,
        error=lambda *_args, **_kwargs: None,
        exception=lambda *_args, **_kwargs: None,
    ),
)
_module(
    'tools',
    VaultClient=object,
    db=SimpleNamespace(),
    serialize=lambda value: value,
    this=SimpleNamespace(),
    auth=SimpleNamespace(),
    rpc_tools=SimpleNamespace(RpcMixin=None),
    context=SimpleNamespace(),
)
_module('utils.llm_settings', get_default_max_tokens=lambda _supports_reasoning: 100)
_module('utils.next_input_suggestion_utils', next_input_suggestion_config=lambda _project_id: {'enabled': False})
_module(
    'utils.skill_utils',
    consume_invoked_skills=lambda message, _skills: (message, []),
    resolve_runtime_skills=lambda _details: [],
)
_module('utils.application_tools', expand_toolkit_settings=lambda tools, *_args: tools)
_module(
    'utils.internal_tools',
    resolve_internal_mcp_tools=lambda *_args: None,
    dedupe_internal_mcp_tools=lambda *_args: None,
)
_package('plugins', PLUGIN_ROOT.parent)
_package('plugins.elitea_core', PLUGIN_ROOT)
for short_name in (
    'models', 'models.elitea_tools', 'models.enums', 'models.enums.all',
    'models.pd', 'models.pd.chat', 'models.pd.tool', 'models.pd.utils',
    'models.pd.version', 'utils', 'utils.application_tools',
    'utils.internal_tools', 'utils.llm_settings',
    'utils.next_input_suggestion_utils', 'utils.skill_utils',
):
    sys.modules[f'plugins.elitea_core.{short_name}'] = sys.modules[short_name]
predict_utils = importlib.import_module('plugins.elitea_core.utils.predict_utils')


AUTH_FIELDS = {
    'mcp_auth_resume': True,
    'mcp_auth_action': 'skip',
    'mcp_auth_decisions': [
        {'interrupt_id': 'mcp_auth_child', 'action': 'skip'},
    ],
    'authorization_request_id': 'mcp_auth_root',
}


def test_application_and_llm_models_preserve_toolkit_authorization_resume():
    common = {
        'project_id': 2,
        'user_input': 'continue',
        'llm_settings': {'model_name': 'test-model'},
        **AUTH_FIELDS,
    }

    application = ApplicationChatRequest.model_validate({
        **common,
        'version_details': {'agent_type': 'agent'},
    })
    llm = LLMChatRequest.model_validate(common)

    for parsed in (application, llm):
        assert parsed.model_dump(include=set(AUTH_FIELDS)) == AUTH_FIELDS


def test_generate_predict_payload_forwards_toolkit_authorization_resume(monkeypatch):
    class FakeVaultClient:
        def __init__(self, _project_id):
            pass

        @staticmethod
        def get_all_secrets():
            return {}

    class FakeRpcCall:
        @staticmethod
        def configurations_get_configuration_model(_project_id, _model_name):
            return {'supports_reasoning': False, 'openai_compatible': True}

    class FakeRpc:
        rpc = SimpleNamespace(call=FakeRpcCall())

    monkeypatch.setattr(predict_utils, 'VaultClient', FakeVaultClient)
    monkeypatch.setattr(
        predict_utils, 'get_predict_token_and_session',
        lambda *_args, **_kwargs: ('token', 'session'),
    )
    monkeypatch.setattr(
        predict_utils, 'get_predict_base_url',
        lambda _project_id: 'https://llm.example',
    )
    monkeypatch.setattr(predict_utils.rpc_tools, 'RpcMixin', lambda: FakeRpc())
    monkeypatch.setattr(
        predict_utils, 'next_input_suggestion_config',
        lambda _project_id: {'enabled': False},
    )

    parsed = SimpleNamespace(
        project_id=2,
        llm_settings=SimpleNamespace(
            model_name='test-model', model_project_id=None,
            max_tokens=100, temperature=0.5, reasoning_effort=None,
        ),
        chat_history=[],
        user_input='continue',
        instructions='',
        thread_id='thread-1',
        checkpoint_id=None,
        tools=[],
        internal_tools=[],
        mcp_tokens={},
        ignored_mcp_servers=[],
        user_declined_mcp_servers=[],
        should_continue=False,
        hitl_resume=False,
        hitl_action=None,
        hitl_value=None,
        hitl_decisions=None,
        execution_generation='generation-1',
        is_regenerate=False,
        meta={},
        context_settings=None,
        invoked_skills=[],
        steps_limit=None,
        stream_id='stream-1',
        **AUTH_FIELDS,
    )

    payload = predict_utils.generate_predict_payload(parsed, user_id=3, sid='sid-1')

    assert {key: payload[key] for key in AUTH_FIELDS} == AUTH_FIELDS
