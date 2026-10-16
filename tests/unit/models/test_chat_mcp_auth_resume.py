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
_module(
    'utils.llm_settings',
    normalize_runtime_max_tokens=lambda value: None if value in (None, -1) else value,
)
_module('utils.next_input_suggestion_utils', next_input_suggestion_config=lambda _project_id: {'enabled': False})
_module(
    'utils.skill_utils',
    consume_message_skills=lambda message, _skills: (message, []),
    resolve_runtime_skills=lambda _details: [],
)
_module(
    'utils.skill_mentions',
    merge_mention_candidates=lambda attached, chat: [*attached, *chat],
    merge_disclosable_skills=lambda own, chat: [*own, *chat],
)
_module('utils.application_tools', expand_toolkit_settings=lambda tools, *_args: tools)
_module(
    'utils.internal_tools',
    resolve_internal_mcp_tools=lambda *_args: None,
    dedupe_internal_mcp_tools=lambda *_args: None,
    MCP_CURRENT_PROJECT_SUFFIXES={'elitea_core/project_context', 'elitea_core/skills'},
)
_package('plugins', PLUGIN_ROOT.parent)
_package('plugins.elitea_core', PLUGIN_ROOT)
for short_name in (
    'models', 'models.elitea_tools', 'models.enums', 'models.enums.all',
    'models.pd', 'models.pd.chat', 'models.pd.tool', 'models.pd.utils',
    'models.pd.version', 'utils', 'utils.application_tools',
    'utils.internal_tools', 'utils.llm_settings',
    'utils.next_input_suggestion_utils', 'utils.skill_utils', 'utils.skill_mentions',
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

TOKEN_CONTINUATION_FIELDS = {
    'truncated_content': 'Visible partial answer',
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


def test_application_and_llm_models_preserve_visible_token_continuation():
    common = {
        'project_id': 2,
        'user_input': 'continue',
        'llm_settings': {'model_name': 'test-model'},
        **TOKEN_CONTINUATION_FIELDS,
    }

    application = ApplicationChatRequest.model_validate({
        **common,
        'version_details': {'agent_type': 'agent'},
    })
    llm = LLMChatRequest.model_validate(common)

    for parsed in (application, llm):
        assert parsed.model_dump(include=set(TOKEN_CONTINUATION_FIELDS)) == TOKEN_CONTINUATION_FIELDS


def _patch_predict_env(monkeypatch):
    class FakeVaultClient:
        def __init__(self, _project_id):
            pass

        @staticmethod
        def get_all_secrets():
            return {}

    class FakeRpcCall:
        @staticmethod
        def configurations_get_configuration_model(_project_id, _model_name):
            return {
                'supports_reasoning': False,
                'openai_compatible': True,
                'max_output_tokens': 64000,
            }

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
    monkeypatch.setattr(predict_utils, 'get_public_base_url', lambda: 'https://public.example')
    monkeypatch.setattr(predict_utils.rpc_tools, 'RpcMixin', lambda: FakeRpc())
    monkeypatch.setattr(
        predict_utils, 'next_input_suggestion_config',
        lambda _project_id: {'enabled': False},
    )


def _parsed_namespace(**overrides):
    fields = dict(
        project_id=2,
        llm_settings=SimpleNamespace(
            model_name='test-model', model_project_id=None,
            max_tokens=-1, temperature=0.5, reasoning_effort=None,
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
    fields.update(overrides)
    return SimpleNamespace(**fields)


def test_generate_predict_payload_forwards_toolkit_authorization_resume(monkeypatch):
    _patch_predict_env(monkeypatch)
    parsed = _parsed_namespace()

    payload = predict_utils.generate_predict_payload(parsed, user_id=3, sid='sid-1')

    assert {key: payload[key] for key in AUTH_FIELDS} == AUTH_FIELDS
    assert 'max_tokens' not in payload['llm']['kwargs']
    assert payload['llm']['kwargs']['max_output_tokens'] == 64000


ESCALATION = {'event': 'regenerate', 'from_message_id': 'msg-uuid-1'}


def test_llm_and_application_models_declare_regenerate_and_escalation():
    # LLMChatRequest used to lack is_regenerate, so the real (non-permissive) model dropped it.
    for model in (ApplicationChatRequest, LLMChatRequest):
        assert {'is_regenerate', 'escalation'} <= set(model.model_fields)

    common = {'project_id': 2, 'user_input': 'again', 'llm_settings': {'model_name': 'test-model'},
              'is_regenerate': True, 'escalation': ESCALATION}
    llm = LLMChatRequest.model_validate(common)
    application = ApplicationChatRequest.model_validate({**common, 'version_details': {'agent_type': 'agent'}})
    for parsed in (llm, application):
        assert parsed.is_regenerate is True
        assert parsed.escalation == ESCALATION


def test_escalation_absent_defaults_to_none_and_validates_shape():
    import pytest
    from pydantic import ValidationError
    common = {'project_id': 2, 'user_input': 'hi', 'llm_settings': {'model_name': 'test-model'}}
    for model, extra in ((LLMChatRequest, {}), (ApplicationChatRequest, {'version_details': {'agent_type': 'agent'}})):
        assert model.model_validate({**common, **extra}).escalation is None
        assert model.model_validate({**common, **extra, 'escalation': None}).escalation is None
        upgrade = model.model_validate({**common, **extra, 'escalation': {'event': 'upgrade_control'}})
        assert upgrade.escalation == {'event': 'upgrade_control', 'from_message_id': None}
        for bad in ({'event': 'correction'}, {}, {'from_message_id': 'm'}, 'regenerate',
                    {'event': 'regenerate', 'from_message_id': 5},
                    {'event': 'regenerate', 'extra': 1}):
            with pytest.raises(ValidationError):
                model.model_validate({**common, **extra, 'escalation': bad})


def test_generate_predict_payload_carries_escalation_on_llm_and_application_paths(monkeypatch):
    _patch_predict_env(monkeypatch)
    common = {'project_id': 2, 'user_input': 'again', 'llm_settings': {'model_name': 'test-model'},
              'is_regenerate': True, 'escalation': ESCALATION}
    llm = LLMChatRequest.model_validate(common)
    application = ApplicationChatRequest.model_validate(
        {**common, 'version_details': {'agent_type': 'agent', 'llm_settings': {}}})
    for parsed in (llm, application):
        # The real parsed model feeds the payload builder; only the LLM settings object is swapped
        # for the namespace the builder reads attributes from.
        parsed.llm_settings = _parsed_namespace().llm_settings
        payload = predict_utils.generate_predict_payload(parsed, user_id=3, sid='sid-1')
        assert payload['is_regenerate'] is True
        assert payload['escalation'] == ESCALATION

    plain = LLMChatRequest.model_validate({k: v for k, v in common.items() if k not in ('is_regenerate', 'escalation')})
    plain.llm_settings = _parsed_namespace().llm_settings
    payload = predict_utils.generate_predict_payload(plain, user_id=3, sid='sid-1')
    assert payload['escalation'] is None
    assert not payload['is_regenerate']


def test_build_regenerate_escalation_owns_message_id_and_allows_upgrade_control():
    build = chat.build_regenerate_escalation
    expected = {'event': 'regenerate', 'from_message_id': 'server-uuid'}
    assert build(None, 'server-uuid') == expected
    assert build({'event': 'regenerate', 'from_message_id': 'forged'}, 'server-uuid') == expected
    assert build({'event': 'bogus'}, 'server-uuid') == expected
    assert build('garbage', 'server-uuid') == expected
    assert build({'event': 'upgrade_control', 'from_message_id': 'forged'}, 'server-uuid') == {
        'event': 'upgrade_control', 'from_message_id': 'server-uuid'}

