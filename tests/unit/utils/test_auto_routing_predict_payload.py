"""Execute the owning Core payload builder with real selection validation.

Only its two local imports are replaced by directly loaded owning functions;
authentication/catalog/secret/skill adapters are fixture seams, not live calls.
"""
import ast
import copy
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Union
from unittest.mock import Mock
import pytest
from pydantic import BaseModel
from fixtures.helpers import load_module_with_stubs


class Chat(NS):
    def __getattr__(self, key):
        return None


class Agent(Chat):
    pass


@pytest.fixture
def builder(models_path):
    llm = load_module_with_stubs(models_path/'pd/llm.py', 'auto_payload_llm')
    path = models_path.parent/'utils/predict_utils.py'
    function = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == 'generate_predict_payload')
    class Imports(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            assert node.module == 'models.pd.llm'
            return None
    function = Imports().visit(function)
    rpc = Mock()
    rpc.configurations_get_auto_routing_settings.return_value = {'enabled': True}
    rpc.configurations_get_configuration_model.return_value = {'supports_reasoning': True, 'max_output_tokens': 16000}
    namespace = {'Union': Union, 'LLMChatRequest': Chat, 'ApplicationChatRequest': Agent,
        'validate_model_selection_surface': llm.validate_model_selection_surface,
        '_normalize_llm_settings_family': llm._normalize_llm_settings_family,
        'PredictPayloadError': ValueError, 'VaultClient': lambda project: NS(get_all_secrets=lambda: {}),
        'rpc_tools': NS(RpcMixin=lambda: NS(rpc=NS(call=rpc))),
        'get_predict_token_and_session': lambda *a: ('fixture-only', None),
        'get_predict_base_url': lambda *a: 'https://unit.invalid',
        'normalize_runtime_max_tokens': lambda value: None if value in (None, -1) else value,
        'next_input_suggestion_config': lambda *a: {'enabled': False},
        'serialize': lambda value: value,
        'AgentTypes': NS(pipeline=NS(value='pipeline')), 'resolve_application_name': lambda p: 'fixture',
        'resolve_runtime_skills': lambda version: [], 'consume_invoked_skills': lambda text, skills: (text, []),
    }
    exec(compile(ast.Module([function], []), str(path), 'exec'), namespace)
    return namespace['generate_predict_payload'], llm, rpc


def parsed(llm, agent=False, auto=True):
    settings = {'selection': {'mode': 'auto', 'profile_ref': {'id': 'v7-quality-cost', 'revision': 1},
                'scope_mode': 'task_episode', 'reasoning': {'mode': 'explicit', 'preset': 'medium'}}, 'max_tokens': 8000}
    if not auto:
        settings = {'model_name': 'fixed-model', 'reasoning_effort': 'high', 'max_tokens': 8000}
    cls = Agent if agent else Chat
    return cls(project_id=7, llm_settings=llm.LLMSettingsModel(**settings), chat_history=[],
        user_input='Build the delegated worker', thread_id='root-thread', tools=[], instructions='fixture',
        version_details={'agent_type': 'openai', 'llm_settings': copy.deepcopy(settings)} if agent else None)


@pytest.mark.parametrize('agent,surface', [(False, 'chat'), (True, 'agent')])
def test_auto_request_reaches_worker_without_default_model_resolution(builder, agent, surface):
    build, llm, rpc = builder;value = parsed(llm, agent)
    before = copy.deepcopy(value.version_details)
    result = build(value, 2, skip_expansion=True)
    assert result['routing_principal']=={'project_id':7,'user_id':2}
    config = result['llm']['kwargs']
    assert config['model'] is None and config['selection']['reasoning']['preset'] == 'medium'
    assert config['routing_surface'] == surface and config['project_id'] == 7
    assert result['user_input'] == value.user_input and result['thread_id'] == 'root-thread'
    rpc.configurations_get_configuration_model.assert_not_called()
    rpc.configurations_get_auto_routing_settings.assert_called_once_with(7)
    assert value.version_details == before
    if agent:
        assert result['application']['version_details']['llm_settings']['selection'] == config['selection']


def test_fixed_predict_bypasses_auto_and_keeps_effort(builder):
    build, llm, rpc = builder
    result = build(parsed(llm, auto=False), 2, skip_expansion=True)
    assert result['llm']['kwargs']['model'] == 'fixed-model'
    assert result['llm']['kwargs']['reasoning_effort'] == 'high'
    assert 'selection' not in result['llm']['kwargs']
    rpc.configurations_get_auto_routing_settings.assert_not_called()


def test_revoked_auto_and_pipeline_fail_before_worker_payload(builder):
    build, llm, rpc = builder
    rpc.configurations_get_auto_routing_settings.return_value = {'enabled': False}
    with pytest.raises(ValueError, match='disabled'):
        build(parsed(llm), 2, skip_expansion=True)
    pipeline = parsed(llm, agent=True);pipeline.version_details['agent_type'] = 'pipeline'
    with pytest.raises(ValueError, match='release'):
        build(pipeline, 2, skip_expansion=True)


@pytest.fixture
def application_settings_resolver(models_path):
    path = models_path.parent / 'rpc/chat_all.py'
    functions = [n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef)
                 and n.name == '_resolve_application_llm_settings']
    llm = load_module_with_stubs(models_path / 'pd/llm.py', 'auto_override_llm')
    namespace = {'PublishStatus': NS(published='published'), 'get_public_project_id': lambda: 1,
                 'merge_llm_selection_override': llm.merge_llm_selection_override}
    exec(compile(ast.Module(functions, []), str(path), 'exec'), namespace)
    return namespace['_resolve_application_llm_settings'], llm.merge_llm_selection_override


@pytest.mark.parametrize('saved_auto,requested', [
    (True, {'model_name': 'gpt-5.4', 'model_project_id': 1, 'reasoning_effort': 'medium',
            'max_tokens': 8000, 'temperature': None}),
    (True, {'selection': {'mode': 'fixed', 'model_ref': {'name': 'gpt-5.4', 'project_id': 1},
                          'reasoning': {'mode': 'explicit', 'preset': 'high'}}}),
    (False, {'selection': {'mode': 'auto', 'profile_ref': {'id': 'v7-quality-cost', 'revision': 1},
                           'scope_mode': 'agent_task', 'reasoning': {'mode': 'auto'}}}),
    (False, {'selection': {'mode': 'auto', 'profile_ref': {'id': 'v7-quality-cost', 'revision': 1},
                           'scope_mode': 'agent_task', 'reasoning': {'mode': 'explicit', 'preset': 'high'}}}),
])
def test_saved_agent_selection_can_be_overridden_both_directions_through_worker_payload(
        builder, application_settings_resolver, saved_auto, requested):
    build, llm, rpc = builder
    resolve, _ = application_settings_resolver
    value = parsed(llm, agent=True, auto=saved_auto)
    original = copy.deepcopy(value.version_details)
    request = NS(llm_settings=llm.LLMSettingsModel(**requested))
    resolved = resolve(value.version_details, NS(llm_settings=None), request, 7)
    value.llm_settings = llm.LLMSettingsModel(**resolved)
    result = build(value, 2, skip_expansion=True)
    config = result['llm']['kwargs']
    if requested.get('selection', {}).get('mode') == 'auto':
        assert config['model'] is None
        assert config['selection'] == requested['selection']
        assert value.llm_settings.model_name is None and value.llm_settings.model_project_id is None
        assert value.llm_settings.reasoning_effort == requested['selection']['reasoning'].get('preset')
        rpc.configurations_get_configuration_model.assert_not_called()
    else:
        assert config['model'] == 'gpt-5.4'
        assert config['reasoning_effort'] == ('medium' if 'model_name' in requested else 'high')
        assert 'selection' not in config
        assert 'selection' not in result['application']['version_details']['llm_settings']
        rpc.configurations_get_auto_routing_settings.assert_not_called()
    assert config['max_tokens'] == 8000
    assert value.version_details == original


def test_selection_merge_is_shared_with_plain_chat_and_preserves_omitted_settings(
        builder, application_settings_resolver, models_path):
    _, llm, _ = builder
    _, merge = application_settings_resolver
    saved = parsed(llm, agent=True).version_details['llm_settings']
    original = copy.deepcopy(saved)
    request = {'model_name': 'gpt-5.4', 'reasoning_effort': 'none', 'temperature': None}
    result = merge(saved, request)
    assert result['max_tokens'] == saved['max_tokens']
    assert result['reasoning_effort'] == 'none' and result['temperature'] is None
    assert 'selection' not in result and saved == original
    source = ast.parse((models_path.parent / 'rpc/chat_all.py').read_text())
    function = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == 'generate_payload')
    calls = [n for n in ast.walk(function) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == 'merge_llm_selection_override']
    assert len(calls) == 1


def test_no_override_and_public_stored_override_keep_precedence(builder, application_settings_resolver):
    _, llm, _ = builder
    resolve, _ = application_settings_resolver
    value = parsed(llm, agent=True)
    public = NS(llm_settings=llm.LLMSettingsModel(model_name='public-fixed', max_tokens=4000))
    no_request = NS(llm_settings=None)
    baseline = copy.deepcopy(value.version_details)
    assert resolve(baseline, public, no_request, 7) == baseline['llm_settings']
    baseline['status'] = 'published'
    assert resolve(baseline, public, no_request, 1)['model_name'] == 'public-fixed'
    request = NS(llm_settings=llm.LLMSettingsModel(model_name='request-fixed', reasoning_effort='high'))
    result = resolve(baseline, public, request, 1)
    assert result['model_name'] == 'request-fixed' and result['max_tokens'] == 4000


def test_pipeline_still_rejects_transient_auto_but_accepts_explicit_model(builder, application_settings_resolver):
    build, llm, _ = builder
    resolve, _ = application_settings_resolver
    value = parsed(llm, agent=True, auto=False)
    value.version_details['agent_type'] = 'pipeline'
    auto = NS(llm_settings=parsed(llm).llm_settings)
    result = resolve(value.version_details, NS(llm_settings=None), auto, 7)
    value.llm_settings = llm.LLMSettingsModel(**result)
    with pytest.raises(ValueError, match='Pipelines'):
        build(value, 2, skip_expansion=True)
    fixed = NS(llm_settings=llm.LLMSettingsModel(model_name='pipeline-fixed', reasoning_effort='high'))
    value.llm_settings = llm.LLMSettingsModel(**resolve(value.version_details, NS(llm_settings=None), fixed, 7))
    assert build(value, 2, skip_expansion=True)['llm']['kwargs']['model'] == 'pipeline-fixed'


@pytest.fixture
def application_request_models(builder, models_path):
    """Actual request classes and generic merge, with unrelated tool DTO seams."""
    _, llm, _ = builder
    utils = load_module_with_stubs(models_path / 'pd/utils.py', 'auto_actual_merge_base')
    path = models_path / 'pd/chat.py'
    tree = ast.parse(path.read_text())
    tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and n.level)]
    namespace = {'ToolChatModel': BaseModel, 'ApplicationVariableModel': BaseModel,
                 'LLMSettingsModel': llm.LLMSettingsModel, 'MergeUpdateBase': utils.MergeUpdateBase,
                 'merge_llm_selection_override': llm.merge_llm_selection_override,
                 '__name__': 'auto_actual_chat_models'}
    exec(compile(tree, str(path), 'exec'), namespace)
    return NS(**{k: namespace[k] for k in ('ApplicationChatRequest', 'LLMChatRequest')})


@pytest.mark.parametrize('saved_auto,requested_auto,effort', [
    (True, False, 'medium'), (True, False, 'none'),
    (False, True, None), (False, True, 'high'), (True, True, 'high'), (False, False, 'medium'),
])
def test_actual_application_dto_reload_merge_then_worker_payload(
        builder, application_settings_resolver, application_request_models, saved_auto, requested_auto, effort):
    build, llm, rpc = builder
    resolve, _ = application_settings_resolver
    cls = application_request_models.ApplicationChatRequest
    version = parsed(llm, agent=True, auto=saved_auto).version_details
    saved = cls.model_validate(NS(application_id=33, id=48, project_id=7,
                                  llm_settings=version['llm_settings'], instructions='Saved instructions',
                                  meta={'nested': {'saved': True}}))
    request_settings = {'max_tokens': 8000, 'temperature': None}
    if requested_auto:
        request_settings['selection'] = {'mode': 'auto', 'profile_ref': {'id': 'v7-quality-cost', 'revision': 1},
                                        'scope_mode': 'agent_task', 'reasoning': {'mode': 'auto'} if effort is None
                                        else {'mode': 'explicit', 'preset': effort}}
    else:
        request_settings.update(model_name='gpt-5.4', model_project_id=1, reasoning_effort=effort)
    resolved = resolve(version, NS(llm_settings=None), NS(llm_settings=llm.LLMSettingsModel(**request_settings)), 7)
    incoming = cls.model_validate({'application_id': 33, 'version_id': 48, 'project_id': 7,
                                   'llm_settings': resolved, 'version_details': copy.deepcopy(version),
                                   'user_input': 'Synthetic delegated task', 'meta': {'nested': {'request': True}}})
    before_saved, before_incoming = saved.model_dump(), incoming.model_dump()
    if saved_auto != requested_auto:
        # This was the second installed failure: generic recursive merging
        # combines two already-valid DTOs into an invalid mixed selection.
        with pytest.raises(ValueError, match='Auto selection cannot contain'):
            super(cls, saved).merge_update(incoming)
    merged = saved.merge_update(incoming)  # Exact reloaded-version boundary in both runtime call sites.
    assert merged.instructions == 'Saved instructions'
    assert merged.meta == {'nested': {'saved': True, 'request': True}}
    assert merged.llm_settings.reasoning_effort == effort
    assert saved.model_dump() == before_saved and incoming.model_dump() == before_incoming
    build.__globals__.update(ApplicationChatRequest=cls, LLMChatRequest=application_request_models.LLMChatRequest)
    result = build(merged, 2, skip_expansion=True)
    config = result['llm']['kwargs']
    if requested_auto:
        assert config['model'] is None and config['selection'] == request_settings['selection']
        rpc.configurations_get_configuration_model.assert_not_called()
    else:
        assert config['model'] == 'gpt-5.4' and config['reasoning_effort'] == effort
        assert 'selection' not in config
        assert 'selection' not in result['application']['version_details']['llm_settings']
        rpc.configurations_get_auto_routing_settings.assert_not_called()
    assert config['max_tokens'] == 8000


def test_actual_dto_merge_no_selection_override_and_parameter_only_update(application_request_models):
    cls = application_request_models.ApplicationChatRequest
    saved = cls(project_id=7, application_id=33, version_id=48,
                llm_settings={'model_name': 'saved', 'model_project_id': 1, 'reasoning_effort': 'high', 'max_tokens': 5000})
    no_override = cls(project_id=7, application_id=33, version_id=48, user_input='Task')
    assert saved.merge_update(no_override).llm_settings == saved.llm_settings
    parameter_only = no_override.model_copy(update={'llm_settings': saved.llm_settings.__class__(max_tokens=8000)})
    changed = saved.merge_update(parameter_only)
    assert changed.llm_settings.model_name == 'saved' and changed.llm_settings.reasoning_effort == 'high'
    assert changed.llm_settings.max_tokens == 8000


def test_actual_dto_typed_fixed_replaces_saved_auto_and_pipeline_stays_fixed(
        builder, application_request_models):
    build, llm, _ = builder
    cls = application_request_models.ApplicationChatRequest
    saved = cls(project_id=7, application_id=33, version_id=48,
                llm_settings=parsed(llm).llm_settings)
    fixed = cls(project_id=7, application_id=33, version_id=48,
                llm_settings={'selection': {'mode': 'fixed', 'model_ref': {'name': 'chosen', 'project_id': 1},
                                           'reasoning': {'mode': 'explicit', 'preset': 'high'}}})
    merged = saved.merge_update(fixed)
    assert merged.llm_settings.model_name == 'chosen' and merged.llm_settings.reasoning_effort == 'high'
    assert merged.llm_settings.selection.mode == 'fixed'
    merged.version_details = {'agent_type': 'pipeline', 'llm_settings': merged.llm_settings.model_dump()}
    merged.user_input = 'Pipeline task'
    build.__globals__.update(ApplicationChatRequest=cls, LLMChatRequest=application_request_models.LLMChatRequest)
    assert build(merged, 2, skip_expansion=True)['llm']['kwargs']['model'] == 'chosen'
    auto = merged.merge_update(saved)
    with pytest.raises(ValueError, match='Pipelines'):
        build(auto, 2, skip_expansion=True)


def test_all_existing_reload_merge_sites_use_the_application_request_class(models_path):
    for relative in ('rpc/application.py', 'methods/predict.py'):
        tree = ast.parse((models_path.parent / relative).read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute) and n.func.attr == 'merge_update']
        assert len(calls) == 1
        assert any(isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                   and n.value.id == 'ApplicationChatRequest' and n.attr == 'from_orm' for n in ast.walk(tree))
