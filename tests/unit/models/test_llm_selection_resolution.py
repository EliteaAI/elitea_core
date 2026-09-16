"""Exercise the actual resolver function without importing Pylon or accessing DBs.

The function is compiled from its owning source; dependencies live only in this
test namespace, so no synthetic imports leak into other test modules.
"""
import ast
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock
from types import SimpleNamespace
import pytest
from fixtures.helpers import load_module_with_stubs


@pytest.fixture
def resolver(models_path):
    llm = load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_routing_resolution_llm')
    path = models_path.parent / 'utils' / 'application_utils.py'
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == 'validate_and_resolve_llm_settings')
    rpc_tools = MagicMock()
    available = rpc_tools.RpcMixin.return_value.rpc.timeout.return_value.configurations_get_available_models
    available.return_value = {(42, 'chosen-model'): {'supports_reasoning': True}}
    namespace = {'Optional': Optional, 'LLMSettingsModel': llm.LLMSettingsModel,
                 'rpc_tools': rpc_tools, 'log': MagicMock(),
                 'llm_settings_family_conflict': llm.llm_settings_family_conflict,
                 '_normalize_llm_settings_family': llm._normalize_llm_settings_family}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace['validate_and_resolve_llm_settings'], available


def test_auto_survives_version_resolution_without_default_lookup(resolver):
    resolve, lookup = resolver
    selection = {'mode': 'auto', 'profile_ref': {'id': 'quality-auto', 'revision': 1},
                 'scope_mode': 'agent_task', 'reasoning': {'mode': 'auto'}}
    source = {'selection': selection, 'max_tokens': 1000}
    result = resolve(42, source, include_openai_compatible=True)
    assert result['selection'] == selection and result['model_name'] is None
    assert 'openai_compatible' not in result
    assert source == {'selection': selection, 'max_tokens': 1000}
    lookup.assert_not_called()


def test_explicit_inherit_is_not_a_model_choice(resolver):
    resolve, lookup = resolver
    with pytest.raises(ValueError):
        resolve(42, {'selection': {'mode': 'inherit'}})
    lookup.assert_not_called()


def test_legacy_fixed_resolution_is_unchanged(resolver):
    resolve, lookup = resolver
    fixed = {'model_name': 'chosen-model', 'model_project_id': 42, 'reasoning_effort': 'high'}
    assert resolve(42, fixed) == fixed
    lookup.assert_called_once()


def test_versioned_fixed_selection_keeps_unavailable_binding(resolver):
    resolve, lookup = resolver
    selection = {'mode': 'fixed', 'model_ref': {'name': 'removed-model', 'project_id': 42}}
    result = resolve(42, {'selection': selection})
    assert result['model_name'] == 'removed-model'
    assert result['selection']['model_ref'] == selection['model_ref']
    lookup.assert_called_once()


def test_conflicting_binding_fails_before_lookup(resolver):
    resolve, lookup = resolver
    with pytest.raises(ValueError):
        resolve(42, {'selection': {'mode': 'inherit'}, 'model_name': 'chosen-model'})
    lookup.assert_not_called()


@pytest.mark.parametrize('stored_type,stored_settings,fields', [
    ('pipeline', {'model_name': 'fixed'}, {'llm_settings'}),
    ('openai', {'selection': {'mode': 'auto'}}, {'agent_type'}),
])
def test_partial_write_uses_merged_persisted_type_before_mutation(models_path, stored_type, stored_settings, fields):
    llm = load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_routing_writer_llm')
    path = models_path.parent / 'utils' / 'application_utils.py'
    function = next(node for node in ast.parse(path.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'applications_update_version')
    error_type = type('VersionNotUpdatableError', (Exception,), {})
    namespace = {'ApplicationVersion': MagicMock(), 'VersionNotUpdatableError': error_type,
                 'validate_model_selection_surface': llm.validate_model_selection_surface}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    session = MagicMock()
    version = SimpleNamespace(agent_type=stored_type, llm_settings=stored_settings)
    session.query.return_value.filter.return_value.first.return_value = version
    patch = SimpleNamespace(id=1, model_fields={'id': object()}, model_fields_set=fields,
                            agent_type='pipeline' if 'agent_type' in fields else 'openai',
                            llm_settings={'selection': {'mode': 'auto'}})
    with pytest.raises(error_type, match='this release'):
        namespace['applications_update_version'](patch, session)
    session.commit.assert_not_called()
    session.flush.assert_not_called()
    assert version.agent_type == stored_type and version.llm_settings == stored_settings


def test_missing_stored_model_resolves_project_default_before_child_guard(resolver):
    resolve, lookup = resolver
    rpc = resolve.__globals__['rpc_tools'].RpcMixin.return_value.rpc.timeout.return_value
    rpc.configurations_get_default_model.return_value = {'model_name':'chosen-model','model_project_id':42}
    result = resolve(42, None, include_openai_compatible=True)
    assert result['model_name']=='chosen-model' and result['model_project_id']==42
    assert result['openai_compatible'] is False


def test_expanded_agent_details_always_resolve_effective_default(models_path):
    path=models_path.parent/'utils/application_utils.py'
    fn=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='get_application_version_details_expanded')
    calls=[n for n in ast.walk(fn) if isinstance(n,ast.Assign) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='validate_and_resolve_llm_settings']
    assert len(calls)==1
    namespace={'result':{'llm_settings':None},'validate_and_resolve_llm_settings':MagicMock(return_value={'model_name':'default'}),
               'project_id':42,'application_id':1,'version_id':2}
    exec(compile(ast.Module(body=calls,type_ignores=[]),str(path),'exec'),namespace)
    assert namespace['result']['llm_settings']=={'model_name':'default'}
    for n in ast.walk(fn):
        if isinstance(n,ast.If):
            assert calls[0] not in n.body
