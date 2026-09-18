"""Execute creation/default functions with stubbed storage and RPC owners."""
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fixtures.helpers import load_module_with_stubs


AUTO = {'selection': {'mode': 'auto', 'profile_ref': {'id': 'v7-quality-cost', 'revision': 1},
                      'scope_mode': 'task_episode', 'reasoning': {'mode': 'auto'}}}
FIXED = {'model_name': 'concrete', 'model_project_id': 7}


@pytest.fixture
def defaults(models_path):
    llm = load_module_with_stubs(models_path/'pd/llm.py', 'creation_defaults_llm')
    path = models_path.parent/'utils/model_defaults.py'
    functions = [n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef)]
    rpc_tools, db = MagicMock(), MagicMock()
    rpc = rpc_tools.RpcMixin.return_value.rpc.timeout.return_value
    rpc.configurations_get_default_model.return_value = AUTO
    namespace = {'db': db, 'rpc_tools': rpc_tools, 'ParticipantMapping': MagicMock(),
        'Participant': MagicMock(), 'ParticipantTypes': SimpleNamespace(user='user'), 'Conversation': MagicMock(),
        'merge_llm_selection_override': llm.merge_llm_selection_override}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), 'exec'), namespace)
    return SimpleNamespace(create=namespace['creation_llm_settings'], chat=namespace['chat_request_default'],
        rpc=rpc, session=db.get_session.return_value.__enter__.return_value, llm=llm, models_path=models_path)


@pytest.mark.parametrize('surface', ['chat', 'agent'])
def test_eligible_creation_saves_typed_intent(defaults, surface):
    result = defaults.create(7, surface=surface)
    assert defaults.llm.LLMSettingsWriteModel.model_validate(result).selection.mode == 'auto'
    defaults.rpc.configurations_get_default_model.assert_called_once_with(
        project_id=7, section='llm', include_shared=True, surface=surface)


@pytest.mark.parametrize('settings', [FIXED, {**FIXED, 'reasoning_effort': 'high'}, AUTO])
def test_explicit_settings_win_without_default_lookup(defaults, settings):
    assert defaults.create(7, settings, surface='agent') == settings
    defaults.rpc.configurations_get_default_model.assert_not_called()


def test_pipeline_and_generation_parameters_preserved(defaults):
    defaults.rpc.configurations_get_default_model.return_value = FIXED
    result = defaults.create(7, {'max_tokens': 700, 'temperature': .5}, surface='agent', agent_type='pipeline')
    assert result == {**FIXED, 'max_tokens': 700, 'temperature': .5}
    assert defaults.rpc.configurations_get_default_model.call_args.kwargs['surface'] == 'pipeline'


def test_explicit_effort_survives_auto_creation(defaults):
    result = defaults.create(7, {'max_tokens': 700, 'reasoning_effort': 'high'}, surface='agent')
    checked = defaults.llm.LLMSettingsWriteModel.model_validate(result)
    assert checked.selection.reasoning.preset == checked.reasoning_effort == 'high'
    assert checked.max_tokens == 700


@pytest.mark.parametrize('saved', [FIXED, AUTO])
def test_missing_request_keeps_saved_chat_choice(defaults, saved):
    query = defaults.session.query.return_value.join.return_value.join.return_value.filter.return_value
    query.first.return_value = SimpleNamespace(entity_settings={'llm_settings': saved})
    assert defaults.chat(7, 'chat-id', 42) == saved
    defaults.rpc.configurations_get_default_model.assert_not_called()


def test_legacy_missing_chat_settings_does_not_opt_into_auto(defaults):
    query = defaults.session.query.return_value.join.return_value.join.return_value.filter.return_value
    query.first.return_value = SimpleNamespace(entity_settings={})
    defaults.rpc.configurations_get_default_model.return_value = FIXED
    assert defaults.chat(7, 'chat-id', 42) == FIXED
    defaults.rpc.configurations_get_default_model.assert_called_once_with(
        project_id=7, section='llm', include_shared=True)


def test_actual_application_create_path_defaults_only_new_entities(defaults):
    path = defaults.models_path.parent/'utils/create_utils.py'
    fn = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == 'create_application')
    for node in ast.walk(fn):
        if isinstance(node, ast.If):
            node.body = [n for n in node.body if not isinstance(n, ast.ImportFrom)]
    class Create:
        def __init__(self): self.versions = [SimpleNamespace(llm_settings=None, agent_type='openai')]
        def dict(self, **kwargs): return {}
    class Import:
        def __init__(self): self.versions = [SimpleNamespace(llm_settings=None, agent_type='openai')]
        def dict(self, **kwargs): return {}
    application = SimpleNamespace(versions=[])
    write = MagicMock()
    namespace = {'ApplicationCreateModel': Create, 'ApplicationImportModel': Import,
        'Application': MagicMock(return_value=application), 'serialize': lambda v:v, 'store_secrets': MagicMock(),
        'creation_llm_settings': defaults.create, 'LLMSettingsWriteModel': defaults.llm.LLMSettingsWriteModel,
        'create_version': write}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'), namespace)
    new = Create()
    namespace[fn.name](new, MagicMock(), 7)
    assert write.call_args.args[0].llm_settings.selection.mode == 'auto'
    defaults.rpc.configurations_get_default_model.reset_mock()
    imported = Import()
    namespace[fn.name](imported, MagicMock(), 7)
    assert imported.versions[0].llm_settings is None
    defaults.rpc.configurations_get_default_model.assert_not_called()
    # The real DTOs are siblings; imports never satisfy the new-entity check.
    tree = ast.parse((defaults.models_path/'pd/application.py').read_text())
    imp = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ApplicationImportModel')
    assert 'ApplicationCreateModel' not in [getattr(b, 'id', '') for b in imp.bases]
