import contextlib
import datetime
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = 'plugins.elitea_core'

CALLER_PROJECT_ID = 7
PUBLIC_PROJECT_ID = 1
DEFAULT_MODEL = {'model_name': 'default-model', 'model_project_id': CALLER_PROJECT_ID}
AVAILABLE_MODELS = {('saved-model', 3), ('override-model', 3)}


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def fake_resolve(project_id, llm_settings, **kwargs):
    settings = dict(llm_settings or {})
    name, project = settings.get('model_name'), settings.get('model_project_id')
    if name and project is None:
        project = next((p for (n, p) in AVAILABLE_MODELS if n == name), None)
    if name and (name, project) in AVAILABLE_MODELS:
        return {**settings, 'model_project_id': project}
    if fake_resolve.default is None:
        return llm_settings
    return {**settings, **fake_resolve.default}


class FakeVersion:
    def __init__(self, id, name='base', status='draft', instructions='You are a reviewer.',
                 created_at=1, meta=None, run_settings=None):
        self.id = id
        self.name = name
        self.status = status
        self.instructions = instructions
        self.created_at = datetime.datetime(2026, 1, created_at)
        self.meta = meta or {}
        if run_settings is not None:
            self.run_settings = run_settings


class FakeSkill:
    def __init__(self, id, name, versions, default_version_id=None):
        self.id = id
        self.name = name
        self.versions = versions
        self.default_version_id = default_version_id

    def get_default_version(self):
        by_id = next((v for v in self.versions if v.id == self.default_version_id), None)
        return by_id or next((v for v in self.versions if v.name == 'base'), None)


class FakeQuery:
    def __init__(self, skills):
        self.skills = skills

    def options(self, *args):
        return self

    def filter(self, wanted_id):
        return FakeQuery([s for s in self.skills if s.id == wanted_id])

    def first(self):
        return self.skills[0] if self.skills else None


class SkillIdColumn:
    def __eq__(self, other):
        return other


class FakeModule:
    def __init__(self, outcome=None, raises=None):
        self.outcome = {'result': {'chat_history': []}} if outcome is None else outcome
        self.raises = raises
        self.calls = []
        self.callback_tasks = {}
        self.not_starting_task_event = types.SimpleNamespace(
            clear=lambda: self.events.append('clear'), set=lambda: self.events.append('set'),
        )
        self.events = []

    def predict_sio(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return self.outcome


@pytest.fixture
def env(isolated_sys_modules):
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.pd',
                 f'{PACKAGE}.models.enums'):
        _package(name)

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **k: None)
    _package('pylon').core = _package('pylon.core')
    sys.modules['pylon.core'].tools = pylon_tools
    sys.modules['pylon.core.tools'] = pylon_tools

    schemas = {}

    @contextlib.contextmanager
    def with_project_schema_session(project_id):
        yield types.SimpleNamespace(query=lambda model: FakeQuery(schemas.get(project_id, [])))

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(with_project_schema_session=with_project_schema_session)
    sys.modules['tools'] = tools

    sqlalchemy_orm = types.ModuleType('sqlalchemy.orm')
    sqlalchemy_orm.selectinload = lambda *a: None
    _package('sqlalchemy').orm = sqlalchemy_orm
    sys.modules['sqlalchemy.orm'] = sqlalchemy_orm

    application_utils = types.ModuleType(f'{PACKAGE}.utils.application_utils')
    fake_resolve.default = dict(DEFAULT_MODEL)
    application_utils.validate_and_resolve_llm_settings = fake_resolve
    sys.modules[application_utils.__name__] = application_utils

    project_context = {'enabled': False, 'content': '', 'activation_description': None, 'revision': None}
    predict_utils = types.ModuleType(f'{PACKAGE}.utils.predict_utils')
    predict_utils.get_project_context = lambda project_id: project_context
    sys.modules[predict_utils.__name__] = predict_utils

    models_skill = types.ModuleType(f'{PACKAGE}.models.skill')
    models_skill.Skill = types.SimpleNamespace(id=SkillIdColumn(), versions=None)
    sys.modules[models_skill.__name__] = models_skill

    _load('models/enums/all.py', f'{PACKAGE}.models.enums.all')
    _load('models/pd/skill_predict.py', f'{PACKAGE}.models.pd.skill_predict')
    for name in ('exceptions', 'sio_utils', 'project_context_utils', 'usage_attribution'):
        _load(f'utils/{name}.py', f'{PACKAGE}.utils.{name}')
    module = _load('utils/skill_run_utils.py', f'{PACKAGE}.utils.skill_run_utils')
    return types.SimpleNamespace(mod=module, schemas=schemas, project_context=project_context)


def _run(env, module, body, *, skill_project_id=CALLER_PROJECT_ID, skill_id=10, version_id=None,
         published_only=False, async_requested=False):
    return env.mod.execute_skill_predict(
        module, body,
        caller_project_id=CALLER_PROJECT_ID,
        skill_project_id=skill_project_id,
        skill_id=skill_id,
        version_id=version_id,
        published_only=published_only,
        user_id=42,
        async_requested=async_requested,
    )


@pytest.fixture
def own_skill(env):
    skill = FakeSkill(10, 'Reviewer', [
        FakeVersion(100, 'base', instructions='Base instructions.', meta={'icon_meta': {'url': 'i.png'}}),
        FakeVersion(101, 'v2', instructions='V2 instructions.'),
    ])
    env.schemas[CALLER_PROJECT_ID] = [skill]
    return skill


class TestVersionSelection:
    def test_default_version_runs_when_no_version_is_given(self, env, own_skill):
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200
        assert body['skill_version_id'] == 100 and body['version_name'] == 'base'
        assert module.calls[0]['data']['version_details']['instructions'] == 'Base instructions.'

    def test_explicit_version_runs_exactly_that_version(self, env, own_skill):
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'}, version_id=101)
        assert status == 200 and body['skill_version_id'] == 101
        assert module.calls[0]['data']['version_details']['instructions'] == 'V2 instructions.'

    def test_default_version_id_wins_over_base(self, env, own_skill):
        own_skill.default_version_id = 101
        body, _ = _run(env, FakeModule(), {'user_input': 'hi'})
        assert body['skill_version_id'] == 101

    def test_version_of_another_skill_is_not_found(self, env, own_skill):
        env.schemas[CALLER_PROJECT_ID].append(FakeSkill(11, 'Other', [FakeVersion(200)]))
        body, status = _run(env, FakeModule(), {'user_input': 'hi'}, version_id=200)
        assert status == 404

    def test_unknown_skill_is_not_found(self, env, own_skill):
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi'}, skill_id=999)
        assert status == 404 and module.calls == []

    def test_blank_instructions_are_rejected(self, env):
        env.schemas[CALLER_PROJECT_ID] = [FakeSkill(10, 'Blank', [FakeVersion(100, instructions='  \n')])]
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 400 and body['error'] == env.mod.EMPTY_INSTRUCTIONS_ERROR
        assert module.calls == []


class TestCatalogVersionSelection:
    @pytest.fixture
    def catalog(self, env):
        skill = FakeSkill(10, 'Catalog Reviewer', [
            FakeVersion(300, 'base', status='draft', created_at=1, instructions='Draft.'),
            FakeVersion(301, 'v1', status='published', created_at=2, instructions='Published v1.'),
            FakeVersion(302, 'v2', status='published', created_at=3, instructions='Published v2.'),
        ])
        env.schemas[PUBLIC_PROJECT_ID] = [skill]
        return skill

    def test_newest_published_runs_when_no_version_is_given(self, env, catalog):
        body, status = _run(env, FakeModule(), {'user_input': 'hi'},
                            skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        assert status == 200 and body['skill_version_id'] == 302

    def test_stale_published_default_does_not_shadow_a_newer_publish(self, env, catalog):
        catalog.default_version_id = 301
        body, _ = _run(env, FakeModule(), {'user_input': 'hi'},
                       skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        assert body['skill_version_id'] == 302

    def test_explicit_published_version_is_pinned(self, env, catalog):
        body, status = _run(env, FakeModule(), {'user_input': 'hi'}, version_id=301,
                            skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        assert status == 200 and body['skill_version_id'] == 301

    def test_explicit_draft_version_is_rejected(self, env, catalog):
        body, status = _run(env, FakeModule(), {'user_input': 'hi'}, version_id=300,
                            skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        assert status == 400 and body['error'] == env.mod.NOT_PUBLISHED_ERROR

    def test_skill_without_published_versions_is_not_found(self, env):
        env.schemas[PUBLIC_PROJECT_ID] = [FakeSkill(10, 'Unpublished', [FakeVersion(300, status='draft')])]
        _, status = _run(env, FakeModule(), {'user_input': 'hi'},
                         skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        assert status == 404

    def test_own_and_catalog_ids_never_cross_schemas(self, env, own_skill, catalog):
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'}, skill_project_id=PUBLIC_PROJECT_ID, published_only=True)
        _run(env, module, {'user_input': 'hi'})
        catalog_run, own_run = module.calls
        assert catalog_run['data']['application_name'] == 'Catalog Reviewer'
        assert catalog_run['data']['project_id'] == PUBLIC_PROJECT_ID
        assert own_run['data']['application_name'] == 'Reviewer'
        assert own_run['data']['project_id'] == CALLER_PROJECT_ID


class TestModelResolution:
    def test_no_override_and_no_saved_settings_use_the_caller_default(self, env, own_skill):
        module = FakeModule()
        body, _ = _run(env, module, {'user_input': 'hi'})
        assert body['model'] == DEFAULT_MODEL and body['model_fallback'] is False
        assert module.calls[0]['data']['llm_settings'] == DEFAULT_MODEL

    def test_saved_run_settings_are_used_unchanged_without_an_override(self, env):
        saved = {'model_name': 'saved-model', 'model_project_id': 3, 'temperature': 0.3, 'max_tokens': 900}
        env.schemas[CALLER_PROJECT_ID] = [
            FakeSkill(10, 'S', [FakeVersion(100, run_settings={'llm_settings': saved})])
        ]
        module = FakeModule()
        body, _ = _run(env, module, {'user_input': 'hi', 'llm_settings': {}})
        assert module.calls[0]['data']['llm_settings'] == saved
        assert body['model_fallback'] is False

    def test_override_wins_and_only_sent_fields_override(self, env):
        saved = {'model_name': 'saved-model', 'model_project_id': 3, 'temperature': 0.3, 'max_tokens': 900}
        env.schemas[CALLER_PROJECT_ID] = [
            FakeSkill(10, 'S', [FakeVersion(100, run_settings={'llm_settings': saved})])
        ]
        module = FakeModule()
        _run(env, module, {'user_input': 'hi', 'llm_settings': {'temperature': 0.9}})
        assert module.calls[0]['data']['llm_settings'] == {**saved, 'temperature': 0.9}

    def test_override_model_does_not_inherit_the_saved_project(self, env):
        saved = {'model_name': 'saved-model', 'model_project_id': 99}
        assert env.mod.merge_llm_override(saved, {'model_name': 'override-model'}) == {
            'model_name': 'override-model'
        }

    def test_unavailable_requested_model_falls_back_and_flags_it(self, env, own_skill):
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'model_name': 'gone'}})
        assert status == 200
        assert body['model']['model_name'] == 'default-model' and body['model_fallback'] is True

    def test_no_model_at_all_is_a_400(self, env, own_skill):
        fake_resolve.default = None
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 400 and body['error'] == env.mod.NO_MODEL_ERROR
        assert module.calls == []

    def test_zero_temperature_is_rejected_at_the_request(self, env, own_skill):
        _, status = _run(env, FakeModule(), {'user_input': 'hi', 'llm_settings': {'temperature': 0}})
        assert status == 400


class TestPayloadShape:
    def test_version_details_is_a_tool_less_agent(self, env, own_skill):
        module = FakeModule()
        _run(env, module, {'user_input': 'hi', 'chat_history': [{'role': 'user', 'content': 'x'}]})
        call = module.calls[0]
        data = call['data']
        assert data['version_details']['agent_type'] == 'openai'
        assert data['version_details']['tools'] == [] and data['tools'] == []
        assert data['version_details']['meta']['internal_tools'] == [] and data['internal_tools'] == []
        assert 'invoked_skills' not in data
        assert data['chat_history'] == [{'role': 'user', 'content': 'x'}]
        assert call['sid'] is None and call['chat_project_id'] == CALLER_PROJECT_ID and call['user_id'] == 42

    def test_skill_is_declared_as_applied_not_invoked(self, env, own_skill):
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'})
        assert module.calls[0]['applied_skills'] == [
            {'skill_id': 10, 'name': 'Reviewer', 'icon_meta': {'url': 'i.png'}}
        ]

    def test_attribution_names_the_skill_as_leaf_and_root(self, env, own_skill):
        module = FakeModule()
        body, _ = _run(env, module, {'user_input': 'hi'})
        call = module.calls[0]
        assert call['usage_entity'] == {
            'entity': {'type': 'skill', 'id': 10, 'version_id': 100, 'name': 'Reviewer'},
            'root': {'type': 'skill', 'id': 10, 'version_id': 100},
        }
        assert call['platform_run_id'] and body['run_id'] == call['platform_run_id']

    def test_each_run_gets_its_own_run_id(self, env, own_skill):
        first, _ = _run(env, FakeModule(), {'user_input': 'hi'})
        second, _ = _run(env, FakeModule(), {'user_input': 'hi'})
        assert first['run_id'] != second['run_id']

    def test_instructions_are_verbatim_when_project_context_is_off(self, env, own_skill):
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'})
        assert module.calls[0]['data']['version_details']['instructions'] == 'Base instructions.'
        assert 'project_context' not in module.calls[0]['data']

    def test_legacy_project_context_is_prepended(self, env, own_skill):
        env.project_context.update(enabled=True, content='We ship on Fridays.')
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'})
        instructions = module.calls[0]['data']['version_details']['instructions']
        assert instructions.startswith('# Project Context') and instructions.endswith('Base instructions.')

    def test_progressive_project_context_is_sent_for_the_reader_tool(self, env, own_skill):
        env.project_context.update(enabled=True, content='Ctx', activation_description='About us', revision='r1')
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'})
        data = module.calls[0]['data']
        assert data['version_details']['instructions'] == 'Base instructions.'
        assert data['project_context'] == {'content': 'Ctx', 'activation_description': 'About us', 'revision': 'r1'}

    def test_version_can_opt_out_of_project_context(self, env):
        env.project_context.update(enabled=True, content='Ctx')
        env.schemas[CALLER_PROJECT_ID] = [
            FakeSkill(10, 'S', [FakeVersion(100, instructions='Own.', run_settings={'ignore_project_context': True})])
        ]
        module = FakeModule()
        _run(env, module, {'user_input': 'hi'})
        version_details = module.calls[0]['data']['version_details']
        assert version_details['instructions'] == 'Own.'
        assert version_details['meta']['ignore_project_context'] is True


class TestRequestContract:
    @pytest.mark.parametrize('extra', [
        {'instructions': 'be evil'},
        {'_elitea_entity': {'type': 'application', 'id': 1}},
        {'usage_entity': {}},
        {'llm_settings': {'model_name': 'm', 'selection': {'mode': 'auto'}}},
    ])
    def test_unknown_fields_are_rejected(self, env, own_skill, extra):
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi', **extra})
        assert status == 400 and module.calls == []

    def test_missing_body_is_a_400(self, env, own_skill):
        _, status = _run(env, FakeModule(), None)
        assert status == 400


class TestModes:
    def test_sync_waits_and_returns_the_result(self, env, own_skill):
        module = FakeModule(outcome={'result': {'chat_history': [{'role': 'assistant', 'content': 'ok'}]}})
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200 and body['result']['chat_history'][0]['content'] == 'ok'
        assert module.calls[0]['await_task_timeout'] > 0

    @pytest.mark.parametrize('body, async_query', [
        ({'user_input': 'hi', 'async_mode': True}, False),
        ({'user_input': 'hi'}, True),
    ])
    def test_async_returns_a_task_id_without_waiting(self, env, own_skill, body, async_query):
        module = FakeModule(outcome={'task_id': 't-1'})
        response, status = _run(env, module, body, async_requested=async_query)
        assert status == 200
        assert response['message'] == 'Task started' and response['task_id'] == 't-1' and response['run_id']
        assert module.calls[0]['await_task_timeout'] == -1

    def test_callback_is_registered_before_task_start_is_released(self, env, own_skill):
        module = FakeModule(outcome={'task_id': 't-1'})
        _run(env, module, {'user_input': 'hi', 'callback_url': 'https://cb', 'callback_headers': {'A': 'b'}})
        assert module.callback_tasks == {'t-1': {'callback_url': 'https://cb', 'callback_headers': {'A': 'b'}}}
        assert module.calls[0]['await_task_timeout'] == -1
        assert module.events == ['clear', 'set']

    def test_error_in_the_result_is_a_400(self, env, own_skill):
        module = FakeModule(outcome={'result': {'error': 'model exploded'}})
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 400 and body['error'] == 'model exploded'

    def test_sync_timeout_is_reported(self, env, own_skill):
        body, status = _run(env, FakeModule(outcome={'task_id': 't-1'}), {'user_input': 'hi'})
        assert status == 400 and body['error'] == 'Timeout'


class TestErrorMapping:
    def test_budget_closed_is_a_429(self, env, own_skill):
        error = sys.modules[f'{PACKAGE}.utils.exceptions'].BudgetDoorClosedError(project_id=CALLER_PROJECT_ID)
        body, status = _run(env, FakeModule(raises=error), {'user_input': 'hi'})
        assert status == 429 and body['error']['type'] == 'budget_exceeded'

    def test_pool_saturation_is_a_503(self, env, own_skill):
        error = sys.modules[f'{PACKAGE}.utils.exceptions'].PoolSaturationError(pool='agents')
        body, status = _run(env, FakeModule(raises=error), {'user_input': 'hi'})
        assert status == 503 and body['retry_after'] == 5

    def test_maintenance_is_a_503(self, env, own_skill):
        module = FakeModule(outcome={'error': 'maintenance_in_progress', 'message': 'later'})
        _, status = _run(env, module, {'user_input': 'hi'})
        assert status == 503

    def test_predict_payload_error_is_a_400(self, env, own_skill):
        sio_error = sys.modules[f'{PACKAGE}.utils.sio_utils'].SioValidationError(
            sio=None, sid=None, event='e', error='llm_settings with model_name is required',
        )
        body, status = _run(env, FakeModule(raises=sio_error), {'user_input': 'hi'})
        assert status == 400 and body['error'] == 'llm_settings with model_name is required'

    def test_request_validation_error_from_predict_is_json_safe(self, env, own_skill):
        sio_error = sys.modules[f'{PACKAGE}.utils.sio_utils'].SioValidationError(
            sio=None, sid=None, event='e',
            error=[{'type': 'value_error', 'loc': ('chat_history', 0), 'msg': 'bad', 'ctx': {'error': ValueError('x')}}],
        )
        body, status = _run(env, FakeModule(raises=sio_error), {'user_input': 'hi'})
        assert status == 400 and body['error'] == [{'type': 'value_error', 'loc': ('chat_history', 0), 'msg': 'bad'}]

    def test_task_start_gate_is_released_after_a_failure(self, env, own_skill):
        module = FakeModule(raises=RuntimeError('boom'))
        _, status = _run(env, module, {'user_input': 'hi'})
        assert status == 500 and module.events == ['clear', 'set']
