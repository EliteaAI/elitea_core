import importlib.util
import json
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_5955_published_mapping_guards import skill_utils_module  # noqa: E402,F401
from test_5803_skill_twin_catalog_invisibility import (  # noqa: E402
    FakeSession,
    _fake_db,
    _fork_payload,
    _skill_info,
    pu,  # noqa: F401
)
from test_6414_agent_version_validation import (  # noqa: E402,F401
    _BatchSession,
    _ColumnModel,
    _Session,
    bind_session,
)

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

_draft_fixtures_spec = importlib.util.spec_from_file_location(
    'draft_failure_contract_7123', pathlib.Path(__file__).with_name('test_6416_draft_llm_failure_surfacing.py'),
)
_draft_fixtures = importlib.util.module_from_spec(_draft_fixtures_spec)
_draft_fixtures_spec.loader.exec_module(_draft_fixtures)

endpoints = _draft_fixtures.endpoints

DEFAULT_LIMIT = 50000


class _Row:
    def __init__(self, **columns):
        self.__dict__.update(columns)


@pytest.fixture(autouse=True)
def constructible_models(skill_utils_module, monkeypatch):  # noqa: F811
    models_skill = sys.modules['plugins.elitea_core.models.skill']
    monkeypatch.setattr(skill_utils_module, 'Skill', type('Skill', (_Row, models_skill.Skill), {}))
    monkeypatch.setattr(
        skill_utils_module, 'SkillVersion', type('SkillVersion', (_Row, models_skill.SkillVersion), {'name': 'base'}),
    )
    monkeypatch.setattr(skill_utils_module, 'serialize', lambda value: value)
    monkeypatch.setattr(
        skill_utils_module, 'SkillVersionDetailModel', types.SimpleNamespace(model_validate=lambda value: value),
    )


@pytest.fixture
def admin_config(skill_utils_module, monkeypatch):  # noqa: F811
    def _set(config):
        monkeypatch.setattr(
            skill_utils_module, 'this', types.SimpleNamespace(descriptor=types.SimpleNamespace(config=config)),
        )

    _set({})
    return _set


def _version(instructions):
    return types.SimpleNamespace(
        name='base', instructions=instructions, author_id=7, meta=None, run_settings=None, tags=None,
    )


def _create_skill(skill_utils_module, instructions):  # noqa: F811
    session = _Session()
    skill_data = types.SimpleNamespace(
        name='sk', description='d', owner_id=2, meta=None, versions=[_version(instructions)],
    )
    skill_utils_module.create_skill(skill_data, session, 2)
    return session


def _create_version(skill_utils_module, bind_session, instructions, **kwargs):  # noqa: F811
    session = bind_session(_Session(results={skill_utils_module.Skill: types.SimpleNamespace(id=7)}))
    skill_utils_module.create_skill_version(
        project_id=2, skill_id=7, version_data=_version(instructions), **kwargs,
    )
    return session


def _update(skill_utils_module, version, instructions):  # noqa: F811
    update_data = skill_utils_module.SkillVersionUpdateModel(
        name=None, instructions=instructions, meta=None, tags=None,
    )
    skill_utils_module._update_version_fields(_Session(), version, update_data)


class TestConfiguredLimit:
    def test_the_default_is_50000(self, skill_utils_module, admin_config):  # noqa: F811
        assert skill_utils_module.get_max_instructions_length() == DEFAULT_LIMIT

    def test_admin_config_sets_the_limit(self, skill_utils_module, admin_config):  # noqa: F811
        admin_config({'skills': {'max_instructions_length': 1200}})

        assert skill_utils_module.get_max_instructions_length() == 1200

    @pytest.mark.parametrize('bad_value', ['abc', None, 0, -5])
    def test_a_bad_value_falls_back_to_the_default(self, skill_utils_module, admin_config, bad_value):  # noqa: F811
        admin_config({'skills': {'max_instructions_length': bad_value}})

        assert skill_utils_module.get_max_instructions_length() == DEFAULT_LIMIT

    def test_the_admin_schema_offers_it_in_runtime_with_the_same_default(self):
        schema = json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())
        setting = next(field for field in schema['properties'].values() if field.get('path') == 'skills.max_instructions_length')

        assert setting['section'] == 'runtime'
        assert setting['default'] == DEFAULT_LIMIT
        assert setting['requires_restart'] is False


class TestCreate:
    def test_a_skill_at_the_default_limit_saves(self, skill_utils_module, admin_config):  # noqa: F811
        session = _create_skill(skill_utils_module, 'i' * DEFAULT_LIMIT)

        assert [len(row.instructions) for row in session.added if hasattr(row, 'instructions')] == [DEFAULT_LIMIT]

    def test_a_skill_over_the_limit_is_a_400_naming_the_limit_and_length(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError) as exc:
            _create_skill(skill_utils_module, 'i' * 1201)

        assert exc.value.http_status == 400
        assert 'Maximum allowed is 1200 characters.' in str(exc.value)
        assert '1201 characters long' in str(exc.value)

    def test_nothing_is_written_when_the_skill_is_too_long(self, skill_utils_module, admin_config):  # noqa: F811
        admin_config({'skills': {'max_instructions_length': 1200}})
        session = _Session()
        skill_data = types.SimpleNamespace(
            name='sk', description='d', owner_id=2, meta=None, versions=[_version('i' * 1201)],
        )

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError):
            skill_utils_module.create_skill(skill_data, session, 2)

        assert session.added == []

    def test_a_new_version_over_the_limit_is_rejected(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError):
            _create_version(skill_utils_module, bind_session, 'i' * 1201)

    def test_a_new_version_at_the_limit_saves(self, skill_utils_module, bind_session, admin_config):  # noqa: F811
        admin_config({'skills': {'max_instructions_length': 1200}})

        session = _create_version(skill_utils_module, bind_session, 'i' * 1200)

        assert len(session.added) == 1

    def test_a_publish_copy_of_a_saved_version_skips_a_lowered_limit(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})

        session = _create_version(
            skill_utils_module, bind_session, 'i' * 5000, instructions_already_saved=True,
        )

        assert len(session.added[0].instructions) == 5000


class TestUpdate:
    def test_an_edit_over_the_limit_is_rejected_and_keeps_the_saved_body(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})
        version = types.SimpleNamespace(name='base', instructions='short', meta={}, tags=[])

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError):
            _update(skill_utils_module, version, 'i' * 1201)

        assert version.instructions == 'short'

    def test_a_body_saved_before_the_limit_was_lowered_can_be_resent_unchanged(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})
        long_body = 'i' * 5000
        version = types.SimpleNamespace(name='base', instructions=long_body, meta={}, tags=[])

        _update(skill_utils_module, version, long_body)

        assert version.instructions == long_body

    def test_an_edit_to_a_body_saved_before_the_limit_was_lowered_is_validated(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})
        version = types.SimpleNamespace(name='base', instructions='i' * 5000, meta={}, tags=[])

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError):
            _update(skill_utils_module, version, 'i' * 4999)


class TestServerCopiesOfSavedSkills:
    def test_import_skill_passes_the_copy_flag_to_every_version_it_creates(
        self, skill_utils_module, bind_session, admin_config, monkeypatch,  # noqa: F811
    ):
        bind_session(_Session())
        calls = []
        monkeypatch.setattr(
            skill_utils_module, 'create_skill',
            lambda *args, **kwargs: calls.append(kwargs) or types.SimpleNamespace(id=7, versions=[]),
        )
        monkeypatch.setattr(
            skill_utils_module, 'create_skill_version',
            lambda **kwargs: calls.append(kwargs) or {'id': 71},
        )

        skill_utils_module.import_skill(
            project_id=2, name='sk', description='d', author_id=7, force_new=True,
            versions=[{'name': 'base', 'instructions': 'i' * 5000}, {'name': 'v2', 'instructions': 'i' * 5000}],
            instructions_already_saved=True,
        )

        assert [call['instructions_already_saved'] for call in calls] == [True, True]

    def test_import_skill_validates_by_default(
        self, skill_utils_module, bind_session, admin_config, monkeypatch,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})
        bind_session(_Session())
        monkeypatch.setattr(skill_utils_module, 'SkillCreateModel', types.SimpleNamespace(
            model_validate=lambda raw: types.SimpleNamespace(
                name=raw['name'], description=raw['description'], owner_id=2, meta=None,
                versions=[_version(v['instructions']) for v in raw['versions']],
            ),
        ))

        with pytest.raises(skill_utils_module.SkillInstructionsTooLongError):
            skill_utils_module.import_skill(
                project_id=2, name='sk', description='d', author_id=7, force_new=True,
                versions=[{'name': 'base', 'instructions': 'i' * 5000}],
            )

    def test_a_skill_saved_before_the_limit_was_lowered_can_be_created_from_a_copy(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_instructions_length': 1200}})
        session = _Session()
        skill_data = types.SimpleNamespace(
            name='sk', description='d', owner_id=2, meta=None, versions=[_version('i' * 5000)],
        )

        skill_utils_module.create_skill(skill_data, session, 2, instructions_already_saved=True)

        assert [len(row.instructions) for row in session.added if hasattr(row, 'instructions')] == [5000]

    def test_agent_publish_forks_attached_skills_as_saved_copies(self, pu, monkeypatch):  # noqa: F811
        captured = {}

        def import_wizard(entities, project_id, user_id, **kwargs):
            captured.update(kwargs)
            return {'skills': [{'id': 77}]}, {}

        monkeypatch.setattr(pu, 'build_skill_fork_payload', lambda *a, **k: _fork_payload())
        monkeypatch.setattr(pu, 'this', types.SimpleNamespace(module=types.SimpleNamespace(import_wizard=import_wizard)))
        monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[]]), FakeSession([[(501,)]])]))

        pu._resolve_or_fork_skill_twin(2, 1, _skill_info(), 3)

        assert captured == {'skill_instructions_already_saved': True}

    def test_attaching_a_public_skill_forks_it_as_a_saved_copy(
        self, skill_utils_module, monkeypatch,  # noqa: F811
    ):
        captured = {}
        export_import = types.ModuleType('plugins.elitea_core.utils.skill_export_import')
        export_import.build_skill_fork_payload = lambda *args, **kwargs: {'versions': [{'meta': {}}]}
        monkeypatch.setitem(sys.modules, 'plugins.elitea_core.utils.skill_export_import', export_import)
        monkeypatch.setattr(skill_utils_module, 'auth', types.SimpleNamespace(current_user=lambda: {'id': 1}))
        monkeypatch.setattr(skill_utils_module, 'get_public_project_id', lambda: 1)
        for model in ('ApplicationVersion', 'Application', 'SkillVersion', 'EntitySkillMapping'):
            monkeypatch.setattr(skill_utils_module, model, _ColumnModel())
        monkeypatch.setattr(
            skill_utils_module, 'AgentTypes', types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
        )
        monkeypatch.setattr(skill_utils_module, 'attach_skill_to_agent', lambda *args, **kwargs: None)

        def import_wizard(entities, project_id, author_id, **kwargs):
            captured.update(kwargs)
            return {'skills': [{'id': 10}]}, {}

        monkeypatch.setattr(skill_utils_module, 'this', types.SimpleNamespace(
            module=types.SimpleNamespace(import_wizard=import_wizard),
        ))
        session = _BatchSession([[(146,)], None, (20,), [(10,)], []])

        results = skill_utils_module.attach_public_skill_to_agents(
            project_id=2, public_skill_id=99, public_version_id=98, agent_version_ids=[146], session=session,
        )

        assert captured == {'skill_instructions_already_saved': True}
        assert [r['ok'] for r in results] == [True]


class TestRequestModels:
    @pytest.fixture(scope='class')
    def skill_version_models(self):
        sys.path.insert(0, str(PLUGIN_ROOT / 'tests' / 'unit' / 'models'))
        try:
            from test_6410_skill_update_shape_validation import pd_module  # noqa: F401
        finally:
            sys.path.remove(str(PLUGIN_ROOT / 'tests' / 'unit' / 'models'))
        generator = pd_module.__wrapped__()
        yield next(generator).skill_version
        next(generator, None)

    @pytest.mark.parametrize('model_name', ['SkillVersionCreateModel', 'SkillVersionImportModel'])
    def test_create_and_import_bodies_accept_more_than_5000_characters(self, skill_version_models, model_name):
        model = getattr(skill_version_models, model_name).model_validate({'instructions': 'i' * DEFAULT_LIMIT})

        assert len(model.instructions) == DEFAULT_LIMIT

    def test_update_bodies_accept_more_than_5000_characters(self, skill_version_models):
        model = skill_version_models.SkillVersionUpdateModel.model_validate({'instructions': 'i' * DEFAULT_LIMIT})

        assert len(model.instructions) == DEFAULT_LIMIT


def test_a_generated_draft_is_cut_at_the_configured_limit_not_5000(endpoints):
    over_limit = _draft_fixtures.SKILL_INSTRUCTIONS_MAX_LENGTH + 300
    draft = {**_draft_fixtures.SKILL_DRAFT, 'instructions': 'i' * over_limit}

    payload, status, _handler = _draft_fixtures._call(
        endpoints.skill, {'user_description': 'x'}, _draft_fixtures._thinking_result(json.dumps(draft)),
    )

    assert status == 200
    assert len(payload['instructions']) == _draft_fixtures.SKILL_INSTRUCTIONS_MAX_LENGTH
