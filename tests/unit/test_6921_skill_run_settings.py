import importlib.util
import pathlib
import sys
import types

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'elitea_core_6921'
PUBLIC_PROJECT_ID = 1
TARGET_PROJECT_ID = 5

AUTO = {
    'mode': 'auto',
    'profile_ref': {'id': 'balanced', 'revision': 1},
    'scope_mode': 'run_locked',
    'reasoning': {'mode': 'auto'},
}


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class FakeRpc:
    available = {}

    def timeout(self, seconds):
        return self

    def configurations_get_available_models(self, project_id, section, include_shared):
        if self.available is None:
            raise TimeoutError('configurations is down')
        return self.available


@pytest.fixture
def env(isolated_sys_modules):
    for name in (PKG, f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils', 'pylon', 'pylon.core'):
        _package(name)
    warnings = []
    _module('pylon.core.tools', log=types.SimpleNamespace(
        warning=lambda *args, **kwargs: warnings.append(args[0] % args[1:]),
    ))
    _module('tools', rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=FakeRpc())))
    FakeRpc.available = {}

    llm = _load('models/pd/llm.py', f'{PKG}.models.pd.llm')
    settings_models = _load('models/pd/skill_run_settings.py', f'{PKG}.models.pd.skill_run_settings')
    settings_utils = _load('utils/skill_run_settings.py', f'{PKG}.utils.skill_run_settings')

    _module(f'{PKG}.models.skill', Skill=object, SkillVersion=object)
    _module(f'{PKG}.models.pd.skill', SkillExportModel=object)
    _module(f'{PKG}.utils.export_import_utils', slugify=lambda value: value)
    _module(f'{PKG}.utils.skill_utils', _skill_session=None, get_skill_details=None, import_skill=None)
    sys.modules.setdefault('sqlalchemy.orm', types.ModuleType('sqlalchemy.orm'))
    if not hasattr(sys.modules['sqlalchemy.orm'], 'selectinload'):
        sys.modules['sqlalchemy.orm'].selectinload = lambda *a, **k: None
    export_import = _load('utils/skill_export_import.py', f'{PKG}.utils.skill_export_import')

    return types.SimpleNamespace(
        llm=llm, models=settings_models, utils=settings_utils, md=export_import, warnings=warnings,
    )


def fixed(name, project_id, **extra):
    return {
        'model_name': name,
        'model_project_id': project_id,
        'selection': {'mode': 'fixed', 'model_ref': {'name': name, 'project_id': project_id}},
        **extra,
    }


class TestWriteValidation:
    @pytest.mark.parametrize('llm_settings', [
        fixed('gpt-4.1', 2, temperature=0.3, max_tokens=4096),
        {'selection': AUTO},
        {'model_name': 'gpt-4.1', 'model_project_id': 2},
    ])
    def test_fixed_auto_and_legacy_models_are_accepted(self, env, llm_settings):
        env.models.SkillRunSettingsWriteModel.model_validate({'llm_settings': llm_settings})

    def test_no_model_is_accepted_and_defaults_to_project_context_on(self, env):
        settings = env.models.SkillRunSettingsWriteModel.model_validate({})
        assert settings.llm_settings is None and settings.ignore_project_context is False

    def test_temperature_with_an_active_reasoning_effort_is_rejected_like_agents(self, env):
        body = {'llm_settings': {'model_name': 'o3', 'temperature': 0.5, 'reasoning_effort': 'high'}}
        with pytest.raises(ValidationError) as skill_error:
            env.models.SkillRunSettingsWriteModel.model_validate(body)
        with pytest.raises(ValidationError) as agent_error:
            env.llm.LLMSettingsWriteModel.model_validate(body['llm_settings'])
        assert skill_error.value.errors()[0]['msg'] == agent_error.value.errors()[0]['msg']

    def test_temperature_with_effort_none_is_accepted(self, env):
        env.models.SkillRunSettingsWriteModel.model_validate(
            {'llm_settings': {'model_name': 'o3', 'temperature': 0.5, 'reasoning_effort': 'none'}}
        )

    def test_fixed_selection_without_model_ref_is_rejected(self, env):
        with pytest.raises(ValidationError):
            env.models.SkillRunSettingsWriteModel.model_validate(
                {'llm_settings': {'model_name': 'gpt-4.1', 'selection': {'mode': 'fixed'}}}
            )

    def test_unknown_keys_are_rejected(self, env):
        with pytest.raises(ValidationError):
            env.models.SkillRunSettingsWriteModel.model_validate({'model_name': 'gpt-4.1'})

    def test_stored_shape_drops_unset_fields(self, env):
        settings = env.models.SkillRunSettingsWriteModel.model_validate(
            {'llm_settings': {'model_name': 'gpt-4.1', 'model_project_id': 2, 'temperature': 0.3}}
        )
        assert env.models.dump_run_settings(settings) == {
            'llm_settings': {'model_name': 'gpt-4.1', 'model_project_id': 2, 'temperature': 0.3},
            'ignore_project_context': False,
        }
        assert env.models.dump_run_settings(None) is None


class TestPortableRunSettings:
    def test_available_binding_is_kept(self, env):
        FakeRpc.available = {(PUBLIC_PROJECT_ID, 'gpt-4.1'): {}}
        raw = {'llm_settings': fixed('gpt-4.1', PUBLIC_PROJECT_ID, temperature=0.3)}
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw) == {**raw, 'ignore_project_context': False}

    def test_model_of_another_project_rebinds_to_the_copy_the_target_can_use(self, env):
        FakeRpc.available = {(TARGET_PROJECT_ID, 'gpt-4.1'): {}}
        raw = {'llm_settings': fixed('gpt-4.1', 2)}
        result = env.utils.portable_run_settings(TARGET_PROJECT_ID, raw)
        assert result['llm_settings'] == fixed('gpt-4.1', TARGET_PROJECT_ID)

    def test_missing_model_is_dropped_and_the_rest_survives(self, env):
        FakeRpc.available = {(TARGET_PROJECT_ID, 'other'): {}}
        raw = {'llm_settings': fixed('private', 2, temperature=0.4, max_tokens=900), 'ignore_project_context': True}
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw) == {
            'llm_settings': {'temperature': 0.4, 'max_tokens': 900},
            'ignore_project_context': True,
        }
        assert any('private' in message for message in env.warnings)

    def test_name_only_model_is_bound_to_the_target_project(self, env):
        FakeRpc.available = {(PUBLIC_PROJECT_ID, 'gpt-4.1'): {}}
        raw = {'llm_settings': {'model_name': 'gpt-4.1'}}
        result = env.utils.portable_run_settings(TARGET_PROJECT_ID, raw)
        assert result['llm_settings'] == {'model_name': 'gpt-4.1', 'model_project_id': PUBLIC_PROJECT_ID}

    def test_auto_selection_is_kept_without_a_lookup(self, env):
        FakeRpc.available = None
        raw = {'llm_settings': {'selection': AUTO}}
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw)['llm_settings'] == {'selection': AUTO}

    def test_invalid_model_settings_are_dropped_but_the_toggle_survives(self, env):
        raw = {'llm_settings': {'temperature': 0.5, 'reasoning_effort': 'high'}, 'ignore_project_context': True}
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw) == {'ignore_project_context': True}

    def test_unreachable_model_registry_keeps_the_settings(self, env):
        FakeRpc.available = None
        raw = {'llm_settings': fixed('gpt-4.1', 2)}
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw)['llm_settings'] == fixed('gpt-4.1', 2)

    @pytest.mark.parametrize('raw', [None, {}])
    def test_absent_settings_stay_absent(self, env, raw):
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw) is None


class TestPublishCheck:
    @pytest.mark.parametrize('run_settings', [
        None,
        {'ignore_project_context': True},
        {'llm_settings': {'temperature': 0.3}},
        {'llm_settings': fixed('gpt-4.1', PUBLIC_PROJECT_ID)},
        {'llm_settings': {'selection': AUTO}},
    ])
    def test_versions_without_a_private_model_publish(self, env, run_settings):
        assert env.utils.unshared_model_issue(run_settings, PUBLIC_PROJECT_ID) is None

    @pytest.mark.parametrize('llm_settings', [
        fixed('private', 2),
        {'model_name': 'private'},
    ])
    def test_a_private_or_unbound_model_blocks_publish(self, env, llm_settings):
        issue = env.utils.unshared_model_issue({'llm_settings': llm_settings}, PUBLIC_PROJECT_ID)
        assert issue == "Model 'private' is not a shared model"


class TestSkillMd:
    def skill(self, run_settings):
        return {
            'name': 'Reviewer',
            'description': 'Reviews text',
            'versions': [{'name': 'base'}],
            'version_details': {'name': 'base', 'instructions': 'Review it.', 'run_settings': run_settings},
        }

    def test_run_settings_round_trip_by_model_name(self, env):
        run_settings = {
            'llm_settings': fixed('gpt-4.1', 2, temperature=0.3, max_tokens=4096),
            'ignore_project_context': True,
        }
        parsed = env.md.parse_skill_md(env.md.skill_to_md(self.skill(run_settings)))
        env.md.validate_skill_frontmatter(parsed['frontmatter'])
        assert parsed['frontmatter']['elitea_run_settings'] == {
            'model_name': 'gpt-4.1', 'temperature': 0.3, 'max_tokens': 4096, 'ignore_project_context': True,
        }
        assert env.md.run_settings_from_frontmatter(parsed['frontmatter']) == {
            'llm_settings': {'model_name': 'gpt-4.1', 'temperature': 0.3, 'max_tokens': 4096},
            'ignore_project_context': True,
        }

    def test_auto_selection_is_exported(self, env):
        markdown = env.md.skill_to_md(self.skill({'llm_settings': {'selection': AUTO}}))
        frontmatter = env.md.parse_skill_md(markdown)['frontmatter']
        assert env.md.run_settings_from_frontmatter(frontmatter)['llm_settings'] == {'selection': AUTO}

    def test_skill_without_run_settings_exports_plain_frontmatter(self, env):
        markdown = env.md.skill_to_md(self.skill(None))
        assert 'elitea_run_settings' not in markdown
        assert env.md.run_settings_from_frontmatter(env.md.parse_skill_md(markdown)['frontmatter']) is None

    @pytest.mark.parametrize('block, message', [
        ('gpt-4.1', 'must be a YAML mapping'),
        ({'model_project_id': 2}, 'Unknown'),
    ])
    def test_malformed_block_is_rejected(self, env, block, message):
        with pytest.raises(ValueError, match=message):
            env.md.validate_skill_frontmatter({'name': 'n', 'description': 'd', 'elitea_run_settings': block})

    @pytest.mark.parametrize('flag, ignored', [('false', False), ('true', True), (False, False)])
    def test_quoted_toggle_is_parsed_not_truthy(self, env, flag, ignored):
        frontmatter = {'elitea_run_settings': {'ignore_project_context': flag}}
        raw = env.md.run_settings_from_frontmatter(frontmatter)
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw)['ignore_project_context'] is ignored

    @pytest.mark.parametrize('frontmatter_block, expected', [
        ({'temperature': 5, 'ignore_project_context': 'true'}, {'ignore_project_context': True}),
        ({'temperature': 5, 'ignore_project_context': True}, {'ignore_project_context': True}),
        ({'model_name': 'gpt-4.1', 'ignore_project_context': None},
         {'llm_settings': {'model_name': 'gpt-4.1', 'model_project_id': PUBLIC_PROJECT_ID}, 'ignore_project_context': False}),
        ({'model_name': 'gpt-4.1', 'ignore_project_context': 'maybe'},
         {'llm_settings': {'model_name': 'gpt-4.1', 'model_project_id': PUBLIC_PROJECT_ID}, 'ignore_project_context': False}),
    ])
    def test_a_bad_half_drops_only_itself(self, env, frontmatter_block, expected):
        FakeRpc.available = {(PUBLIC_PROJECT_ID, 'gpt-4.1'): {}}
        raw = env.md.run_settings_from_frontmatter({'elitea_run_settings': frontmatter_block})
        assert env.utils.portable_run_settings(TARGET_PROJECT_ID, raw) == expected
