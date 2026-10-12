import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6920_skill_run import (  # noqa: E402  pylint: disable=C0413
    CALLER_PROJECT_ID,
    DEFAULT_MODEL,
    FakeModule,
    FakeSkill,
    FakeVersion,
    _run,
    env,  # noqa: F401  pylint: disable=W0611
    fake_resolve,
)

AUTO = {
    'mode': 'auto',
    'profile_ref': {'id': 'balanced', 'revision': 1},
    'scope_mode': 'run_locked',
    'reasoning': {'mode': 'auto'},
}


def saved_version(llm_settings, **run_settings):
    return FakeVersion(100, run_settings={'llm_settings': llm_settings, **run_settings})


def fixed(name, project_id, **extra):
    return {
        'model_name': name,
        'model_project_id': project_id,
        'selection': {'mode': 'fixed', 'model_ref': {'name': name, 'project_id': project_id}},
        **extra,
    }


@pytest.fixture
def skill_with(env):
    def install(llm_settings, **run_settings):
        env.schemas[CALLER_PROJECT_ID] = [FakeSkill(10, 'S', [saved_version(llm_settings, **run_settings)])]
    return install


class TestSavedModel:
    def test_saved_fixed_selection_runs_unchanged(self, env, skill_with):
        saved = fixed('saved-model', 3, temperature=0.3)
        skill_with(saved)
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200
        assert module.calls[0]['data']['version_details']['llm_settings'] == saved
        assert body['meta']['skill_run']['model_fallback'] is False

    def test_unavailable_fixed_selection_falls_back_to_the_caller_default(self, env, skill_with):
        skill_with(fixed('private-model', 99, max_tokens=900))
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200
        llm_settings = module.calls[0]['data']['version_details']['llm_settings']
        assert llm_settings == {**DEFAULT_MODEL, 'max_tokens': 900}
        assert body['meta']['skill_run']['model_fallback'] is True
        assert body['model_fallback'] is True

    def test_unavailable_model_without_a_default_is_a_400(self, env, skill_with):
        skill_with(fixed('private-model', 99))
        fake_resolve.default = None
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 400 and body['error'] == env.mod.NO_MODEL_ERROR
        assert module.calls == []


class TestAutoSelection:
    def test_auto_runs_when_the_caller_project_enables_it(self, env, skill_with):
        skill_with({'selection': AUTO})
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {'selection': AUTO}
        assert body['meta']['skill_run']['model_fallback'] is False

    def test_auto_disabled_in_the_caller_project_falls_back(self, env, skill_with):
        env.routing['enabled'] = False
        skill_with({'selection': AUTO, 'max_tokens': 500})
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi'})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {**DEFAULT_MODEL, 'max_tokens': 500}
        assert body['meta']['skill_run']['model_fallback'] is True


class TestOverride:
    def test_override_model_replaces_a_saved_fixed_selection(self, env, skill_with):
        skill_with(fixed('saved-model', 3, temperature=0.3))
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'model_name': 'override-model'}})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {
            'model_name': 'override-model', 'model_project_id': 3, 'temperature': 0.3,
        }
        assert body['model_fallback'] is False

    def test_override_effort_replaces_a_fixed_selection_effort(self, env, skill_with):
        saved = fixed('saved-model', 3, reasoning_effort='low')
        saved['selection']['reasoning'] = {'mode': 'explicit', 'preset': 'low'}
        skill_with(saved)
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': 'high'}})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {
            'model_name': 'saved-model', 'model_project_id': 3, 'reasoning_effort': 'high',
        }

    @pytest.fixture
    def validating_resolve(self, env, monkeypatch):
        llm = sys.modules['plugins.elitea_core.models.pd.llm']

        def resolve(project_id, llm_settings, **kwargs):
            llm.LLMSettingsModel.model_validate(llm_settings)
            return fake_resolve(project_id, llm_settings)

        monkeypatch.setattr(env.mod, 'validate_and_resolve_llm_settings', resolve)

    def test_override_effort_on_auto_becomes_the_routing_preset(self, env, skill_with, validating_resolve):
        skill_with({'selection': AUTO})
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': 'high'}})
        assert status == 200 and body['model_fallback'] is False
        assert module.calls[0]['data']['llm_settings'] == {
            'selection': {**AUTO, 'reasoning': {'mode': 'explicit', 'preset': 'high'}},
        }

    def test_override_effort_replaces_an_explicit_auto_preset(self, env, skill_with, validating_resolve):
        skill_with({'selection': {**AUTO, 'reasoning': {'mode': 'explicit', 'preset': 'low'}}, 'reasoning_effort': 'low'})
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': 'high'}})
        assert status == 200
        assert module.calls[0]['data']['llm_settings']['selection']['reasoning'] == {'mode': 'explicit', 'preset': 'high'}

    def test_null_effort_override_returns_auto_to_automatic_reasoning(self, env, skill_with, validating_resolve):
        skill_with({'selection': {**AUTO, 'reasoning': {'mode': 'explicit', 'preset': 'low'}}, 'reasoning_effort': 'low'})
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': None}})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {'selection': AUTO}

    def test_effort_none_on_auto_is_rejected_clearly(self, env, skill_with, validating_resolve):
        skill_with({'selection': AUTO})
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': 'none'}})
        assert status == 400 and body['error'] == env.mod.AUTO_EFFORT_NONE_ERROR
        assert module.calls == []

    @pytest.mark.parametrize('effort', ['high', 'none'])
    def test_override_model_on_auto_keeps_the_effort(self, env, skill_with, validating_resolve, effort):
        skill_with({'selection': AUTO})
        module = FakeModule()
        _, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'model_name': 'override-model', 'reasoning_effort': effort}})
        assert status == 200
        assert module.calls[0]['data']['llm_settings'] == {
            'model_name': 'override-model', 'model_project_id': 3, 'reasoning_effort': effort,
        }

    @pytest.mark.parametrize('effort', ['high', 'none'])
    def test_auto_fallback_keeps_the_effort_for_the_default_model(self, env, skill_with, validating_resolve, effort):
        env.routing['enabled'] = False
        skill_with({'selection': AUTO})
        module = FakeModule()
        body, status = _run(env, module, {'user_input': 'hi', 'llm_settings': {'reasoning_effort': effort}})
        assert status == 200 and body['model_fallback'] is True
        assert module.calls[0]['data']['llm_settings'] == {**DEFAULT_MODEL, 'reasoning_effort': effort}
