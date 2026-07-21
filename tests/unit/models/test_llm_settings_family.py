"""Issue #5821 — mixing reasoning/non-reasoning llm_settings.

Covers the shared llm_settings_family_conflict predicate and the two LLMSettingsModel
variants (LLMSettingsModel auto-corrects on read, LLMSettingsWriteModel rejects on write).
No stubs needed: models/pd/llm.py has zero pylon/tools dependencies.

Run via:
    python tests/run_tests.py unit/models/test_llm_settings_family.py -v
"""
import pytest
from pydantic import ValidationError

from fixtures.helpers import load_module_with_stubs


@pytest.fixture(scope='module')
def llm_module(models_path):
    return load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_llm_settings_llm')


class TestLlmSettingsFamilyConflict:
    def test_no_conflict_when_reasoning_effort_unset(self, llm_module):
        assert llm_module.llm_settings_family_conflict(0.6, None) is False

    def test_no_conflict_when_temperature_unset(self, llm_module):
        assert llm_module.llm_settings_family_conflict(None, 'medium') is False

    def test_no_conflict_when_reasoning_effort_is_none_string(self, llm_module):
        # gpt-5/gpt-5-chat allow a custom temperature when reasoning_effort='none'
        assert llm_module.llm_settings_family_conflict(0.6, 'none') is False

    def test_conflict_when_both_set(self, llm_module):
        assert llm_module.llm_settings_family_conflict(0.6, 'medium') is True


class TestLLMSettingsModelReadVariant:
    def test_auto_corrects_conflicting_combo(self, llm_module):
        settings = llm_module.LLMSettingsModel(
            temperature=0.6, reasoning_effort='medium', model_name='global.anthropic.claude-sonnet-5'
        )
        assert settings.temperature is None
        assert settings.reasoning_effort == 'medium'

    def test_leaves_non_conflicting_combo_untouched(self, llm_module):
        settings = llm_module.LLMSettingsModel(temperature=0.6, model_name='gpt-4o')
        assert settings.temperature == 0.6
        assert settings.reasoning_effort is None

    def test_leaves_reasoning_effort_none_with_temperature(self, llm_module):
        settings = llm_module.LLMSettingsModel(temperature=0.6, reasoning_effort='none', model_name='gpt-5-chat')
        assert settings.temperature == 0.6
        assert settings.reasoning_effort == 'none'


class TestLLMSettingsWriteModel:
    def test_rejects_conflicting_combo(self, llm_module):
        with pytest.raises(ValidationError):
            llm_module.LLMSettingsWriteModel(
                temperature=0.6, reasoning_effort='medium', model_name='global.anthropic.claude-sonnet-5'
            )

    def test_accepts_temperature_only(self, llm_module):
        settings = llm_module.LLMSettingsWriteModel(temperature=0.6, model_name='gpt-4o')
        assert settings.temperature == 0.6

    def test_accepts_reasoning_effort_only(self, llm_module):
        settings = llm_module.LLMSettingsWriteModel(reasoning_effort='high', model_name='claude')
        assert settings.reasoning_effort == 'high'

    def test_accepts_temperature_with_reasoning_effort_none(self, llm_module):
        settings = llm_module.LLMSettingsWriteModel(temperature=0.6, reasoning_effort='none', model_name='gpt-5-chat')
        assert settings.temperature == 0.6
        assert settings.reasoning_effort == 'none'
