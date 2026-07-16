"""Issue #5860 — heal-all llm_settings family normalizer (decide_family_heal).

decide_family_heal is the per-row arm selector shared by the broadened
heal_llm_settings_family_conflicts admin task. It classifies a stored llm_settings dict against
the resolved model's real supports_reasoning (looked up per project via RPC by the caller) and
returns the aligned dict, or None when the row is already aligned / must not be touched.

Mirrors test_llm_settings_family.py: models/pd/llm.py is dep-free, so no stubs are needed.

Run via:
    python tests/run_tests.py unit/models/test_5860_heal_llm_settings_normalizer.py -v
"""
import pytest

from fixtures.helpers import load_module_with_stubs


@pytest.fixture(scope='module')
def llm_module(models_path):
    return load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_llm_settings_llm_5860')


class TestArm1BothSetReasoningModel:
    """#5821 arm — reasoning model carrying a stale temperature alongside an active effort."""

    def test_strips_temperature(self, llm_module):
        healed = llm_module.decide_family_heal(
            {'temperature': 0.6, 'reasoning_effort': 'medium', 'model_name': 'claude-sonnet-4-5'},
            supports_reasoning=True,
        )
        assert healed['temperature'] is None
        assert healed['reasoning_effort'] == 'medium'

    def test_aligned_reasoning_row_is_skipped(self, llm_module):
        # temperature already null + effort set -> nothing to do (idempotency).
        assert llm_module.decide_family_heal(
            {'temperature': None, 'reasoning_effort': 'high', 'model_name': 'claude'},
            supports_reasoning=True,
        ) is None


class TestArm2EffortOnNonReasoningModel:
    """Impossible config — an active effort on a non-reasoning model. Safe to strip."""

    def test_strips_effort_and_sets_default_temperature(self, llm_module):
        healed = llm_module.decide_family_heal(
            {'reasoning_effort': 'medium', 'model_name': 'gpt-4o'},
            supports_reasoning=False,
        )
        assert healed['reasoning_effort'] is None
        assert healed['temperature'] == 0.7

    def test_keeps_existing_temperature(self, llm_module):
        healed = llm_module.decide_family_heal(
            {'temperature': 0.3, 'reasoning_effort': 'high', 'model_name': 'gpt-4o'},
            supports_reasoning=False,
        )
        assert healed['reasoning_effort'] is None
        assert healed['temperature'] == 0.3


class TestArm3NullEffortOnReasoningModel:
    """#5858 arm — reasoning model running thinking-off (bare null effort). Now unconditional:
    the RPC supports_reasoning tells us deterministically this is a reasoning model, and a bare
    null is the stale/unset default, not the deliberate 'none' opt-out."""

    def test_healed_with_temperature(self, llm_module):
        healed = llm_module.decide_family_heal(
            {'temperature': 0.6, 'reasoning_effort': None, 'model_name': 'claude-sonnet-4-5'},
            supports_reasoning=True,
        )
        assert healed['temperature'] is None
        assert healed['reasoning_effort'] == 'medium'

    def test_healed_when_no_temperature(self, llm_module):
        # Pure thinking-off (effort null, temp absent) on a reasoning model is still the defect.
        healed = llm_module.decide_family_heal(
            {'reasoning_effort': None, 'model_name': 'claude-sonnet-4-5'},
            supports_reasoning=True,
        )
        assert healed['reasoning_effort'] == 'medium'
        assert healed['temperature'] is None

    def test_healed_when_effort_key_absent(self, llm_module):
        healed = llm_module.decide_family_heal(
            {'temperature': 0.6, 'model_name': 'claude-sonnet-4-5'},
            supports_reasoning=True,
        )
        assert healed['reasoning_effort'] == 'medium'
        assert healed['temperature'] is None


class TestNeverTouched:
    def test_explicit_none_effort_is_deliberate_thinking_off(self, llm_module):
        # 'none' is an explicit opt-out, not a stale default. Left alone even on a reasoning model.
        assert llm_module.decide_family_heal(
            {'temperature': 0.6, 'reasoning_effort': 'none', 'model_name': 'gpt-5-chat'},
            supports_reasoning=True,
        ) is None

    def test_aligned_non_reasoning_row_with_temperature_skipped(self, llm_module):
        assert llm_module.decide_family_heal(
            {'temperature': 0.7, 'reasoning_effort': None, 'model_name': 'gpt-4o'},
            supports_reasoning=False,
        ) is None

    def test_non_reasoning_null_effort_no_temp_not_touched(self, llm_module):
        # A valid non-reasoning row with no temperature must NOT get a default temperature injected.
        assert llm_module.decide_family_heal(
            {'reasoning_effort': None, 'model_name': 'gpt-4o'},
            supports_reasoning=False,
        ) is None

    def test_non_reasoning_no_effort_key_not_touched(self, llm_module):
        assert llm_module.decide_family_heal(
            {'model_name': 'gpt-4o'},
            supports_reasoning=False,
        ) is None
