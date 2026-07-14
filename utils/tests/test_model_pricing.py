"""
Tests for the hardcoded pricing defaults in llm_cost.

ModelPricing ORM has been removed (dead code — table was never populated).
These tests verify the new _PRICE_DEFAULTS dict-based fallback.

Run:
    cd elitea_core/utils/tests && python3 -m pytest test_model_pricing.py -v
"""

import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import llm_cost as m


class TestPriceDefaults:
    def test_known_model_gpt4o_has_pricing(self):
        pricing = m._PRICE_DEFAULTS.get("gpt-4o")
        assert pricing is not None
        assert len(pricing) == 2
        input_cpm, output_cpm = pricing
        assert input_cpm > 0
        assert output_cpm > 0

    def test_known_model_claude_sonnet_has_pricing(self):
        pricing = m._PRICE_DEFAULTS.get("claude-3-5-sonnet")
        assert pricing is not None
        input_cpm, output_cpm = pricing
        assert input_cpm > 0
        assert output_cpm > 0

    def test_all_entries_have_two_positive_values(self):
        for model, (inp, out) in m._PRICE_DEFAULTS.items():
            assert inp is not None and inp >= 0, f"{model}: bad input price {inp}"
            assert out is not None and out >= 0, f"{model}: bad output price {out}"

    def test_estimate_cost_uses_defaults(self):
        """estimate_cost falls back to _PRICE_DEFAULTS for known models."""
        result = m.estimate_cost("gpt-4o", input_tokens=1000, output_tokens=1000)
        assert result is not None
        assert result > 0

    def test_unknown_model_returns_none(self):
        result = m.estimate_cost("unknown-model-xyz", input_tokens=100, output_tokens=50)
        assert result is None

    def test_zero_tokens_returns_zero(self):
        result = m.estimate_cost("gpt-4o", input_tokens=0, output_tokens=0)
        assert result == 0.0

    def test_none_tokens_treated_as_zero(self):
        result = m.estimate_cost("gpt-4o", input_tokens=None, output_tokens=None)
        assert result == 0.0

    def test_empty_model_name_returns_none(self):
        assert m.estimate_cost("", input_tokens=100, output_tokens=50) is None
        assert m.estimate_cost(None, input_tokens=100, output_tokens=50) is None

    def test_result_rounded_to_8_decimal_places(self):
        result = m.estimate_cost("gpt-4o", input_tokens=1500, output_tokens=250)
        assert result is not None
        assert len(str(result).split('.')[-1]) <= 8

    def test_partial_model_name_match(self):
        """Partial name matching: 'gpt-4o-mini' substring lookup."""
        result = m.estimate_cost("my-gpt-4o-mini-endpoint", input_tokens=1000, output_tokens=1000)
        assert result is not None

    def test_no_db_dependency(self):
        """The new implementation must not import from models.model_pricing."""
        import inspect
        src = inspect.getsource(m)
        assert "model_pricing" not in src, "llm_cost must not import ModelPricing"
        assert "from tools import db" not in src, "llm_cost must not import db"
