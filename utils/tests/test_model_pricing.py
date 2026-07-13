"""
Unit tests for llm_cost.estimate_cost edge cases.

Tests: unknown model, zero tokens, None tokens, normal computation,
input-only, and cache invalidation.

Run:
    cd elitea_core/utils/tests && python3 -m pytest test_model_pricing.py -v
"""

import sys
import types
import pathlib
import unittest.mock as mock

# Stub pylon before importing the module under test
_pylon = types.ModuleType("pylon")
_pylon_core = types.ModuleType("pylon.core")
_pylon_tools = types.ModuleType("pylon.core.tools")
_pylon_tools.log = mock.MagicMock()
_pylon.core = _pylon_core
_pylon_core.tools = _pylon_tools
sys.modules.setdefault("pylon", _pylon)
sys.modules.setdefault("pylon.core", _pylon_core)
sys.modules.setdefault("pylon.core.tools", _pylon_tools)
sys.modules.setdefault("pylon.core.tools.log", _pylon_tools.log)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import llm_cost as m

MOCK_PRICING = {
    "gpt-4o": (0.005, 0.015),
    "claude-3-5-sonnet": (0.003, 0.015),
}


def setup_function():
    m._PRICE_CACHE.clear()
    m._CACHE_LOADED = True
    m._PRICE_CACHE.update(MOCK_PRICING)


class TestEstimateCostEdgeCases:
    def setup_method(self):
        m._PRICE_CACHE.clear()
        m._CACHE_LOADED = True
        m._PRICE_CACHE.update(MOCK_PRICING)

    def test_unknown_model_returns_none(self):
        result = m.estimate_cost("unknown-model", input_tokens=100, output_tokens=50)
        assert result is None

    def test_zero_tokens_returns_zero(self):
        result = m.estimate_cost("gpt-4o", input_tokens=0, output_tokens=0)
        assert result == 0.0

    def test_none_tokens_treated_as_zero(self):
        result = m.estimate_cost("gpt-4o", input_tokens=None, output_tokens=None)
        assert result == 0.0

    def test_normal_computation(self):
        # 1000 input tokens * 0.005/1k = 0.005
        # 500 output tokens * 0.015/1k = 0.0075
        # total = 0.0125
        result = m.estimate_cost("gpt-4o", input_tokens=1000, output_tokens=500)
        assert abs(result - 0.0125) < 1e-8

    def test_input_only(self):
        result = m.estimate_cost("gpt-4o", input_tokens=1000, output_tokens=0)
        assert abs(result - 0.005) < 1e-8

    def test_empty_model_name_returns_none(self):
        assert m.estimate_cost("", input_tokens=1000, output_tokens=500) is None
        assert m.estimate_cost(None, input_tokens=1000, output_tokens=500) is None

    def test_invalidate_cache_clears_state(self):
        """invalidate_cache must clear both the loaded flag and the cache dict."""
        assert m._CACHE_LOADED is True
        assert len(m._PRICE_CACHE) > 0
        m.invalidate_cache()
        assert m._CACHE_LOADED is False
        assert len(m._PRICE_CACHE) == 0

    def test_result_rounded_to_8_decimal_places(self):
        """Cost results must not exceed 8 decimal places."""
        # 1500 * 0.003/1K + 250 * 0.015/1K = 0.0045 + 0.00375 = 0.00825
        result = m.estimate_cost("claude-3-5-sonnet", input_tokens=1500, output_tokens=250)
        assert len(str(result).split('.')[-1]) <= 8

    def test_none_pricing_tuple_returns_none(self):
        """If the pricing tuple contains None values, estimate_cost should handle gracefully."""
        m._PRICE_CACHE["broken-model"] = (None, None)
        result = m.estimate_cost("broken-model", input_tokens=1000, output_tokens=500)
        assert result is None
