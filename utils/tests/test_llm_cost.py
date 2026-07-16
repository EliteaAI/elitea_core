"""
Unit tests for elitea_core.utils.llm_cost.estimate_cost.

Tests the new hardcoded _PRICE_DEFAULTS approach — no DB, no mock needed.

Run standalone:
    cd elitea_core/utils/tests && python3 -m pytest test_llm_cost.py -v
"""

import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import llm_cost as m


def test_known_model_basic():
    # gpt-4o: (0.0025, 0.010) per 1K tokens
    cost = m.estimate_cost("gpt-4o", input_tokens=1000, output_tokens=500)
    expected = 1 * 0.0025 + 0.5 * 0.010
    assert abs(cost - expected) < 1e-9, f"got {cost}, expected {expected}"


def test_known_model_zero_output():
    cost = m.estimate_cost("gpt-4o", input_tokens=2000, output_tokens=0)
    assert abs(cost - 2 * 0.0025) < 1e-9, cost


def test_unknown_model_returns_none():
    result = m.estimate_cost("unknown-model-xyz", 100, 50)
    assert result is None, result


def test_empty_model_name():
    assert m.estimate_cost("", 100, 50) is None
    assert m.estimate_cost(None, 100, 50) is None


def test_zero_tokens():
    cost = m.estimate_cost("gpt-4o", input_tokens=0, output_tokens=0)
    assert cost == 0.0, cost


def test_none_tokens_treated_as_zero():
    cost = m.estimate_cost("gpt-4o", input_tokens=None, output_tokens=None)
    assert cost == 0.0, cost


def test_result_rounded_to_8_decimal_places():
    # claude-3-5-sonnet: (0.003, 0.015) per 1K
    # 1500 input * 0.003/1K = 0.0045; 250 output * 0.015/1K = 0.00375; total = 0.00825
    cost = m.estimate_cost("claude-3-5-sonnet", input_tokens=1500, output_tokens=250)
    assert len(str(cost).split('.')[-1]) <= 8, f"too many decimals: {cost}"
    assert abs(cost - 0.00825) < 1e-9, cost


def test_none_pricing_tuple_returns_none():
    """A model with (None, None) pricing must return None, not raise."""
    original = m._PRICE_DEFAULTS.get("gpt-4o")
    m._PRICE_DEFAULTS["broken-model-test"] = (None, None)
    try:
        result = m.estimate_cost("broken-model-test", input_tokens=100, output_tokens=50)
        assert result is None, f"expected None, got {result}"
    finally:
        del m._PRICE_DEFAULTS["broken-model-test"]


def test_partial_none_pricing_returns_none():
    """A model with partial None pricing (only one cost is None) must return None."""
    m._PRICE_DEFAULTS["half-priced-test"] = (0.005, None)
    try:
        result = m.estimate_cost("half-priced-test", input_tokens=100, output_tokens=50)
        assert result is None, f"expected None, got {result}"
    finally:
        del m._PRICE_DEFAULTS["half-priced-test"]


def test_substring_match_fallback():
    """Partial name matching finds a model by substring."""
    # "claude-3-5-sonnet-20241022" should match "claude-3-5-sonnet"
    result = m.estimate_cost("claude-3-5-sonnet-20241022", input_tokens=1000, output_tokens=500)
    assert result is not None
    assert result > 0


def test_no_db_or_cache_globals():
    """The new implementation must not expose _PRICE_CACHE or _CACHE_LOADED."""
    assert not hasattr(m, "_PRICE_CACHE"), "_PRICE_CACHE should not exist in new impl"
    assert not hasattr(m, "_CACHE_LOADED"), "_CACHE_LOADED should not exist in new impl"
