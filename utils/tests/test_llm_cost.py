"""
Unit tests for elitea_core.utils.llm_cost.estimate_cost.

Run standalone:
    cd elitea_core && python3 -m pytest utils/tests/test_llm_cost.py -v
"""

import sys
import pathlib
import types
import unittest.mock as mock

# Stub pylon.core.tools.log before importing the module under test
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


def setup_function():
    """Pre-load a synthetic pricing cache so DB is never hit."""
    m._PRICE_CACHE.clear()
    m._CACHE_LOADED = True
    # gpt-4o: $5/1M input, $15/1M output -> $0.005/1K, $0.015/1K
    m._PRICE_CACHE["gpt-4o"] = (0.005, 0.015)
    # claude-3-sonnet: $3/1M input, $15/1M output
    m._PRICE_CACHE["claude-3-sonnet"] = (0.003, 0.015)


def test_known_model_basic():
    cost = m.estimate_cost("gpt-4o", input_tokens=1000, output_tokens=500)
    expected = 1 * 0.005 + 0.5 * 0.015
    assert abs(cost - expected) < 1e-9, f"got {cost}, expected {expected}"


def test_known_model_zero_output():
    cost = m.estimate_cost("gpt-4o", input_tokens=2000, output_tokens=0)
    assert abs(cost - 2 * 0.005) < 1e-9, cost


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
    # 1500 input * 0.003/1K = 0.0045; output 250 * 0.015/1K = 0.00375; total = 0.00825
    cost = m.estimate_cost("claude-3-sonnet", input_tokens=1500, output_tokens=250)
    assert len(str(cost).split('.')[-1]) <= 8, f"too many decimals: {cost}"
    assert abs(cost - 0.00825) < 1e-9, cost


def test_invalidate_cache():
    m.invalidate_cache()
    assert not m._CACHE_LOADED
    assert not m._PRICE_CACHE
    # Re-load for subsequent tests
    setup_function()
