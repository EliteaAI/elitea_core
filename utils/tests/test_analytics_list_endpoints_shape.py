"""
Static shape test for modified analytics list endpoints.

Verifies that analytics_agents, analytics_tools, analytics_users
all reference the new token/cost columns.

Run:
    cd elitea_core/utils/tests && python3 -m pytest test_analytics_list_endpoints_shape.py -v
"""

import ast
import pathlib

API_DIR = pathlib.Path(__file__).resolve().parents[2] / "api" / "v2"

ENDPOINTS = [
    "analytics_agents.py",
    "analytics_tools.py",
    "analytics_users.py",
]


def _get_source(filename):
    return (API_DIR / filename).read_text()


class TestAnalyticsAgentsTokenColumns:
    def test_input_tokens_referenced(self):
        src = _get_source("analytics_agents.py")
        assert "input_tokens" in src

    def test_output_tokens_referenced(self):
        src = _get_source("analytics_agents.py")
        assert "output_tokens" in src

    def test_total_tokens_in_response(self):
        src = _get_source("analytics_agents.py")
        assert "total_tokens" in src

    def test_llm_cost_in_response(self):
        src = _get_source("analytics_agents.py")
        assert "llm_cost" in src or "total_cost" in src


class TestAnalyticsToolsTokenColumns:
    def test_input_tokens_referenced(self):
        src = _get_source("analytics_tools.py")
        assert "input_tokens" in src

    def test_output_tokens_referenced(self):
        src = _get_source("analytics_tools.py")
        assert "output_tokens" in src

    def test_total_tokens_in_response(self):
        src = _get_source("analytics_tools.py")
        assert "total_tokens" in src

    def test_llm_cost_intentionally_omitted(self):
        """Tools are not LLM calls; llm_cost is intentionally not tracked per-tool (ADR-0008 P3-T02)."""
        src = _get_source("analytics_tools.py")
        assert "llm_cost" not in src, "llm_cost should not be in tools endpoint (tools are not LLM calls)"


class TestAnalyticsUsersTokenColumns:
    def test_input_tokens_referenced(self):
        src = _get_source("analytics_users.py")
        assert "input_tokens" in src

    def test_output_tokens_referenced(self):
        src = _get_source("analytics_users.py")
        assert "output_tokens" in src

    def test_total_tokens_in_response(self):
        src = _get_source("analytics_users.py")
        assert "total_tokens" in src

    def test_llm_cost_in_response(self):
        src = _get_source("analytics_users.py")
        assert "llm_cost" in src or "total_cost" in src
