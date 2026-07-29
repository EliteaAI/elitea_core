"""
Static shape test for analytics_costs endpoint.

Verifies that the response dict in analytics_costs.py contains the expected
top-level keys and that all new column references are syntactically valid.

Run:
    cd elitea_core/utils/tests && python3 -m pytest test_analytics_costs_shape.py -v
"""

import ast
import pathlib

ENDPOINT_PATH = pathlib.Path(__file__).resolve().parents[2] / "api" / "v2" / "analytics_costs.py"


def _get_source():
    return ENDPOINT_PATH.read_text()


def test_file_parses():
    ast.parse(_get_source())


def test_required_response_keys_present():
    src = _get_source()
    for key in ("kpis", "by_model", "by_agent", "by_user", "daily"):
        assert f'"{key}"' in src or f"'{key}'" in src, f"Missing key: {key}"


def test_kpi_sub_keys_present():
    src = _get_source()
    for key in ("total_cost", "total_tokens", "total_input_tokens", "total_output_tokens", "avg_cost_per_call"):
        assert key in src, f"Missing KPI sub-key: {key}"


def test_model_fields_present():
    src = _get_source()
    for key in ("model_name", "display_name", "calls", "input_tokens", "output_tokens"):
        assert key in src, f"Missing model field: {key}"


def test_cost_column_references():
    src = _get_source()
    assert "AuditEvent.llm_cost" in src, "Missing llm_cost column reference"
    assert "AuditEvent.input_tokens" in src, "Missing input_tokens column reference"
    assert "AuditEvent.output_tokens" in src, "Missing output_tokens column reference"


def test_api_class_defined():
    src = _get_source()
    tree = ast.parse(src)
    class_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
    assert "API" in class_names, "API class not defined"


def test_prompt_lib_api_handler_defined():
    src = _get_source()
    tree = ast.parse(src)
    class_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}
    assert "PromptLibAPI" in class_names, "PromptLibAPI handler not defined"
