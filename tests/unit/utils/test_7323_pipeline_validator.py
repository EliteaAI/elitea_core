"""Unit tests for pipeline_validator.py (EL-7323).

Covers validate_pipeline_yaml (V-01 through V-10) and fix_deprecated_end_node.

Run via:
    python tests/run_tests.py unit/utils/test_7323_pipeline_validator.py -v
"""
import pathlib
import sys

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT))

from utils.pipeline_validator import fix_deprecated_end_node, validate_pipeline_yaml


# ---------------------------------------------------------------------------
# Minimal valid pipeline (used across tests)
# ---------------------------------------------------------------------------

_MINIMAL_VALID = """
nodes:
  - id: step1
    type: llm
    transition: END
entry_point: step1
"""

_TWO_STEP = """
nodes:
  - id: step1
    type: llm
    transition: step2
  - id: step2
    type: llm
    transition: END
entry_point: step1
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _error_codes(result):
    return [e.code for e in result.errors]


def _warning_codes(result):
    return [w.code for w in result.warnings]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_minimal_valid_pipeline_passes(self):
        r = validate_pipeline_yaml(_MINIMAL_VALID)
        assert r.valid is True
        assert r.errors == []

    def test_two_step_pipeline_passes(self):
        r = validate_pipeline_yaml(_TWO_STEP)
        assert r.valid is True

    def test_valid_with_agent_node(self):
        yaml = """
nodes:
  - id: run_agent
    type: agent
    name: my_agent
    transition: END
entry_point: run_agent
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is True

    def test_valid_with_code_node(self):
        yaml = """
nodes:
  - id: proc
    type: code
    code: "print('hello')"
    transition: END
entry_point: proc
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is True


# ---------------------------------------------------------------------------
# V-01: missing / empty nodes key
# ---------------------------------------------------------------------------

class TestV01MissingNodes:
    def test_missing_nodes_key_is_warning_not_error(self):
        r = validate_pipeline_yaml("entry_point: step1\n")
        assert r.valid is True
        assert "missing_nodes" in _warning_codes(r)
        assert "missing_nodes" not in _error_codes(r)

    def test_empty_nodes_list_is_warning_not_error(self):
        r = validate_pipeline_yaml("nodes: []\nentry_point: step1\n")
        assert r.valid is True
        assert "missing_nodes" in _warning_codes(r)
        assert "missing_nodes" not in _error_codes(r)

    def test_nodes_is_not_a_list(self):
        r = validate_pipeline_yaml("nodes: scalar\nentry_point: step1\n")
        assert r.valid is False
        assert "missing_nodes" in _error_codes(r)

    def test_invalid_yaml_parse_error(self):
        r = validate_pipeline_yaml("nodes: [{\n  malformed: yaml")
        assert r.valid is False
        assert "yaml_parse_error" in _error_codes(r)

    def test_non_dict_yaml_top_level(self):
        r = validate_pipeline_yaml("- item1\n- item2\n")
        assert r.valid is False
        assert "not_a_dict" in _error_codes(r)


# ---------------------------------------------------------------------------
# V-02: missing / duplicate node ids
# ---------------------------------------------------------------------------

class TestV02NodeIds:
    def test_node_missing_id(self):
        yaml = """
nodes:
  - type: llm
    transition: END
entry_point: ~
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "missing_node_id" in _error_codes(r)

    def test_duplicate_node_id(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: step1
entry_point: step1
"""
        # single node, not a duplicate — but a self-loop; duplicate id check happens when
        # the same id appears more than once
        yaml_dup = """
nodes:
  - id: step1
    type: llm
    transition: END
  - id: step1
    type: llm
    transition: END
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml_dup)
        assert r.valid is False
        assert "duplicate_node_id" in _error_codes(r)


# ---------------------------------------------------------------------------
# V-03: unknown node types
# ---------------------------------------------------------------------------

class TestV03NodeTypes:
    def test_unknown_node_type(self):
        yaml = """
nodes:
  - id: step1
    type: foobar
    transition: END
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "unknown_node_type" in _error_codes(r)

    def test_known_types_all_pass(self):
        known = ["llm", "code", "agent", "toolkit", "mcp", "router", "decision",
                 "state_modifier", "printer", "hitl", "custom"]
        for ntype in known:
            yaml = f"""
nodes:
  - id: n1
    type: {ntype}
    {"name: tool_x" if ntype in ("agent", "toolkit", "mcp") else ""}
    {"code: 'x=1'" if ntype == "code" else ""}
    transition: END
entry_point: n1
"""
            r = validate_pipeline_yaml(yaml)
            codes = _error_codes(r)
            assert "unknown_node_type" not in codes, f"type={ntype} raised unknown_node_type"

    def test_deprecated_end_type_is_warning_not_error(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: end_node
  - id: end_node
    type: END
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert "deprecated_end_node_type" in _warning_codes(r)
        # type: END is deprecated but not an error; the missing-transition error
        # should not appear for the end_node since its type signals termination
        assert "unknown_node_type" not in _error_codes(r)


# ---------------------------------------------------------------------------
# V-04: entrypoint
# ---------------------------------------------------------------------------

class TestV04Entrypoint:
    def test_unknown_entry_point_key(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: END
entry_point: nonexistent
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "unknown_entrypoint" in _error_codes(r)

    def test_implicit_entrypoint_warning_when_no_entry_point(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: END
"""
        r = validate_pipeline_yaml(yaml)
        assert "implicit_entrypoint" in _warning_codes(r)

    def test_legacy_entrypoint_true_on_node(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    entrypoint: true
    transition: END
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is True
        assert "implicit_entrypoint" not in _warning_codes(r)

    def test_multiple_entrypoints_error(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    entrypoint: true
    transition: step2
  - id: step2
    type: llm
    entrypoint: true
    transition: END
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "multiple_entrypoints" in _error_codes(r)


# ---------------------------------------------------------------------------
# V-05: END reachability
# ---------------------------------------------------------------------------

class TestV05EndReachability:
    def test_no_terminal_node(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: step2
  - id: step2
    type: llm
    transition: step1
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "no_end_node" in _error_codes(r)

    def test_empty_pipeline_does_not_raise_no_end_node(self):
        r = validate_pipeline_yaml("nodes: []\n")
        assert "no_end_node" not in _error_codes(r)
        assert r.valid is True

    def test_terminal_unreachable_from_entrypoint(self):
        yaml = """
nodes:
  - id: start
    type: llm
    transition: loop
  - id: loop
    type: llm
    transition: loop
  - id: isolated_end
    type: llm
    transition: END
entry_point: start
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "end_node_unreachable" in _error_codes(r)

    def test_end_reachable_via_router(self):
        yaml = """
nodes:
  - id: start
    type: llm
    transition: router
  - id: router
    type: router
    routes:
      - end_step
      - alt_step
  - id: alt_step
    type: llm
    transition: end_step
  - id: end_step
    type: llm
    transition: END
entry_point: start
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is True


# ---------------------------------------------------------------------------
# V-06: unknown transition targets
# ---------------------------------------------------------------------------

class TestV06TransitionTargets:
    def test_unknown_transition_target(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: ghost_node
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "unknown_transition_target" in _error_codes(r)

    def test_end_sentinel_not_treated_as_node_id(self):
        r = validate_pipeline_yaml(_MINIMAL_VALID)
        assert r.valid is True
        assert "unknown_transition_target" not in _error_codes(r)

    def test_routes_list_with_unknown_target(self):
        yaml = """
nodes:
  - id: router
    type: router
    routes:
      - existing
      - ghost
  - id: existing
    type: llm
    transition: END
entry_point: router
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "unknown_transition_target" in _error_codes(r)


# ---------------------------------------------------------------------------
# V-07: LLM node without model_name
# ---------------------------------------------------------------------------

class TestV07LlmModel:
    def test_llm_without_model_name_is_warning(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    transition: END
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert "llm_no_model" in _warning_codes(r)
        # Warning only — still valid
        assert r.valid is True

    def test_llm_with_model_name_no_warning(self):
        yaml = """
nodes:
  - id: step1
    type: llm
    llm_settings:
      model_name: gpt-4o
    transition: END
entry_point: step1
"""
        r = validate_pipeline_yaml(yaml)
        assert "llm_no_model" not in _warning_codes(r)


# ---------------------------------------------------------------------------
# V-08: agent/toolkit/mcp require name
# ---------------------------------------------------------------------------

class TestV08ToolName:
    @pytest.mark.parametrize("ntype", ["agent", "toolkit", "mcp"])
    def test_missing_name_is_error(self, ntype):
        yaml = f"""
nodes:
  - id: n1
    type: {ntype}
    transition: END
entry_point: n1
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "missing_tool_name" in _error_codes(r)

    @pytest.mark.parametrize("ntype", ["agent", "toolkit", "mcp"])
    def test_tool_name_alias_accepted(self, ntype):
        yaml = f"""
nodes:
  - id: n1
    type: {ntype}
    tool_name: my_tool
    transition: END
entry_point: n1
"""
        r = validate_pipeline_yaml(yaml)
        assert "missing_tool_name" not in _error_codes(r)


# ---------------------------------------------------------------------------
# V-09: code node requires code field
# ---------------------------------------------------------------------------

class TestV09CodeField:
    def test_code_node_without_code_is_error(self):
        yaml = """
nodes:
  - id: proc
    type: code
    transition: END
entry_point: proc
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "missing_code_field" in _error_codes(r)

    def test_code_node_with_empty_string_is_error(self):
        yaml = """
nodes:
  - id: proc
    type: code
    code: ""
    transition: END
entry_point: proc
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "empty_code_field" in _error_codes(r)

    def test_code_node_with_value_dict_empty_is_error(self):
        yaml = """
nodes:
  - id: proc
    type: code
    code:
      value: ""
    transition: END
entry_point: proc
"""
        r = validate_pipeline_yaml(yaml)
        assert r.valid is False
        assert "empty_code_value" in _error_codes(r)

    def test_code_node_with_code_passes(self):
        yaml = """
nodes:
  - id: proc
    type: code
    code: "state['result'] = 42"
    transition: END
entry_point: proc
"""
        r = validate_pipeline_yaml(yaml)
        assert "missing_code_field" not in _error_codes(r)
        assert "empty_code_field" not in _error_codes(r)


# ---------------------------------------------------------------------------
# V-10: router without conditions
# ---------------------------------------------------------------------------

class TestV10RouterConditions:
    def test_router_without_conditions_is_warning(self):
        yaml = """
nodes:
  - id: router
    type: router
    transition: END
entry_point: router
"""
        r = validate_pipeline_yaml(yaml)
        assert "router_no_conditions" in _warning_codes(r)
        assert r.valid is True

    def test_router_with_routes_no_warning(self):
        yaml = """
nodes:
  - id: router
    type: router
    routes:
      - end_step
  - id: end_step
    type: llm
    transition: END
entry_point: router
"""
        r = validate_pipeline_yaml(yaml)
        assert "router_no_conditions" not in _warning_codes(r)


# ---------------------------------------------------------------------------
# fix_deprecated_end_node
# ---------------------------------------------------------------------------

class TestFixDeprecatedEndNode:
    def test_no_end_node_unchanged(self):
        original = _MINIMAL_VALID.strip()
        result = fix_deprecated_end_node(original)
        # No deprecated pattern: function returns a re-serialised version; key content unchanged
        import yaml
        orig_data = yaml.safe_load(original)
        result_data = yaml.safe_load(result)
        assert orig_data == result_data

    def test_rewrites_transition_to_end_sentinel(self):
        yaml_str = """
nodes:
  - id: step1
    type: llm
    transition: end_node
  - id: end_node
    type: END
entry_point: step1
"""
        result = fix_deprecated_end_node(yaml_str)
        import yaml
        data = yaml.safe_load(result)
        step1 = next(n for n in data["nodes"] if n["id"] == "step1")
        assert step1["transition"] == "END"
        # end_node itself must be removed
        ids = [n["id"] for n in data["nodes"]]
        assert "end_node" not in ids

    def test_multiple_paths_to_end_node(self):
        yaml_str = """
nodes:
  - id: a
    type: llm
    transition: b
  - id: b
    type: router
    routes:
      - end_node
      - c
  - id: c
    type: llm
    transition: end_node
  - id: end_node
    type: end
entry_point: a
"""
        result = fix_deprecated_end_node(yaml_str)
        import yaml
        data = yaml.safe_load(result)
        ids = [n["id"] for n in data["nodes"]]
        assert "end_node" not in ids
        b_node = next(n for n in data["nodes"] if n["id"] == "b")
        assert "END" in b_node["routes"]
        c_node = next(n for n in data["nodes"] if n["id"] == "c")
        assert c_node["transition"] == "END"

    def test_invalid_yaml_returns_original(self):
        bad = "nodes: [{\n  bad:"
        assert fix_deprecated_end_node(bad) == bad

    def test_no_end_type_node_returns_equivalent(self):
        yaml_str = _TWO_STEP.strip()
        result = fix_deprecated_end_node(yaml_str)
        import yaml
        assert yaml.safe_load(result) == yaml.safe_load(yaml_str)
