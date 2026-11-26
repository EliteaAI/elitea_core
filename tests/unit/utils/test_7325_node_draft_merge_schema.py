"""EL-7325: node draft schema (allowed keys, defaults) and edit-mode patch merge.

Covers AC-23, AC-23a, AC-32 (unknown keys dropped).

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_merge_schema.py -v
"""
import copy
import pathlib
import sys

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

_merge, _schema = load('utils.node_draft.merge'), load('utils.node_draft.schema')
changed_paths, merge_patch = _merge.changed_paths, _merge.merge_patch
SUPPORTED_TYPES, allowed_keys, apply_defaults, defaults, drop_unknown = (
    _schema.SUPPORTED_TYPES, _schema.allowed_keys, _schema.apply_defaults, _schema.defaults, _schema.drop_unknown,
)

LLM_CURRENT = {
    "id": "summarize",
    "type": "llm",
    "input": ["issues"],
    "output": ["summary"],
    "input_mapping": {
        "system": {"type": "fixed", "value": "You are a release analyst. Be brief."},
        "task": {"type": "fstring", "value": "Summarize {issues}."},
    },
    "tool_names": {"github_elitea": ["list_issues"]},
    "structured_output": False,
    "transition": "END",
}


class TestSchema:
    def test_supported_types_are_the_ten_active_ones(self):
        assert set(SUPPORTED_TYPES) == {
            "llm", "agent", "toolkit", "mcp", "code", "router", "decision", "hitl", "state_modifier", "printer",
        }

    @pytest.mark.parametrize("node_type", ["custom", "function", "loop", "tool", "pipeline", "condition"])
    def test_deprecated_types_unsupported(self, node_type):
        assert node_type not in SUPPORTED_TYPES

    @pytest.mark.parametrize("node_type", [
        "llm", "agent", "toolkit", "mcp", "code", "router", "decision", "hitl", "state_modifier", "printer",
    ])
    def test_defaults_never_carry_id_or_type(self, node_type):
        assert "id" not in defaults(node_type) and "type" not in defaults(node_type)

    def test_defaults_mirror_ui_initial_node_data(self):
        # flowEditor.constants.js InitialNodeData (EliteaUI); update both together.
        assert defaults("llm") == {"input": [], "output": [], "transition": "END", "structured_output": False}
        assert defaults("router") == {"default_output": "", "routes": [], "input": [], "condition": ""}
        assert defaults("hitl") == {
            "input": [], "user_message": {"type": "fixed", "value": ""},
            "routes": {"approve": "", "edit": "", "reject": "END"}, "edit_state_key": "",
        }
        assert defaults("code") == {
            "code": {"type": "fixed", "value": ""}, "input": [], "output": [],
            "transition": "END", "structured_output": False,
        }
        assert defaults("printer") == {"transition": "END"}

    def test_defaults_are_copies(self):
        first = defaults("router")
        first["routes"].append("x")
        assert defaults("router")["routes"] == []

    def test_allowed_keys_include_ui_only_fields(self):
        assert {"variables_to_clean", "template", "input", "output"} <= allowed_keys("state_modifier")
        assert {"final_message", "input_mapping", "transition"} <= allowed_keys("printer")
        assert {"edit_state_key", "user_message", "routes"} <= allowed_keys("hitl")

    def test_drop_unknown_returns_dropped_names(self):
        cleaned, dropped = drop_unknown("router", {"condition": "x", "routes": [], "foo": 1, "tool_names": {}})
        assert cleaned == {"condition": "x", "routes": []}
        assert sorted(dropped) == ["foo", "tool_names"]

    def test_drop_unknown_ignores_id_and_type(self):
        cleaned, dropped = drop_unknown("llm", {"id": "x", "type": "code", "transition": "END"})
        assert cleaned == {"transition": "END"}
        assert dropped == []

    def test_apply_defaults_keeps_model_values(self):
        node = apply_defaults("router", {"condition": "{{ 'a' }}", "routes": ["a"]})
        assert node == {"condition": "{{ 'a' }}", "routes": ["a"], "default_output": "", "input": []}


class TestMergePatch:
    def test_only_task_replaced_rest_byte_identical(self):
        """AC-23."""
        patch = {"input_mapping": {"task": {"type": "fstring", "value": "Summarize {issues}. Count them."}}}
        before = copy.deepcopy(LLM_CURRENT)
        merged = merge_patch(LLM_CURRENT, patch)
        assert LLM_CURRENT == before, "current must not be mutated"
        assert merged["input_mapping"]["task"]["value"] == "Summarize {issues}. Count them."
        assert merged["input_mapping"]["system"] == LLM_CURRENT["input_mapping"]["system"]
        for key in ("input", "output", "tool_names", "structured_output", "transition", "id", "type"):
            assert merged[key] == LLM_CURRENT[key]
        assert changed_paths(LLM_CURRENT, merged) == ["input_mapping.task"]

    def test_null_removes_key(self):
        """AC-23a."""
        merged = merge_patch(LLM_CURRENT, {"tool_names": None})
        assert "tool_names" not in merged
        assert changed_paths(LLM_CURRENT, merged) == ["tool_names"]

    def test_list_replaces(self):
        """AC-23a."""
        current = {"id": "r", "type": "router", "routes": ["a", "b", "c"], "condition": "x"}
        merged = merge_patch(current, {"routes": ["d"]})
        assert merged["routes"] == ["d"]
        assert changed_paths(current, merged) == ["routes"]

    def test_nested_null_removes_nested_key(self):
        merged = merge_patch(LLM_CURRENT, {"input_mapping": {"system": None}})
        assert "system" not in merged["input_mapping"]
        assert "task" in merged["input_mapping"]

    def test_patch_cannot_change_id_or_type(self):
        merged = merge_patch(LLM_CURRENT, {"id": "other", "type": "code"})
        assert merged["id"] == "summarize" and merged["type"] == "llm"

    def test_new_key_added(self):
        current = {"id": "s", "type": "state_modifier", "template": "x"}
        merged = merge_patch(current, {"variables_to_clean": ["draft"]})
        assert changed_paths(current, merged) == ["variables_to_clean"]

    def test_value_object_is_one_path(self):
        current = {"id": "p", "type": "printer", "input_mapping": {"printer": {"type": "fixed", "value": "a"}}}
        merged = merge_patch(current, {"input_mapping": {"printer": {"type": "fstring", "value": "{report}"}}})
        assert changed_paths(current, merged) == ["input_mapping.printer"]

    def test_routes_dict_changes_per_key(self):
        current = {"id": "h", "type": "hitl", "routes": {"approve": "", "edit": "", "reject": "END"}}
        merged = merge_patch(current, {"routes": {"approve": "create_issue"}})
        assert merged["routes"] == {"approve": "create_issue", "edit": "", "reject": "END"}
        assert changed_paths(current, merged) == ["routes.approve"]

    def test_no_change_no_paths(self):
        assert changed_paths(LLM_CURRENT, copy.deepcopy(LLM_CURRENT)) == []

    def test_paths_sorted(self):
        merged = merge_patch(LLM_CURRENT, {"transition": "next", "input": ["issues", "messages"]})
        assert changed_paths(LLM_CURRENT, merged) == ["input", "transition"]
