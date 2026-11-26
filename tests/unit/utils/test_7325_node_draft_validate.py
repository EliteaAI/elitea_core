"""EL-7325: node draft finalisation (validate, repair references, merge, attachments, new state).

Covers AC-19 (attachments), AC-20 (new state), AC-21 / AC-22 (invalid references removed with warnings),
AC-32 (shape, unknown keys, id/type kept), AC-44 (fstring checks), the 422 `details` shape (AC-33).

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_validate.py -v
"""
import pathlib
import sys

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

_validate, _models = load('utils.node_draft.validate'), load('models.pd.node_draft')
DraftInvalid = _validate.DraftInvalid


def finalize(parsed, target):
    """The response as the API returns it (models dumped to plain JSON)."""
    import types  # pylint: disable=C0415
    return types.SimpleNamespace(**_validate.finalize(parsed, target).model_dump())
DraftTarget, MAX_DETAILS = _models.DraftTarget, _models.LIMITS.max_details


def _target(node_type="llm", mode="build", current=None, base=None, **kw):
    resources = kw.pop("resources", {
        "toolkit": {
            "github_elitea": {"id": 1, "attached": True, "needs_login": False,
                              "tools": {"list_issues": {"repo": True, "state": False}}},
            "jira_platform": {"id": 2, "attached": False, "needs_login": False,
                              "tools": {"create_issue": {"project_key": True, "summary": True}}},
        },
        "mcp": {"docs_mcp": {"id": 3, "attached": False, "needs_login": True, "tools": {"search": {"q": True}}}},
        "agent": {"Daily Issue Summary Agent": {"id": 12, "attached": False, "needs_login": False,
                                                "tools": {}, "entity_type": "agent"}},
    })
    return DraftTarget(
        node_id=kw.pop("node_id", "step"),
        node_type=node_type,
        mode=mode,
        current=current or {"id": "step", "type": node_type},
        base=base,
        state={"input": "str", "messages": "list", "issues": "list"},
        node_ids={"fetch_issues", "summarize", "create_jira_issue", "END", "step"},
        resources=resources,
        **kw,
    )


def _llm_answer(node=None, **extra):
    answer = {
        "summary": "Summarizes issues.",
        "node": node if node is not None else {
            "input": ["issues"], "output": ["summary"],
            "input_mapping": {"system": {"type": "fixed", "value": "You are an analyst."},
                              "task": {"type": "fstring", "value": "Summarize {issues}"}},
            "transition": "END",
        },
        "new_state": [{"name": "summary", "type": "str"}],
        "warnings": [],
    }
    answer.update(extra)
    return answer


class TestShape:
    @pytest.mark.parametrize("parsed", [None, [], "text", {"summary": "x"}, {"node": []}])
    def test_not_a_draft_object_is_invalid(self, parsed):
        with pytest.raises(DraftInvalid) as exc:
            finalize(parsed, _target())
        assert exc.value.details and all({"path", "message"} == set(d) for d in exc.value.details)

    def test_id_and_type_forced_from_request(self):
        result = finalize(_llm_answer(node={**_llm_answer()["node"], "id": "x", "type": "code"}), _target())
        assert result.node["id"] == "step" and result.node["type"] == "llm"

    def test_unknown_keys_dropped_silently(self):
        result = finalize(_llm_answer(node={**_llm_answer()["node"], "foo": 1, "code": {}}), _target())
        assert "foo" not in result.node and "code" not in result.node

    def test_build_fills_defaults(self):
        result = finalize(_llm_answer(node={"input_mapping": {"task": {"type": "fixed", "value": "Hi"}}}),
                          _target())
        assert result.node["structured_output"] is False
        assert result.node["input"] == [] and result.node["transition"] == "END"

    def test_bad_value_object_is_invalid(self):
        node = {**_llm_answer()["node"], "input_mapping": {"task": {"type": "python", "value": "x"}}}
        with pytest.raises(DraftInvalid) as exc:
            finalize(_llm_answer(node=node), _target())
        assert any(d["path"] == "node.input_mapping.task.type" for d in exc.value.details)

    def test_missing_required_is_invalid(self):
        with pytest.raises(DraftInvalid) as exc:
            finalize(_llm_answer(node={"code": {"type": "fixed", "value": ""}}), _target(node_type="code"))
        assert any("code.value" in d["path"] for d in exc.value.details)

    def test_details_capped(self):
        node = {"input_mapping": {f"p{i}": {"type": "bad", "value": 1} for i in range(30)}, "tool": "x",
                "toolkit_name": "github_elitea"}
        with pytest.raises(DraftInvalid) as exc:
            finalize(_llm_answer(node=node), _target(node_type="toolkit"))
        assert len(exc.value.details) <= MAX_DETAILS

    def test_summary_truncated(self):
        result = finalize(_llm_answer(summary="s" * 500), _target())
        assert len(result.summary) <= 200

    def test_model_warnings_kept(self):
        result = finalize(_llm_answer(warnings=["No toolkit can create Jira issues."]), _target())
        assert "No toolkit can create Jira issues." in result.warnings


class TestFstring:
    """AC-44."""

    def _task(self, value, inputs=("issues",)):
        node = {"input": list(inputs), "output": ["summary"], "transition": "END",
                "input_mapping": {"task": {"type": "fstring", "value": value}}}
        return _llm_answer(node=node)

    def test_unescaped_json_braces_invalid(self):
        with pytest.raises(DraftInvalid):
            finalize(self._task('Return {"severity": "high"} for {issues}'), _target())

    def test_escaped_braces_ok(self):
        result = finalize(self._task('Return {{"severity": "high"}} for {issues}'), _target())
        assert result.node["input_mapping"]["task"]["type"] == "fstring"

    def test_placeholder_not_in_input_invalid(self):
        with pytest.raises(DraftInvalid) as exc:
            finalize(self._task("Summarize {report}"), _target())
        assert any("report" in d["message"] for d in exc.value.details)

    def test_printer_reads_any_state_variable(self):
        """A printer has no input list; the runtime formats over the whole state."""
        node = {"input_mapping": {"printer": {"type": "fstring", "value": "Report: {issues}"}}, "transition": "END"}
        finalize(_llm_answer(node=node, new_state=[]), _target(node_type="printer"))

    def test_printer_still_rejects_unescaped_braces(self):
        node = {"input_mapping": {"printer": {"type": "fstring", "value": '{"a": 1} {issues}'}}, "transition": "END"}
        with pytest.raises(DraftInvalid):
            finalize(_llm_answer(node=node, new_state=[]), _target(node_type="printer"))

    def test_fixed_with_braces_ok(self):
        node = {"input": [], "output": ["x"], "transition": "END",
                "input_mapping": {"task": {"type": "fixed", "value": '{"severity": "high"}'}}}
        finalize(_llm_answer(node=node), _target())


class TestReferences:
    """AC-21, AC-22."""

    def test_unknown_transition_cleared_with_warning(self):
        result = finalize(_llm_answer(node={**_llm_answer()["node"], "transition": "notify_team"}), _target())
        assert result.node["transition"] == ""
        assert any("notify_team" in w for w in result.warnings)

    def test_router_invalid_routes_removed(self):
        node = {"condition": "{{ 'summarize' }}", "routes": ["summarize", "ghost"], "default_output": "ghost2",
                "input": []}
        result = finalize(_llm_answer(node=node), _target(node_type="router"))
        assert result.node["routes"] == ["summarize"]
        assert result.node["default_output"] == ""
        assert len([w for w in result.warnings if "ghost" in w]) == 2

    def test_hitl_route_targets_checked(self):
        node = {"user_message": {"type": "fixed", "value": "Approve?"},
                "routes": {"approve": "create_jira_issue", "edit": "nowhere", "reject": "END"},
                "edit_state_key": "issues", "input": []}
        result = finalize(_llm_answer(node=node), _target(node_type="hitl"))
        assert result.node["routes"] == {"approve": "create_jira_issue", "edit": "", "reject": "END"}

    def test_decision_nodes_checked(self):
        node = {"description": "pick", "nodes": ["summarize", "ghost"], "default_output": "END", "input": []}
        result = finalize(_llm_answer(node=node), _target(node_type="decision"))
        assert result.node["nodes"] == ["summarize"]

    def test_unknown_toolkit_removed(self):
        node = {"toolkit_name": "slack_tk", "tool": "post", "input": [], "output": [], "transition": "END"}
        result = finalize(_llm_answer(node=node), _target(node_type="toolkit"))
        assert result.node["toolkit_name"] == "" and result.node["tool"] == ""
        assert any("slack_tk" in w for w in result.warnings)

    def test_tool_not_in_toolkit_removed(self):
        node = {"toolkit_name": "github_elitea", "tool": "delete_repo", "input": [], "output": [],
                "transition": "END"}
        result = finalize(_llm_answer(node=node), _target(node_type="toolkit"))
        assert result.node["toolkit_name"] == "github_elitea" and result.node["tool"] == ""
        assert any("delete_repo" in w for w in result.warnings)

    def test_missing_required_tool_param_is_warning(self):
        node = {"toolkit_name": "jira_platform", "tool": "create_issue", "input": ["summary"], "output": [],
                "transition": "END",
                "input_mapping": {"summary": {"type": "variable", "value": "summary"}}}
        result = finalize(_llm_answer(node=node, new_state=[]), _target(node_type="toolkit"))
        assert any("project_key" in w for w in result.warnings)

    def test_unknown_tool_param_dropped(self):
        node = {"toolkit_name": "github_elitea", "tool": "list_issues", "input": [], "output": [],
                "transition": "END",
                "input_mapping": {"repo": {"type": "fixed", "value": "a/b"},
                                  "colour": {"type": "fixed", "value": "red"}}}
        result = finalize(_llm_answer(node=node), _target(node_type="toolkit"))
        assert "colour" not in result.node["input_mapping"]

    def test_llm_tool_names_filtered(self):
        node = {**_llm_answer()["node"],
                "tool_names": {"github_elitea": ["list_issues", "nope"], "ghost_tk": ["x"]}}
        result = finalize(_llm_answer(node=node), _target())
        assert result.node["tool_names"] == {"github_elitea": ["list_issues"]}
        assert any("ghost_tk" in w for w in result.warnings)
        assert any("nope" in w for w in result.warnings)

    def test_mcp_resource_kind_respected(self):
        """A toolkit name can't be used in an MCP node."""
        node = {"toolkit_name": "github_elitea", "tool": "list_issues", "input": [], "output": [],
                "transition": "END"}
        result = finalize(_llm_answer(node=node), _target(node_type="mcp"))
        assert result.node["toolkit_name"] == ""

    def test_agent_unknown_removed(self):
        node = {"tool": "Ghost Agent", "input": [], "output": [], "transition": "END",
                "input_mapping": {"task": {"type": "fixed", "value": "go"}}}
        result = finalize(_llm_answer(node=node), _target(node_type="agent"))
        assert result.node["tool"] == ""


class TestAttachments:
    """AC-19."""

    def test_unattached_toolkit_listed(self):
        node = {"toolkit_name": "jira_platform", "tool": "create_issue", "input": [], "output": [],
                "transition": "END",
                "input_mapping": {"project_key": {"type": "fixed", "value": "QA"},
                                  "summary": {"type": "fixed", "value": "x"}}}
        result = finalize(_llm_answer(node=node), _target(node_type="toolkit"))
        assert result.attachments == [{"entity_type": "toolkit", "id": 2, "name": "jira_platform",
                                       "needs_login": False}]

    def test_attached_toolkit_not_listed(self):
        node = {**_llm_answer()["node"], "tool_names": {"github_elitea": ["list_issues"]}}
        assert finalize(_llm_answer(node=node), _target()).attachments == []

    def test_mcp_needs_login_flag(self):
        node = {"toolkit_name": "docs_mcp", "tool": "search", "input": [], "output": [], "transition": "END",
                "input_mapping": {"q": {"type": "fixed", "value": "x"}}}
        result = finalize(_llm_answer(node=node), _target(node_type="mcp"))
        assert result.attachments == [{"entity_type": "mcp", "id": 3, "name": "docs_mcp", "needs_login": True}]

    def test_agent_attachment(self):
        node = {"tool": "Daily Issue Summary Agent", "input": [], "output": [], "transition": "END",
                "input_mapping": {"task": {"type": "fixed", "value": "go"}}}
        result = finalize(_llm_answer(node=node), _target(node_type="agent"))
        assert result.attachments[0]["entity_type"] == "agent" and result.attachments[0]["id"] == 12


class TestNewState:
    """AC-20."""

    def test_existing_variable_dropped(self):
        result = finalize(_llm_answer(new_state=[{"name": "issues", "type": "list"},
                                                 {"name": "summary", "type": "str"}]), _target())
        assert result.new_state == [{"name": "summary", "type": "str"}]

    def test_invalid_entries_dropped_with_warning(self):
        result = finalize(_llm_answer(new_state=[{"name": "Bad Name", "type": "str"},
                                                 {"name": "ok", "type": "tuple"},
                                                 {"name": "summary", "type": "str"}]), _target())
        assert result.new_state == [{"name": "summary", "type": "str"}]
        assert len(result.warnings) >= 2

    def test_undeclared_output_auto_added(self):
        result = finalize(_llm_answer(new_state=[]), _target())
        assert {"name": "summary", "type": "str"} in result.new_state
        assert any("summary" in w for w in result.warnings)


class TestEditMode:
    def test_patch_merged_and_changed_paths(self):
        current = {"id": "step", "type": "llm", "input": ["issues"], "output": ["summary"], "transition": "END",
                   "input_mapping": {"system": {"type": "fixed", "value": "Keep me."},
                                     "task": {"type": "fstring", "value": "Summarize {issues}"}}}
        answer = {"summary": "Task counts.", "warnings": [], "new_state": [],
                  "node": {"input_mapping": {"task": {"type": "fstring", "value": "Count {issues}"}}}}
        result = finalize(answer, _target(mode="edit", current=current))
        assert result.node["input_mapping"]["system"]["value"] == "Keep me."
        assert result.changed_paths == ["input_mapping.task"]

    def test_edit_does_not_fill_defaults(self):
        current = {"id": "step", "type": "printer", "input_mapping": {"printer": {"type": "fixed", "value": "a"}}}
        answer = {"summary": "s", "warnings": [], "new_state": [], "node": {"final_message": "bye"}}
        result = finalize(answer, _target(node_type="printer", mode="edit", current=current))
        assert "transition" not in result.node

    def test_refine_merges_over_draft_paths_vs_current(self):
        """AC-18: base is the previous draft; changed_paths compares to the original node."""
        current = {"id": "step", "type": "llm", "transition": "END", "input": [], "output": ["x"],
                   "input_mapping": {"task": {"type": "fixed", "value": "orig"}}}
        draft = {**current, "input_mapping": {"task": {"type": "fixed", "value": "draft"}}}
        answer = {"summary": "s", "warnings": [], "new_state": [], "node": {"transition": "summarize"}}
        result = finalize(answer, _target(mode="edit", current=current, base=draft))
        assert result.node["input_mapping"]["task"]["value"] == "draft"
        assert result.changed_paths == ["input_mapping.task", "transition"]

    def test_build_changed_paths_against_current(self):
        result = finalize(_llm_answer(), _target())
        assert "input_mapping.task" in result.changed_paths
