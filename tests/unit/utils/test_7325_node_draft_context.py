"""EL-7325: node draft context builder (pure parts).

Covers AC-36b (named resources beyond the cap), the §5.3 context rules
(state always has input/messages, unattended, resources only for some types, caps, trimming).
Access filtering (AC-30a) is tested with the predicate injected; the real predicate is confirmed in T5.

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_context.py -v
"""
import pathlib
import sys

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

_context = load('utils.node_draft.context')
needs_resources, parse_pipeline, select_resources = (
    _context.needs_resources, _context.parse_pipeline, _context.select_resources,
)
LIMITS = load('models.pd.node_draft').LIMITS
RESOURCE_CAP, DESCRIPTION_LIMIT = LIMITS.resource_cap, LIMITS.description_limit

PIPELINE_YAML = """
entry_point: fetch_issues
state:
  issues:
    type: list
nodes:
  - id: fetch_issues
    type: toolkit
    transition: summarize
  - id: summarize
    type: llm
    transition: END
  - id: route
    type: router
    routes: [summarize, END]
    default_output: summarize
"""


class TestParsePipeline:
    def test_state_always_has_input_and_messages(self):
        parsed = parse_pipeline(PIPELINE_YAML, node_id="summarize")
        names = {s["name"]: s["type"] for s in parsed.state}
        assert names["issues"] == "list"
        assert "input" in names and "messages" in names

    def test_nodes_include_end_and_exclude_nothing(self):
        parsed = parse_pipeline(PIPELINE_YAML, node_id="summarize")
        assert {"fetch_issues", "summarize", "route", "END"} == set(parsed.node_ids)

    def test_incoming_edges(self):
        parsed = parse_pipeline(PIPELINE_YAML, node_id="summarize")
        assert sorted(parsed.incoming) == ["fetch_issues", "route"]

    def test_entry_point(self):
        assert parse_pipeline(PIPELINE_YAML, node_id="summarize").entry_point == "fetch_issues"

    def test_unattended_from_trigger(self):
        """The trigger is stored on the version (pipeline_settings), not in the YAML; the handler passes it."""
        for trigger in ("schedule", "webhook", "Webhook"):
            assert parse_pipeline(PIPELINE_YAML, node_id="summarize", trigger=trigger).unattended is True
        for trigger in (None, "", "chat_message"):
            assert parse_pipeline(PIPELINE_YAML, node_id="summarize", trigger=trigger).unattended is False

    def test_invalid_yaml_raises_value_error(self):
        try:
            parse_pipeline("nodes: [", node_id="x")
        except ValueError:
            return
        raise AssertionError("expected ValueError")


def _toolkit(name, description="", tools=()):
    return {"name": name, "type": "github", "description": description, "tools": list(tools)}


class TestSelectResources:
    def test_types_that_need_resources(self):
        assert needs_resources("llm") and needs_resources("agent")
        assert needs_resources("toolkit") and needs_resources("mcp")
        for node_type in ("code", "router", "decision", "hitl", "state_modifier", "printer"):
            assert not needs_resources(node_type)

    def test_cap_plus_attached(self):
        candidates = [_toolkit(f"tk_{i}", "issues tracker") for i in range(30)]
        picked = select_resources(candidates, requirement="list issues", attached={"tk_29"})
        names = [r["name"] for r in picked]
        assert "tk_29" in names
        assert len([n for n in names if n != "tk_29"]) <= RESOURCE_CAP

    def test_named_resource_always_included(self):
        """AC-36b."""
        candidates = [_toolkit(f"tk_{i}", "github issues") for i in range(30)]
        candidates.append(_toolkit("jira_platform", "unrelated"))
        picked = select_resources(candidates, requirement="use jira_platform to create an issue", attached=set())
        assert "jira_platform" in [r["name"] for r in picked]

    def test_named_by_word_match(self):
        """'use the Jira toolkit' matches jira_platform (word in the name)."""
        candidates = [_toolkit(f"tk_{i}", "github issues") for i in range(30)]
        candidates.append(_toolkit("jira_platform"))
        picked = select_resources(candidates, requirement="Use the Jira toolkit", attached=set())
        assert "jira_platform" in [r["name"] for r in picked]

    def test_relevance_ranking(self):
        candidates = [_toolkit("confluence_docs", "wiki pages")] + [_toolkit(f"x_{i}") for i in range(20)]
        picked = select_resources(candidates, requirement="search wiki pages", attached=set())
        assert picked[0]["name"] == "confluence_docs"

    def test_descriptions_trimmed(self):
        long_tool = {"name": "list_issues", "description": "d" * 500, "params": {"repo": "str*"}}
        picked = select_resources([_toolkit("gh", "x" * 500, [long_tool])], requirement="", attached=set())
        assert len(picked[0]["description"]) <= DESCRIPTION_LIMIT
        assert len(picked[0]["tools"][0]["description"]) <= DESCRIPTION_LIMIT

    def test_inaccessible_dropped(self):
        """AC-30a: the access predicate is applied before ranking."""
        candidates = [_toolkit("mine"), dict(_toolkit("private_other"), _hidden=True)]
        picked = select_resources(
            candidates, requirement="use private_other", attached=set(),
            is_accessible=lambda r: not r.get("_hidden"),
        )
        assert [r["name"] for r in picked] == ["mine"]

    def test_params_only_for_top_three(self):
        tool = {"name": "t", "description": "", "params": {"a": "str*"}}
        candidates = [_toolkit(f"tk_{i}", "issues", [tool]) for i in range(6)]
        picked = select_resources(candidates, requirement="issues", attached=set(), params_for_top=3)
        with_params = [r for r in picked if any("params" in t for t in r["tools"])]
        assert len(with_params) == 3
