"""EL-7325: mapping project rows to draft resources (AC-30a, AC-19, AC-22).

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_resources.py -v
"""
import pathlib
import sys

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

_resources = load('utils.node_draft.resources')
agent_candidate, index_resources, toolkit_candidate, prompt_view = (
    _resources.agent_candidate, _resources.index_resources, _resources.toolkit_candidate, _resources.prompt_view,
)

GITHUB_SCHEMA = {"properties": {"selected_tools": {"args_schemas": {
    "get_issues": {"properties": {"repo": {"type": "string"}, "state": {"type": "string"}}, "required": ["repo"]},
    "create_issue": {"properties": {"title": {"type": "string"}}, "required": ["title"]},
}}}}


def _row(**kw):
    row = {"id": 1, "type": "github", "name": "github_elitea", "description": "Our repo",
           "settings": {"selected_tools": ["get_issues", "create_issue"]}, "meta": {}}
    row.update(kw)
    return row


class TestToolkitCandidate:
    def test_toolkit_with_params(self):
        cand = toolkit_candidate(_row(), GITHUB_SCHEMA, attached_ids={1}, blocked=lambda t: False)
        assert cand["kind"] == "toolkit" and cand["attached"] is True
        assert cand["tools"][0] == {"name": "get_issues", "description": "", "params": {"repo": "str*", "state": "str"}}

    def test_mcp_kind(self):
        assert toolkit_candidate(_row(type="mcp"), {}, set(), lambda t: False)["kind"] == "mcp"
        assert toolkit_candidate(_row(meta={"mcp": True}), {}, set(), lambda t: False)["kind"] == "mcp"

    def test_blocked_type_skipped(self):
        """AC-30a: toolkit types blocked by toolkit_security never reach the model."""
        assert toolkit_candidate(_row(type="shell"), {}, set(), lambda t: t == "shell") is None

    def test_applications_and_nameless_skipped(self):
        assert toolkit_candidate(_row(type="application"), {}, set(), lambda t: False) is None
        assert toolkit_candidate(_row(name=""), {}, set(), lambda t: False) is None

    def test_mcp_params_come_from_the_row(self):
        """A remote MCP keeps its tools' schemas on the toolkit row (settings.available_mcp_tools), not in
        the type schema; without them the draft can't fill required parameters (found in e2e, MCP node)."""
        row = _row(id=17, type="mcp", name="Toolkits", settings={
            "selected_tools": ["get_elitea_core_toolkits"],
            "available_mcp_tools": [{
                "value": "get_elitea_core_toolkits", "description": "List the project toolkits",
                "args_schema": {"properties": {"project_id": {"type": "integer"}, "mcp": {"type": "boolean"}},
                                "required": ["project_id"]},
            }],
        })
        cand = toolkit_candidate(row, {}, set(), lambda t: False)
        assert cand["tools"] == [{"name": "get_elitea_core_toolkits", "description": "List the project toolkits",
                                  "params": {"project_id": "int*", "mcp": "bool"}}]

    def test_tool_without_schema_has_no_params(self):
        cand = toolkit_candidate(_row(settings={"selected_tools": ["other"]}), GITHUB_SCHEMA, set(),
                                 lambda t: False)
        assert cand["tools"] == [{"name": "other", "description": ""}]


class TestAgentCandidate:
    def test_agent_and_pipeline(self):
        agent = agent_candidate({"application_id": 12, "name": "Daily Agent", "description": "d",
                                 "agent_type": "openai", "variables": ["team"]}, attached_ids=set())
        assert agent == {"kind": "agent", "entity_type": "agent", "id": 12, "name": "Daily Agent",
                         "description": "d", "attached": False, "variables": ["team"], "tools": []}
        pipe = agent_candidate({"application_id": 3, "name": "P", "description": "", "agent_type": "pipeline",
                                "variables": []}, attached_ids={3})
        assert pipe["entity_type"] == "pipeline" and pipe["attached"] is True


class TestIndexResources:
    def test_index_shape_for_validation(self):
        tk = toolkit_candidate(_row(), GITHUB_SCHEMA, {1}, lambda t: False)
        mcp = toolkit_candidate(_row(id=3, type="mcp", name="docs", settings={"selected_tools": ["search"]}),
                                {}, set(), lambda t: False)
        agent = agent_candidate({"application_id": 12, "name": "A", "description": "", "agent_type": "x",
                                 "variables": []}, set())
        index = index_resources([tk, mcp, agent])
        assert index["toolkit"]["github_elitea"] == {
            "id": 1, "attached": True, "needs_login": False, "entity_type": "toolkit",
            "tools": {"get_issues": {"repo": True, "state": False}, "create_issue": {"title": True}},
        }
        assert index["mcp"]["docs"]["tools"] == {"search": None}
        assert index["agent"]["A"]["entity_type"] == "agent"

    def test_prompt_view_hides_internal_fields(self):
        tk = toolkit_candidate(_row(), GITHUB_SCHEMA, {1}, lambda t: False)
        view = prompt_view([tk])
        assert view == {"toolkits": [{"name": "github_elitea", "type": "github", "attached": True,
                                      "description": "Our repo", "tools": tk["tools"]}],
                        "mcps": [], "agents": []}
