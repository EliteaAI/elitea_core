"""Tests for GenerateApplicationDraftResponse validators.

Covers Pydantic field validators for all suggested_* fields plus
GenerateApplicationDraftRequest edit-mode validation.

Run via:
    python tests/run_tests.py unit/models/test_generate_application_draft.py -v
"""
import sys
import pathlib
import importlib.util
import pytest

# ---------------------------------------------------------------------------
# Module loading
# ---------------------------------------------------------------------------

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent


def _load(rel_path: str, name: str):
    path = PLUGIN_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def pd_module():
    _load("models/pd/predict_llm.py", "elitea_core_predict_llm_stub")

    # Patch the relative import so generate_application_draft.py can resolve it
    predict_llm = sys.modules["elitea_core_predict_llm_stub"]
    sys.modules["elitea_core_gad.predict_llm"] = predict_llm

    path = PLUGIN_ROOT / "models/pd/generate_application_draft.py"
    spec = importlib.util.spec_from_file_location(
        "elitea_core_gad", path,
        submodule_search_locations=[]
    )
    mod = importlib.util.module_from_spec(spec)
    # Make the relative import resolve
    sys.modules["elitea_core_gad"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_base(pd_module) -> dict:
    return {
        "name": "My Agent",
        "description": "Does things",
        "instructions": "Be helpful",
    }


def _make_response(pd_module, extra: dict):
    return pd_module.GenerateApplicationDraftResponse.model_validate(
        {**_valid_base(pd_module), **extra}
    )


# ---------------------------------------------------------------------------
# validate_toolkits
# ---------------------------------------------------------------------------

class TestValidateToolkits:
    def test_keeps_valid_toolkit(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [{"id": 1, "type": "github", "name": "gh"}]
        })
        assert len(r.suggested_toolkits) == 1
        assert r.suggested_toolkits[0].type == "github"

    def test_drops_mcp_from_toolkits(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [{"id": 2, "type": "mcp", "name": "my-mcp"}]
        })
        assert r.suggested_toolkits == []

    def test_drops_application_type(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [{"id": 3, "type": "application", "name": "base"}]
        })
        assert r.suggested_toolkits == []

    def test_drops_item_with_no_type(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [{"id": 4, "name": "nameless"}]
        })
        assert r.suggested_toolkits == []

    def test_drops_non_dict_items(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": ["not-a-dict", 42]
        })
        assert r.suggested_toolkits == []

    def test_empty_list(self, pd_module):
        r = _make_response(pd_module, {"suggested_toolkits": []})
        assert r.suggested_toolkits == []

    def test_none_becomes_empty(self, pd_module):
        r = _make_response(pd_module, {"suggested_toolkits": None})
        assert r.suggested_toolkits == []

    def test_mixed_keeps_only_valid(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [
                {"id": 1, "type": "github", "name": "gh"},
                {"id": 2, "type": "mcp", "name": "mcp-server"},
                {"id": 3, "type": "application", "name": "base"},
                {"id": 4, "type": "artifact", "name": "store"},
            ]
        })
        types = [t.type for t in r.suggested_toolkits]
        assert types == ["github", "artifact"]


# ---------------------------------------------------------------------------
# validate_mcp
# ---------------------------------------------------------------------------

class TestValidateMcp:
    def test_keeps_mcp_item(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_mcp": [{"id": 1, "type": "mcp", "name": "server"}]
        })
        assert len(r.suggested_mcp) == 1

    def test_drops_non_mcp(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_mcp": [{"id": 1, "type": "github", "name": "gh"}]
        })
        assert r.suggested_mcp == []

    def test_drops_non_dict(self, pd_module):
        r = _make_response(pd_module, {"suggested_mcp": ["string"]})
        assert r.suggested_mcp == []

    def test_none_becomes_empty(self, pd_module):
        r = _make_response(pd_module, {"suggested_mcp": None})
        assert r.suggested_mcp == []

    def test_mixed_keeps_only_mcp(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_mcp": [
                {"id": 1, "type": "mcp", "name": "mcp-a"},
                {"id": 2, "type": "github", "name": "gh"},
                {"id": 3, "type": "mcp", "name": "mcp-b"},
            ]
        })
        assert len(r.suggested_mcp) == 2


# ---------------------------------------------------------------------------
# validate_agents
# ---------------------------------------------------------------------------

class TestValidateAgents:
    def test_keeps_valid_agent(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 7, "name": "reviewer", "type": "agent"}
            ]
        })
        assert len(r.suggested_agents) == 1
        assert r.suggested_agents[0].application_id == 7
        assert r.suggested_agents[0].type == "agent"

    def test_drops_pipeline_type(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 12, "name": "deploy", "type": "pipeline"}
            ]
        })
        assert r.suggested_agents == []

    def test_drops_missing_application_id(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_agents": [{"name": "no-id", "type": "agent"}]
        })
        assert r.suggested_agents == []

    def test_drops_zero_application_id(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_agents": [{"application_id": 0, "name": "zero", "type": "agent"}]
        })
        assert r.suggested_agents == []

    def test_id_mirrors_application_id(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 5, "name": "a", "type": "agent"}
            ]
        })
        assert r.suggested_agents[0].id == 5

    def test_drops_non_dict(self, pd_module):
        r = _make_response(pd_module, {"suggested_agents": ["bad"]})
        assert r.suggested_agents == []

    def test_none_becomes_empty(self, pd_module):
        r = _make_response(pd_module, {"suggested_agents": None})
        assert r.suggested_agents == []

    def test_type_is_forced_to_agent(self, pd_module):
        # Even if LLM outputs correct type, validator preserves "agent"
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 3, "name": "x", "type": "agent"}
            ]
        })
        assert r.suggested_agents[0].type == "agent"


# ---------------------------------------------------------------------------
# validate_pipelines
# ---------------------------------------------------------------------------

class TestValidatePipelines:
    def test_keeps_valid_pipeline(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_pipelines": [
                {"application_id": 12, "name": "deploy", "type": "pipeline"}
            ]
        })
        assert len(r.suggested_pipelines) == 1
        assert r.suggested_pipelines[0].type == "pipeline"

    def test_drops_agent_type(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_pipelines": [
                {"application_id": 7, "name": "reviewer", "type": "agent"}
            ]
        })
        assert r.suggested_pipelines == []

    def test_drops_missing_application_id(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_pipelines": [{"name": "no-id", "type": "pipeline"}]
        })
        assert r.suggested_pipelines == []

    def test_id_mirrors_application_id(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_pipelines": [
                {"application_id": 99, "name": "p", "type": "pipeline"}
            ]
        })
        assert r.suggested_pipelines[0].id == 99

    def test_none_becomes_empty(self, pd_module):
        r = _make_response(pd_module, {"suggested_pipelines": None})
        assert r.suggested_pipelines == []


# ---------------------------------------------------------------------------
# limit_suggested_skills
# ---------------------------------------------------------------------------

class TestLimitSuggestedSkills:
    def test_keeps_up_to_max(self, pd_module):
        skills = [{"id": i, "name": f"skill-{i}"} for i in range(5)]
        r = _make_response(pd_module, {"suggested_skills": skills})
        assert len(r.suggested_skills) == 5

    def test_truncates_over_max(self, pd_module):
        skills = [{"id": i, "name": f"skill-{i}"} for i in range(10)]
        r = _make_response(pd_module, {"suggested_skills": skills})
        assert len(r.suggested_skills) == pd_module.MAX_SUGGESTED_SKILLS

    def test_none_becomes_empty(self, pd_module):
        r = _make_response(pd_module, {"suggested_skills": None})
        assert r.suggested_skills == []


# ---------------------------------------------------------------------------
# limit_conversation_starters
# ---------------------------------------------------------------------------

class TestLimitConversationStarters:
    def test_keeps_up_to_4(self, pd_module):
        r = _make_response(pd_module, {
            "conversation_starters": ["a", "b", "c", "d"]
        })
        assert len(r.conversation_starters) == 4

    def test_rejects_more_than_4(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            _make_response(pd_module, {
                "conversation_starters": ["a", "b", "c", "d", "e"]
            })

    def test_strips_whitespace(self, pd_module):
        r = _make_response(pd_module, {
            "conversation_starters": ["  hello  ", " world "]
        })
        assert r.conversation_starters == ["hello", "world"]

    def test_drops_empty_strings(self, pd_module):
        r = _make_response(pd_module, {
            "conversation_starters": ["", "  ", "valid"]
        })
        assert r.conversation_starters == ["valid"]

    def test_none_passthrough(self, pd_module):
        r = _make_response(pd_module, {"conversation_starters": None})
        assert r.conversation_starters is None


# ---------------------------------------------------------------------------
# GenerateApplicationDraftRequest — edit mode validation
# ---------------------------------------------------------------------------

class TestEditModeValidation:
    def test_both_ids_accepted(self, pd_module):
        r = pd_module.GenerateApplicationDraftRequest.model_validate({
            "user_description": "improve it",
            "application_id": 1,
            "version_id": 2,
        })
        assert r.is_edit_mode is True

    def test_neither_id_is_create_mode(self, pd_module):
        r = pd_module.GenerateApplicationDraftRequest.model_validate({
            "user_description": "new agent",
        })
        assert r.is_edit_mode is False

    def test_only_application_id_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            pd_module.GenerateApplicationDraftRequest.model_validate({
                "user_description": "edit",
                "application_id": 1,
            })

    def test_only_version_id_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            pd_module.GenerateApplicationDraftRequest.model_validate({
                "user_description": "edit",
                "version_id": 2,
            })


# ---------------------------------------------------------------------------
# Realistic LLM output scenarios
# ---------------------------------------------------------------------------

class TestRealisticLlmOutput:
    def test_llm_puts_pipeline_in_agents_list_with_wrong_type(self, pd_module):
        # LLM hallucinated type="pipeline" for an item in suggested_agents —
        # validate_agents drops it because type != "agent"
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 12, "name": "deploy-pipeline", "type": "pipeline"},
            ],
            "suggested_pipelines": [],
        })
        assert r.suggested_agents == []
        assert r.suggested_pipelines == []

    def test_llm_puts_pipeline_in_agents_with_agent_type(self, pd_module):
        # LLM says type="agent" for a pipeline app — validator cannot detect this
        # without DB; it passes through. Guard is at prompt level.
        r = _make_response(pd_module, {
            "suggested_agents": [
                {"application_id": 12, "name": "deploy-pipeline", "type": "agent"},
            ],
        })
        assert len(r.suggested_agents) == 1
        assert r.suggested_agents[0].type == "agent"

    def test_llm_puts_agent_in_pipelines_list(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_pipelines": [
                {"application_id": 7, "name": "code-reviewer", "type": "pipeline"},
            ],
        })
        # type="pipeline" is accepted by validate_pipelines — it stays as pipeline
        assert len(r.suggested_pipelines) == 1
        assert r.suggested_pipelines[0].type == "pipeline"

    def test_llm_omits_application_id_uses_id_instead(self, pd_module):
        # LLM output {"id": 5, "type": "agent"} — missing application_id, must be dropped
        r = _make_response(pd_module, {
            "suggested_agents": [{"id": 5, "name": "x", "type": "agent"}]
        })
        assert r.suggested_agents == []

    def test_llm_puts_mcp_in_toolkits(self, pd_module):
        r = _make_response(pd_module, {
            "suggested_toolkits": [
                {"id": 1, "type": "github", "name": "gh"},
                {"id": 2, "type": "mcp", "name": "leaked-mcp"},
            ],
            "suggested_mcp": [],
        })
        assert len(r.suggested_toolkits) == 1
        assert r.suggested_toolkits[0].type == "github"
        assert r.suggested_mcp == []

    def test_full_valid_response(self, pd_module):
        r = _make_response(pd_module, {
            "welcome_message": "Hello!",
            "conversation_starters": ["What can you do?"],
            "suggested_toolkits": [{"id": 1, "type": "github", "name": "gh"}],
            "suggested_mcp": [{"id": 2, "type": "mcp", "name": "server"}],
            "suggested_agents": [{"application_id": 7, "name": "reviewer", "type": "agent"}],
            "suggested_pipelines": [{"application_id": 12, "name": "deploy", "type": "pipeline"}],
            "suggested_skills": [{"id": 3, "name": "review-skill"}],
        })
        assert r.suggested_toolkits[0].id == 1
        assert r.suggested_mcp[0].type == "mcp"
        assert r.suggested_agents[0].application_id == 7
        assert r.suggested_pipelines[0].application_id == 12
        assert r.suggested_skills[0].id == 3
