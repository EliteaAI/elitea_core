"""Unit tests for generate_pipeline_draft Pydantic models (EL-7323).

Covers GeneratePipelineDraftRequest validation and GeneratePipelineDraftResponse
field validators inherited from generate_application_draft.

Run via:
    python tests/run_tests.py unit/models/test_7323_generate_pipeline_draft.py -v
"""
import pathlib
import sys
import importlib.util
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT))

# The real plugin sits at plugins.elitea_core.models.pd.generate_pipeline_draft.
# Registering under that 3-level package makes `from ...utils.pipeline_validator`
# resolve correctly (3 dots = up to plugins.elitea_core, then down to utils).
_ROOT = "plugins.elitea_core"
_MODELS_PD = f"{_ROOT}.models.pd"


# ---------------------------------------------------------------------------
# Module loading (mirrors test_generate_application_draft.py pattern)
# ---------------------------------------------------------------------------

def _load(rel_path: str, name: str):
    path = PLUGIN_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ensure_pkg(name: str):
    """Register an empty package stub if not already present."""
    if name not in sys.modules:
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg


@pytest.fixture(scope="module")
def pd_module():
    # Ensure intermediate package stubs so relative imports resolve.
    for pkg in ("plugins", _ROOT, f"{_ROOT}.models", _MODELS_PD, f"{_ROOT}.utils"):
        _ensure_pkg(pkg)

    # 1. predict_llm — no deep relative imports
    _load("models/pd/predict_llm.py", f"{_MODELS_PD}.predict_llm")
    predict_llm = sys.modules[f"{_MODELS_PD}.predict_llm"]

    # 2. generate_application_draft — relative-imports .predict_llm
    _load("models/pd/generate_application_draft.py", f"{_MODELS_PD}.generate_application_draft")

    # 3. pipeline_validator — load the real module so that any other test suite that later
    # loads pipeline_utils under plugins.elitea_core can resolve the relative import
    # `from .pipeline_validator import validate_pipeline_yaml` without hitting a thin stub.
    _load("utils/pipeline_validator.py", f"{_ROOT}.utils.pipeline_validator")
    from utils.pipeline_validator import PipelineValidationResult  # noqa: PLC0415

    # 4. generate_pipeline_draft — imports .predict_llm, .generate_application_draft, ...utils.pipeline_validator
    _load("models/pd/generate_pipeline_draft.py", f"{_MODELS_PD}.generate_pipeline_draft")
    return sys.modules[f"{_MODELS_PD}.generate_pipeline_draft"]


def _valid_response(pd_module, extra=None):
    from utils.pipeline_validator import PipelineValidationResult
    base = {
        "name": "My Pipeline",
        "description": "Does things",
        "pipeline_yaml": "nodes:\n  - id: s1\n    type: llm\n    transition: END\n",
        "validation_result": PipelineValidationResult(valid=True).model_dump(),
    }
    if extra:
        base.update(extra)
    return pd_module.GeneratePipelineDraftResponse.model_validate(base)


# ---------------------------------------------------------------------------
# GeneratePipelineDraftRequest — edit mode validation
# ---------------------------------------------------------------------------

class TestRequestEditMode:
    def test_create_mode_no_ids(self, pd_module):
        r = pd_module.GeneratePipelineDraftRequest.model_validate({
            "user_description": "Build a PR review pipeline",
        })
        assert r.is_edit_mode is False
        assert r.pipeline_id is None
        assert r.version_id is None

    def test_edit_mode_both_ids(self, pd_module):
        r = pd_module.GeneratePipelineDraftRequest.model_validate({
            "user_description": "Add an approval step",
            "pipeline_id": 5,
            "version_id": 12,
        })
        assert r.is_edit_mode is True

    def test_only_pipeline_id_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            pd_module.GeneratePipelineDraftRequest.model_validate({
                "user_description": "edit",
                "pipeline_id": 5,
            })

    def test_only_version_id_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            pd_module.GeneratePipelineDraftRequest.model_validate({
                "user_description": "edit",
                "version_id": 12,
            })

    def test_blank_description_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            pd_module.GeneratePipelineDraftRequest.model_validate({
                "user_description": "   ",
            })


# ---------------------------------------------------------------------------
# GeneratePipelineDraftResponse — name and description lengths
# ---------------------------------------------------------------------------

class TestResponseFields:
    def test_valid_response(self, pd_module):
        r = _valid_response(pd_module)
        assert r.name == "My Pipeline"
        assert r.description == "Does things"

    def test_name_too_long_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            _valid_response(pd_module, {"name": "x" * 33})

    def test_empty_name_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            _valid_response(pd_module, {"name": ""})

    def test_description_too_long_rejected(self, pd_module):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            _valid_response(pd_module, {"description": "x" * 2305})

    def test_empty_description_accepted(self, pd_module):
        r = _valid_response(pd_module, {"description": ""})
        assert r.description == ""


# ---------------------------------------------------------------------------
# Suggested resources inherit ApplicationSuggestion validators
# ---------------------------------------------------------------------------

class TestSuggestedResources:
    def test_suggested_agents_accepted(self, pd_module):
        # GeneratePipelineDraftResponse does not inherit type-filtering validators
        # from GenerateApplicationDraftResponse — all ApplicationSuggestion items pass through.
        r = _valid_response(pd_module, {
            "suggested_agents": [
                {"application_id": 1, "name": "reviewer", "type": "agent"},
                {"application_id": 2, "name": "deployer", "type": "pipeline"},
            ],
        })
        assert len(r.suggested_agents) == 2
        assert r.suggested_agents[0].application_id == 1
        assert r.suggested_agents[1].application_id == 2

    def test_suggested_pipelines_accepted(self, pd_module):
        # Same — no type filtering on this model.
        r = _valid_response(pd_module, {
            "suggested_pipelines": [
                {"application_id": 10, "name": "deploy", "type": "pipeline"},
                {"application_id": 11, "name": "reviewer", "type": "agent"},
            ],
        })
        assert len(r.suggested_pipelines) == 2
        assert r.suggested_pipelines[0].application_id == 10

    def test_suggested_skills_accepted(self, pd_module):
        r = _valid_response(pd_module, {
            "suggested_skills": [{"id": 3, "name": "review-skill"}],
        })
        assert len(r.suggested_skills) == 1
        assert r.suggested_skills[0].id == 3

    def test_lists_default_to_empty_when_omitted(self, pd_module):
        # Fields default to [] when not provided (default_factory=list).
        r = _valid_response(pd_module)
        assert r.suggested_agents == []
        assert r.suggested_pipelines == []
        assert r.suggested_skills == []
        assert r.suggested_toolkits == []
        assert r.suggested_mcp == []

    def test_catalog_project_id_passthrough(self, pd_module):
        """project_id on a suggestion is preserved for catalog items."""
        r = _valid_response(pd_module, {
            "suggested_agents": [
                {"application_id": 7, "name": "cat-agent", "type": "agent", "project_id": 99},
            ],
        })
        assert r.suggested_agents[0].project_id == 99

    def test_no_project_id_defaults_to_none(self, pd_module):
        r = _valid_response(pd_module, {
            "suggested_agents": [
                {"application_id": 7, "name": "agent", "type": "agent"},
            ],
        })
        assert r.suggested_agents[0].project_id is None
