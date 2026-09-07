"""Focused compatibility tests for issue #5326 Project Context delivery."""

import importlib.util
import pathlib
import sys

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
prepare_project_context_delivery = load_utils_module(
    PLUGIN_ROOT / "utils", "project_context_utils"
).prepare_project_context_delivery
PROJECT_CONTEXT_MODEL_PATH = PLUGIN_ROOT / "models/pd/project_context.py"
_model_spec = importlib.util.spec_from_file_location("project_context_model_5326", PROJECT_CONTEXT_MODEL_PATH)
_model_module = importlib.util.module_from_spec(_model_spec)
_model_spec.loader.exec_module(_model_module)
ProjectContextUpdate = _model_module.ProjectContextUpdate

# the contract interpolates the caps from the model module, so it has to answer to the name that
# module's relative import resolves to
_context_utils = load_utils_module(
    PLUGIN_ROOT / "utils",
    "generate_project_context_utils",
    extra_stubs={"plugins.elitea_core.models.pd.project_context": _model_module},
)
build_create_project_context_system_prompt = _context_utils.build_create_project_context_system_prompt
build_edit_project_context_system_prompt = _context_utils.build_edit_project_context_system_prompt


def test_activation_description_uses_progressive_disclosure():
    instructions, runtime_context = prepare_project_context_delivery(
        "Base instructions",
        {
            "content": "Use dry humor.",
            "enabled": True,
            "activation_description": "Use for joke generation requests.",
            "revision": "rev-1",
        },
    )

    assert instructions == "Base instructions"
    assert runtime_context == {
        "content": "Use dry humor.",
        "activation_description": "Use for joke generation requests.",
        "revision": "rev-1",
    }


def test_missing_activation_description_preserves_legacy_injection():
    instructions, runtime_context = prepare_project_context_delivery(
        "Base instructions",
        {"content": "Legacy background", "enabled": True},
    )

    assert instructions.startswith("# Project Context\n\nLegacy background")
    assert instructions.endswith("Base instructions")
    assert runtime_context is None


def test_disabled_context_is_not_delivered():
    instructions, runtime_context = prepare_project_context_delivery(
        "Base instructions",
        {
            "content": "Hidden background",
            "enabled": False,
            "activation_description": "Use for hidden requests.",
        },
    )

    assert instructions == "Base instructions"
    assert runtime_context is None


def test_omitted_activation_is_distinguishable_from_explicit_removal():
    legacy_client_update = ProjectContextUpdate(content="Background", enabled=True)
    explicit_removal = ProjectContextUpdate(
        content="Background",
        enabled=True,
        activation_description="   ",
    )

    assert "activation_description" not in legacy_client_update.model_fields_set
    assert "activation_description" in explicit_removal.model_fields_set
    assert explicit_removal.activation_description is None


def test_create_generator_contract_requires_both_fields():
    prompt = build_create_project_context_system_prompt("Existing stored prompt")

    assert "Existing stored prompt" in prompt
    assert '"activation_description"' in prompt
    assert '"project_background"' in prompt
    assert "exactly these keys" in prompt


def test_edit_generator_contract_includes_current_activation_description():
    prompt = build_edit_project_context_system_prompt(
        "Current: {current_config}",
        "Existing background",
        "Use for joke requests.",
    )

    assert '"activation_description": "Use for joke requests."' in prompt
    assert '"project_background": "Existing background"' in prompt
    assert "exactly these keys" in prompt
