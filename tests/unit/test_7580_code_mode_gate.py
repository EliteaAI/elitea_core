"""#7580: Code Mode experimental gate - project whitelist and flag stripping.

Every model may use Code Mode (decision of 2026-11-20): only the enabled flag and the project whitelist strip it.

The helper reads `this.descriptor.config` live, so each test swaps the config in place and
checks the decision code unchanged.
"""
import json
import pathlib
import types

import pytest

from unit._tool_spend_loader import load

cm = load("utils/code_mode_utils.py")

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def config(monkeypatch):
    cfg = {}
    monkeypatch.setattr(cm, "this", types.SimpleNamespace(descriptor=types.SimpleNamespace(config=cfg)))
    return cfg


def gate(cfg, **section):
    cfg["code_mode_experimental"] = section


def test_off_by_default(config):
    assert cm.is_code_mode_allowed(3) is False
    assert cm.strip_code_mode(["code_mode", "swarm"], 3) == ["swarm"]


def test_enabled_with_empty_whitelist_allows_every_project(config):
    gate(config, enabled=True)
    assert cm.is_code_mode_allowed(3) and cm.is_code_mode_allowed(25)


def test_whitelist_limits_projects(config):
    gate(config, enabled=True, whitelist_project_ids=[3, "7", "x"])
    assert cm.get_code_mode_whitelist() == {3, 7}
    assert cm.is_code_mode_allowed(3) and cm.is_code_mode_allowed("7")
    assert not cm.is_code_mode_allowed(25)
    assert not cm.is_code_mode_allowed(None)


def test_live_config_change_applies_without_reload(config):
    assert not cm.is_code_mode_allowed(3)
    gate(config, enabled=True)
    assert cm.is_code_mode_allowed(3)


def test_a_leftover_allowed_models_key_has_no_effect(config):
    # A config saved before the model gate was dropped must not start stripping by model
    gate(config, enabled=True, allowed_models=["opus"])
    assert cm.strip_code_mode(["code_mode"], 3) == ["code_mode"]
    assert not hasattr(cm, "is_code_mode_model_allowed")


def test_strip_keeps_list_identity_when_nothing_to_strip(config):
    tools = ["swarm"]
    assert cm.strip_code_mode(tools, 3) is tools
    assert cm.strip_code_mode(None, 3) is None


def test_strip_from_subagent_version_follows_the_project_gate_only(config):
    gate(config, enabled=True, whitelist_project_ids=[3])
    small = {"meta": {"internal_tools": ["code_mode", "pyodide"]}, "llm_settings": {"model_name": "haiku"}}
    other_project = {"meta": {"internal_tools": ["code_mode", "pyodide"]}}
    cm.strip_code_mode_from_version(small, 3)
    cm.strip_code_mode_from_version(other_project, 25)
    assert small["meta"]["internal_tools"] == ["code_mode", "pyodide"]
    assert other_project["meta"]["internal_tools"] == ["pyodide"]
    cm.strip_code_mode_from_version({}, 3)  # no meta: no error


def test_limitations_include_the_smaller_models_note():
    keys = [item["key"] for item in cm.CODE_MODE_LIMITATIONS]
    assert keys == ["swarm", "sensitive", "indexer", "pipelines", "models"]
    assert "Smaller models" in cm.CODE_MODE_LIMITATIONS[-1]["text"]


def test_admin_schema_fields():
    schema = json.loads((PLUGIN_ROOT / "admin_schema.json").read_text())
    fields = schema.get("properties", schema)
    enabled = fields["is_code_mode_enabled"]
    assert enabled["path"] == "code_mode_experimental.enabled" and enabled["requires_restart"] is False
    projects = fields["code_mode_whitelist_project_ids"]
    assert projects["visible_when"] == {"field": "is_code_mode_enabled", "value": True}
    assert projects["requires_restart"] is False
    assert "code_mode_allowed_models" not in fields


def test_predict_payload_and_subagent_paths_strip():
    # Source-level guard: both payload builders route internal_tools through the gate
    predict = (PLUGIN_ROOT / "utils/predict_utils.py").read_text()
    prefetch = (PLUGIN_ROOT / "utils/subagent_prefetch.py").read_text()
    assert "'internal_tools': strip_code_mode(" in predict
    assert "payload['meta']['internal_tools'], parsed.project_id" in predict
    assert "strip_code_mode_from_version(version_details, project_id)" in prefetch
