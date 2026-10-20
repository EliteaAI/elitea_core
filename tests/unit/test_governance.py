"""C-1009 (#6754): operator-only enterprise credential governance flag.

The flag lives in the deployment config only. It must default to legacy
behaviour, reject sloppy values at startup, and never be editable from the
admin config editor.
"""

import json
import re
import types
import sys
from pathlib import Path

import pytest
import yaml

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module  # noqa: E402

SECTION = "enterprise_credential_governance"


@pytest.fixture(scope="module")
def governance():
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")
    tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
    )
    return load_utils_module(
        PLUGIN_ROOT / "utils", "governance",
        extra_stubs={"pylon": pylon, "pylon.core": core, "pylon.core.tools": tools},
    )


def _fake_module(config):
    return types.SimpleNamespace(descriptor=types.SimpleNamespace(config=config))


def test_missing_section_is_legacy(governance):
    profile = governance.load_governance_profile({})
    assert (profile.enabled, profile.policy_revision, profile.profile) == (False, None, "legacy")


def test_null_section_is_legacy(governance):
    assert governance.load_governance_profile({SECTION: None}).profile == "legacy"


def test_disabled_is_legacy(governance):
    profile = governance.load_governance_profile(
        {SECTION: {"enabled": False, "policy_revision": None}}
    )
    assert profile.enabled is False
    assert profile.profile == "legacy"


def test_enabled_with_revision_is_governed(governance):
    profile = governance.load_governance_profile(
        {SECTION: {"enabled": True, "policy_revision": "2026-10-01"}}
    )
    assert profile.enabled is True
    assert profile.policy_revision == "2026-10-01"
    assert profile.profile == "governed"


@pytest.mark.parametrize("bad", ["true", "false", "yes", 1, 0, [], None])
def test_non_boolean_enabled_is_rejected(governance, bad):
    with pytest.raises(ValueError, match="enabled"):
        governance.load_governance_profile({SECTION: {"enabled": bad}})


@pytest.mark.parametrize("revision", [None, "", "   ", 5])
def test_enabled_without_usable_revision_is_rejected(governance, revision):
    with pytest.raises(ValueError, match="policy_revision"):
        governance.load_governance_profile(
            {SECTION: {"enabled": True, "policy_revision": revision}}
        )
    with pytest.raises(ValueError, match="policy_revision"):
        governance.load_governance_profile({SECTION: {"enabled": True}})


@pytest.mark.parametrize("section", ["on", 1, ["enabled"]])
def test_non_mapping_section_is_rejected(governance, section):
    with pytest.raises(ValueError, match=SECTION):
        governance.load_governance_profile({SECTION: section})


def test_init_governance_stores_and_returns_profile(governance):
    module = _fake_module(
        {SECTION: {"enabled": True, "policy_revision": "r1"}}
    )
    profile = governance.init_governance(module)
    assert module.governance_profile is profile
    assert profile.profile == "governed" and profile.policy_revision == "r1"
    assert governance.is_governed(module) is True


def test_init_governance_defaults_to_legacy(governance):
    module = _fake_module({})
    governance.init_governance(module)
    assert governance.is_governed(module) is False


def test_init_governance_propagates_misconfiguration(governance):
    with pytest.raises(ValueError):
        governance.init_governance(_fake_module({SECTION: {"enabled": "true"}}))


def test_profile_is_immutable(governance):
    profile = governance.load_governance_profile({})
    with pytest.raises(Exception):
        profile.enabled = True


_ENV_REF = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<default>[^}]*))?\}")


def _shipped_section(env):
    """config.yml as pylon loads it: ``${VAR:default}`` expanded from ``env`` before YAML parsing."""
    text = (PLUGIN_ROOT / "config.yml").read_text()
    expanded = _ENV_REF.sub(lambda m: env.get(m["name"], m["default"] if m["default"] is not None else m[0]), text)
    return yaml.safe_load(expanded)[SECTION]


def test_config_yml_ships_the_section_disabled():
    section = _shipped_section({})

    assert section["enabled"] is False
    assert section["policy_revision"] == ""   # quoted expansion: empty, never a YAML date or number
    assert section["environment"] == ""


def test_config_yml_takes_the_flag_from_the_environment(governance):
    section = _shipped_section({
        "ELITEA_CREDENTIAL_GOVERNANCE_ENABLED": "true",
        "ELITEA_CREDENTIAL_GOVERNANCE_POLICY_REVISION": "2026-10-20",
        "ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT": "local-dev",
    })

    profile = governance.load_governance_profile({SECTION: section})
    assert (profile.enabled, profile.policy_revision, section["environment"]) == (True, "2026-10-20", "local-dev")


def test_config_yml_refuses_a_non_boolean_flag_from_the_environment(governance):
    section = _shipped_section({"ELITEA_CREDENTIAL_GOVERNANCE_ENABLED": "yes"})

    with pytest.raises(ValueError):
        governance.load_governance_profile({SECTION: section})


def test_flag_is_not_declared_in_the_admin_schema():
    # The runtime plugin-config editor can only write properties declared in
    # admin_schema.json, so keeping the key out keeps it operator-only.
    schema_path = PLUGIN_ROOT / "admin_schema.json"
    assert schema_path.exists()
    text = schema_path.read_text()
    assert SECTION not in text
    for prop in json.loads(text)["properties"].values():
        assert not str(prop.get("path", "")).startswith(SECTION)
    assert SECTION not in (PLUGIN_ROOT / "metadata.json").read_text()
