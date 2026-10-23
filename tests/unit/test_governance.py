"""C-1009 (#6754, #7120): the governance mode helpers and the shipped config.

The mode itself (off, audit_only, enforce) is stored in the database and resolved by ``utils/governance_settings.py``
(see ``test_7120_governance_settings.py``). Here: the readers every caller uses, the profile, and what ``config.yml`` and
the admin schema still say about governance.
"""

import json
import sys
import types
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


def _module(mode=None, unavailable=False, registered=None, enabled=False):
    module = types.SimpleNamespace(governance_profile=types.SimpleNamespace(enabled=enabled))
    if mode is not None:
        module.governance_effective = types.SimpleNamespace(mode=mode, unavailable=unavailable)
    if registered is not None:
        module.governed_handlers_registered = registered
    return module


def test_the_modes_are_ordered_off_audit_only_enforce(governance):
    assert governance.MODES == ("off", "audit_only", "enforce")
    assert governance.MODE_RANK["off"] < governance.MODE_RANK["audit_only"] < governance.MODE_RANK["enforce"]


def test_only_enforce_is_governed_and_only_audit_only_is_auditing(governance):
    assert governance.is_governed(_module(enabled=True)) is True
    assert governance.is_governed(_module(enabled=False)) is False
    assert governance.is_auditing(_module("audit_only")) is True
    assert governance.is_auditing(_module("enforce")) is False
    assert governance.is_auditing(_module("off")) is False
    assert governance.is_auditing(_module()) is False   # a module with no switch at all (older fakes)


def test_unavailable_is_read_from_the_effective_state_and_defaults_to_available(governance):
    assert governance.is_unavailable(_module("enforce", unavailable=True)) is True
    assert governance.is_unavailable(_module("enforce")) is False
    assert governance.is_unavailable(_module()) is False


def test_lifecycle_work_continues_after_enforce_was_left(governance):
    assert governance.governance_lifecycle_active(_module(enabled=True)) is True
    assert governance.governance_lifecycle_active(_module(registered=True)) is True   # enforce earlier in this process
    assert governance.governance_lifecycle_active(_module(registered=False)) is False
    assert governance.governance_lifecycle_active(_module()) is False


def test_profile_is_immutable(governance):
    profile = governance.GovernanceProfile(False, None, governance.PROFILE_LEGACY)
    with pytest.raises(Exception):
        profile.enabled = True


def test_config_yml_keeps_only_the_context_ttl_in_the_section():
    section = yaml.safe_load((PLUGIN_ROOT / "config.yml").read_text())[SECTION]
    assert section == {"context_ttl_s": 3600}   # the switch is in the database; the deployment keys are environment


def test_config_yml_does_not_expand_any_governance_variable():
    text = (PLUGIN_ROOT / "config.yml").read_text()
    assert "${ELITEA_CREDENTIAL_GOVERNANCE" not in text   # a pylon.db copy of the config can shadow the file (C9-SEC-11)


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
