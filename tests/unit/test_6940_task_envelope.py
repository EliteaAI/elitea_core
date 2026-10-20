"""C-1009 (#6940): versioned worker task envelope, capability registry, dispatch gate."""

import json
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_module_with_stubs  # noqa: E402

PKG = "plugins.elitea_core"


@pytest.fixture(scope="module")
def mods():
    saved = dict(sys.modules)  # the stubs below must not leak into other suites
    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")
    tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
    )
    sys.modules.update({"pylon": pylon, "pylon.core": core, "pylon.core.tools": tools})
    env = load_module_with_stubs(
        PLUGIN_ROOT / "models" / "pd" / "task_envelope.py", f"{PKG}.models.pd.task_envelope")
    exc = load_module_with_stubs(PLUGIN_ROOT / "utils" / "exceptions.py", f"{PKG}.utils.exceptions")
    caps = load_module_with_stubs(
        PLUGIN_ROOT / "utils" / "worker_capabilities.py", f"{PKG}.utils.worker_capabilities")
    yield types.SimpleNamespace(env=env, exc=exc, caps=caps)
    sys.modules.clear()
    sys.modules.update(saved)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _payload(ident="w1", capabilities=("task_envelope.v1",)):
    return {"ident": ident, "pool": "agents", "capabilities": list(capabilities), "ts": 1.0}


# --- envelope model -------------------------------------------------------------------------

def test_attach_legacy_envelope(mods):
    kwargs = mods.env.attach_envelope({"a": 1}, "legacy")
    assert kwargs["envelope"] == {
        "schema_version": 1, "security_profile": "legacy",
        "required_capabilities": [], "context_ref": None,
    }
    assert kwargs["a"] == 1


def test_attach_governed_envelope_requires_v1_capability(mods):
    kwargs = mods.env.attach_envelope({}, "governed")
    assert kwargs["envelope"]["security_profile"] == "governed"
    assert kwargs["envelope"]["required_capabilities"] == ["task_envelope.v1"]


def test_attach_keeps_existing_envelope(mods):
    governed = mods.env.attach_envelope({}, "governed")["envelope"]
    kwargs = mods.env.attach_envelope({"envelope": governed}, "legacy")
    assert kwargs["envelope"] is governed


def test_envelope_is_json_serialisable_round_trip(mods):
    kwargs = mods.env.attach_envelope({}, "governed")
    wire = json.loads(json.dumps(kwargs))
    envelope = mods.env.read_envelope(wire)
    assert envelope.security_profile == "governed"
    assert envelope.required_capabilities == frozenset({"task_envelope.v1"})


def test_missing_envelope_reads_as_legacy_default(mods):
    envelope = mods.env.read_envelope({"other": 1})
    assert envelope == mods.env.TaskEnvelope()
    assert envelope.security_profile == "legacy" and envelope.required_capabilities == frozenset()


def test_legacy_envelope_is_never_rejected(mods):
    wire = {"envelope": {"schema_version": 7, "security_profile": "legacy", "future": 1}}
    assert mods.env.read_envelope(wire).security_profile == "legacy"


def test_governed_unknown_schema_version_is_rejected(mods):
    wire = mods.env.attach_envelope({}, "governed")
    wire["envelope"]["schema_version"] = 2
    with pytest.raises(mods.env.EnvelopeRejected) as err:
        mods.env.read_envelope(wire)
    assert err.value.reason_code == "UNKNOWN_SCHEMA_VERSION"


def test_governed_extra_field_is_rejected(mods):
    wire = mods.env.attach_envelope({}, "governed")
    wire["envelope"]["api_token"] = "x"
    with pytest.raises(mods.env.EnvelopeRejected) as err:
        mods.env.read_envelope(wire)
    assert err.value.reason_code == "MALFORMED_ENVELOPE"


@pytest.mark.parametrize("bad", ["governed", 5, ["governed"], {"security_profile": "bogus"}])
def test_malformed_envelope_is_rejected(mods, bad):
    with pytest.raises(mods.env.EnvelopeRejected) as err:
        mods.env.read_envelope({"envelope": bad})
    assert err.value.reason_code == "MALFORMED_ENVELOPE"


def test_governed_missing_capability_is_rejected(mods):
    wire = mods.env.attach_envelope({}, "governed")
    wire["envelope"]["required_capabilities"] = ["task_envelope.v1", "future.v9"]
    with pytest.raises(mods.env.EnvelopeRejected) as err:
        mods.env.read_envelope(wire, supported_capabilities=frozenset({"task_envelope.v1"}))
    assert err.value.reason_code == "CAPABILITY_MISSING"
    assert err.value.missing == ("future.v9",)


def test_governed_supported_capabilities_pass(mods):
    wire = mods.env.attach_envelope({}, "governed")
    envelope = mods.env.read_envelope(wire, supported_capabilities=frozenset({"task_envelope.v1"}))
    assert envelope.security_profile == "governed"


def test_canary_envelope_carries_only_the_four_fields_and_no_secrets(mods):
    wire = mods.env.attach_envelope({"api_token": "secret-token-value"}, "governed")
    assert set(wire["envelope"]) == {
        "schema_version", "security_profile", "required_capabilities", "context_ref"}
    assert "secret-token-value" not in json.dumps(wire["envelope"])
    assert set(mods.env.TaskEnvelope.model_fields) == set(wire["envelope"])


# --- capability registry --------------------------------------------------------------------

def test_registry_tracks_and_matches_required_capabilities(mods):
    reg = mods.caps.WorkerCapabilityRegistry(ttl_s=120, clock=Clock())
    assert reg.has_capable_worker({"task_envelope.v1"}) is False
    reg.on_capabilities("task_node_capabilities", _payload())
    assert reg.has_capable_worker({"task_envelope.v1"}) is True
    assert reg.has_capable_worker({"task_envelope.v1", "other"}) is False
    assert reg.has_capable_worker(set()) is True


def test_registry_ttl_expiry_and_refresh(mods):
    clock = Clock()
    reg = mods.caps.WorkerCapabilityRegistry(ttl_s=120, clock=clock)
    reg.on_capabilities("e", _payload())
    clock.now += 119
    assert reg.has_capable_worker({"task_envelope.v1"}) is True
    clock.now += 2
    assert reg.has_capable_worker({"task_envelope.v1"}) is False
    reg.on_capabilities("e", _payload())
    assert reg.has_capable_worker({"task_envelope.v1"}) is True


def test_registry_any_single_worker_must_cover_all(mods):
    reg = mods.caps.WorkerCapabilityRegistry(ttl_s=120, clock=Clock())
    reg.on_capabilities("e", _payload("w1", ["a"]))
    reg.on_capabilities("e", _payload("w2", ["b"]))
    assert reg.has_capable_worker({"a", "b"}) is False
    assert reg.has_capable_worker({"b"}) is True


def test_registry_withhold_drops_worker_and_ignores_malformed(mods):
    reg = mods.caps.WorkerCapabilityRegistry(ttl_s=120, clock=Clock())
    reg.on_capabilities("e", _payload("w1"))
    reg.on_capabilities("e", {"ident": 5, "capabilities": "x"})
    reg.on_capabilities("e", {"capabilities": ["a"]})
    assert len(reg._workers) == 1  # malformed announcements were not recorded
    reg.on_withhold("e", {"ident": "w1"})
    assert reg.has_capable_worker(set()) is False


# --- dispatch gate --------------------------------------------------------------------------

LEGACY = types.SimpleNamespace(enabled=False, policy_revision=None, profile="legacy")
GOVERNED = types.SimpleNamespace(enabled=True, policy_revision="r1", profile="governed")


def _registry(mods, *workers):
    """Workers that can resolve credential slots (a governed agent task needs that, #6946)."""
    reg = mods.caps.WorkerCapabilityRegistry(ttl_s=120, clock=Clock())
    for w in workers:
        reg.on_capabilities("e", _payload(w, ["task_envelope.v1", "credential_slots.v1"]))
    return reg


def test_gate_legacy_attaches_legacy_envelope_and_does_not_consult_registry(mods):
    call = {"kwargs": {"x": 1}}
    mods.caps.apply_envelope_gate("indexer_agent", call, LEGACY, _registry(mods))
    assert call["kwargs"]["envelope"]["security_profile"] == "legacy"


def test_gate_ignores_tasks_outside_envelope_set(mods):
    call = {"kwargs": {"x": 1}}
    mods.caps.apply_envelope_gate("indexer_validator", call, GOVERNED, _registry(mods))
    assert call == {"kwargs": {"x": 1}}


def test_gate_governed_without_capable_worker_raises_and_counts(mods):
    call = {"kwargs": {}}
    before = mods.caps.GOVERNED_DISPATCH_UNAVAILABLE.total()
    with pytest.raises(mods.exc.WorkerCapabilityUnavailable) as err:
        mods.caps.apply_envelope_gate("indexer_agent", call, GOVERNED, _registry(mods))
    assert err.value.reason_code == "WORKER_CAPABILITY_UNAVAILABLE"
    assert mods.caps.GOVERNED_DISPATCH_UNAVAILABLE.total() == before + 1


def test_gate_governed_with_capable_worker_attaches_governed_envelope(mods):
    call = {}
    mods.caps.apply_envelope_gate("indexer_agent", call, GOVERNED, _registry(mods, "w1"))
    assert call["kwargs"]["envelope"]["security_profile"] == "governed"


def test_gate_redispatch_keeps_parent_envelope_even_if_flag_changed(mods):
    parent = mods.env.attach_envelope({}, "governed")["envelope"]
    call = {"kwargs": {"envelope": dict(parent)}}
    mods.caps.apply_envelope_gate("indexer_agent", call, LEGACY, _registry(mods, "w1"))
    # still the parent's governed envelope, now also asking for credential slots (a sealed payload needs them)
    assert call["kwargs"]["envelope"] == {**parent, "required_capabilities": ["credential_slots.v1", "task_envelope.v1"]}
    # and the parent's governed envelope is still gated on capability
    with pytest.raises(mods.exc.WorkerCapabilityUnavailable):
        mods.caps.apply_envelope_gate("indexer_agent", {"kwargs": {"envelope": dict(parent)}},
                                      LEGACY, _registry(mods))


def test_gate_keeps_legacy_parent_envelope_when_flag_flips_on(mods):
    legacy_parent = mods.env.attach_envelope({}, "legacy")["envelope"]
    call = {"kwargs": {"envelope": dict(legacy_parent)}}
    mods.caps.apply_envelope_gate("indexer_agent", call, GOVERNED, _registry(mods))
    assert call["kwargs"]["envelope"]["security_profile"] == "legacy"  # never re-derived


def test_module_gate_runs_in_the_start_task_wrapper_before_dispatch():
    source = (PLUGIN_ROOT / "module.py").read_text()
    wrapper = source[source.index("def _maintenance_gated_start_task"):
                     source.index("self.task_node.start_task = _maintenance_gated_start_task")]
    assert "apply_envelope_gate(" in wrapper
    assert wrapper.index("apply_envelope_gate(") < wrapper.index("_original_start_task(*args, **kwargs)")
    # the eval node runs locally in Main and has no remote worker to negotiate with
    eval_wrapper = source[source.index("def _maintenance_gated_eval_start_task"):
                          source.index("self.eval_task_node.start_task = ")]
    assert "apply_envelope_gate" not in eval_wrapper
