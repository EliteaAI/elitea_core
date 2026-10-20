"""C-1009 (#7120): ``off`` is today's platform with governance disabled.

Whatever makes the mode ``off`` (a stored ``off`` row, no row, a missing table, ``MAX_MODE=off``, the deprecated
``ENABLED=false``), the real ``_maintenance_gated_start_task`` of ``module.py`` sends the worker exactly what the
integration head sent before this change (``fixtures/golden/governance_off_dispatch.json``, captured from that head
before the switch was written), for a chat, a predict, a pipeline and a test-tool dispatch, with or without an admission
request - and calls nothing: no identity verification, no audit method, no credential facts, no secret store, no
database, no corporate Vault module loaded.
"""
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import governance_api_env as genv  # noqa: E402  pylint: disable=C0413
from fixtures.dispatch_wrapper import run_matrix  # noqa: E402  pylint: disable=C0413

GOLDEN = PLUGIN_ROOT / "tests" / "fixtures" / "golden" / "governance_off_dispatch.json"
MAX, ENABLED = "ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE", "ELITEA_CREDENTIAL_GOVERNANCE_ENABLED"
OFF_SOURCES = ["stored off row", "no row", "missing table", "MAX_MODE=off", "ENABLED=false (deprecated)"]


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield genv.install(PLUGIN_ROOT, tmp_path_factory.mktemp("off_invariant"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


def stored(ns, mode):
    return ns.governance_settings.Stored(
        mode=mode, policy_revision="rev-1" if mode != "off" else None, admit_legacy_pat=True,
        allow_runtime_domain_binding=True, environment=None, revision=1, updated_by=1, updated_at=None)


def effective_from(ns, source):
    gs = ns.governance_settings
    row, read, environ = {
        "stored off row": (stored(ns, "off"), "ok", {}),
        "no row": (None, "ok", {}),
        "missing table": (None, "missing_table", {}),
        "MAX_MODE=off": (stored(ns, "enforce"), "ok", {MAX: "off"}),
        "ENABLED=false (deprecated)": (stored(ns, "audit_only"), "ok", {ENABLED: "false"}),
    }[source]
    effective = gs.resolve(row, ceiling=gs.read_ceiling(environ).max_mode, env_environment=None, preflight=[], read=read)
    assert effective.mode == "off"
    return effective


def golden():
    return json.loads(GOLDEN.read_text())


def as_json(out):
    return json.loads(json.dumps({name: [[list(args), kwargs] for args, kwargs in calls] for name, calls in out.items()},
                                 sort_keys=True))


class Spies:
    """Every thing governance could touch, as mocks that fail loudly - and are asserted not called afterwards, because
    a broad ``except`` in the code under test would swallow the failure itself."""

    def __init__(self, ns):
        self.ns = ns
        boom = AssertionError("governance touched in off")
        self.names = {
            "auth.verify_identity": mock.Mock(side_effect=boom), "auth.read_identity_epochs": mock.Mock(side_effect=boom),
            "this.for_module (tracing, secrets, configurations)": mock.Mock(side_effect=boom),
            "secret_stores.get_store": mock.Mock(side_effect=boom), "db.get_session": mock.Mock(side_effect=boom),
            "settings read": mock.Mock(side_effect=boom), "admit_execution": mock.Mock(side_effect=boom),
            "_admit": mock.Mock(side_effect=boom), "flask.has_request_context": mock.Mock(side_effect=boom),
        }

    def __enter__(self):
        ns, spies = self.ns, self.names
        self.saved = (ns.tools.auth.verify_identity, ns.tools.auth.read_identity_epochs, ns.tools.this.for_module,
                      ns.tools.db.get_session, ns.adm.admit_execution, ns.adm._admit,  # pylint: disable=W0212
                      ns.governance_settings.read_settings, ns.flask.has_request_context)
        ns.tools.auth.verify_identity = spies["auth.verify_identity"]
        ns.tools.auth.read_identity_epochs = spies["auth.read_identity_epochs"]
        ns.tools.this.for_module = spies["this.for_module (tracing, secrets, configurations)"]
        ns.tools.db.get_session = spies["db.get_session"]
        ns.tools.secret_stores.get_store = spies["secret_stores.get_store"]
        ns.adm.admit_execution, ns.adm._admit = spies["admit_execution"], spies["_admit"]  # pylint: disable=W0212
        ns.governance_settings.read_settings = spies["settings read"]
        ns.flask.has_request_context = spies["flask.has_request_context"]
        return self

    def __exit__(self, *exc):
        ns = self.ns
        (ns.tools.auth.verify_identity, ns.tools.auth.read_identity_epochs, ns.tools.this.for_module,
         ns.tools.db.get_session, ns.adm.admit_execution, ns.adm._admit,  # pylint: disable=W0212
         ns.governance_settings.read_settings, ns.flask.has_request_context) = self.saved
        vars(ns.tools.secret_stores).pop("get_store", None)
        return False

    def assert_untouched(self):
        for name, spy in self.names.items():
            assert not spy.called, name
            spy.assert_not_called()


@pytest.mark.parametrize("with_request", [False, True], ids=["no request", "with a request"])
@pytest.mark.parametrize("source", OFF_SOURCES)
def test_off_sends_the_worker_exactly_what_the_integration_head_sent_and_touches_nothing(ns, env, source, with_request):
    module = env.module(effective=effective_from(ns, source))
    request = env.request() if with_request else None
    with Spies(ns) as spies:
        out = run_matrix(ns, module, admission=request)
    spies.assert_untouched()
    assert as_json(out) == golden()
    assert set(out) == {"chat", "predict", "pipeline", "test_tool"}
    # nothing was tracked, and nothing marks a governed run
    assert module._context_by_task == {}   # pylint: disable=W0212
    for calls in out.values():
        [(_, sent)] = calls
        assert "admission" not in sent and "context_ref" not in sent["meta"]
        assert sent["kwargs"]["envelope"] == {
            "context_ref": None, "required_capabilities": [], "schema_version": 1, "security_profile": "legacy"} or (
            "envelope" not in sent["kwargs"])


def test_the_golden_snapshot_has_the_four_dispatch_kinds_and_the_legacy_envelope():
    data = golden()
    assert set(data) == {"chat", "predict", "pipeline", "test_tool"}
    for name in ("chat", "predict", "pipeline"):
        [[args, sent]] = data[name]
        assert sent["kwargs"]["envelope"]["security_profile"] == "legacy" and sent["kwargs"]["_elitea_predict_run_id"]
    [[args, sent]] = data["test_tool"]
    assert args == ["indexer_test_toolkit_tool"] and sent["kwargs"]["envelope"]["security_profile"] == "legacy"


@pytest.mark.parametrize("source", OFF_SOURCES)
def test_off_builds_no_admission_request_and_reads_nothing(ns, env, source):
    module = env.module(effective=effective_from(ns, source))
    env.sio_users["sid-1"] = type("Holder", (), {"type": "user", "id": "42", "reference": "cookie"})()
    with Spies(ns) as spies:
        request = ns.pd_admission.admission_request_from(
            module, sid="sid-1", project_id=1, binding_project_id=1,
            version_row=type("V", (), {"id": 70, "application_id": 7})(), run_kind="interactive", entry_point="chat_sio")
    spies.assert_untouched()
    assert request is None


@pytest.mark.parametrize("source", OFF_SOURCES)
def test_off_admit_dispatch_returns_before_looking_at_the_call(ns, env, source):
    module = env.module(effective=effective_from(ns, source))
    call = {"args": [], "kwargs": {"project_id": 1}, "pool": "agents", "meta": {}}
    with Spies(ns) as spies:
        assert ns.adm.admit_dispatch(module, "indexer_agent", call, env.request()) is None
    spies.assert_untouched()
    assert call == {"args": [], "kwargs": {"project_id": 1}, "pool": "agents", "meta": {}}


def test_the_corporate_vault_module_is_not_loaded_by_governance_in_off(ns, env):
    module = env.module(effective=effective_from(ns, "no row"))
    run_matrix(ns, module, admission=env.request())
    assert not [name for name in sys.modules if name.endswith("secret_stores.corporate_vault") or "corporate_vault" in name]


def test_booting_in_off_registers_no_handler_and_runs_no_preflight(ns, env):
    gs = ns.governance_settings
    registered, probes = [], []
    state = gs.GovernanceState(
        ceiling=gs.Ceiling("enforce", "default"), env_environment=None, context_ttl_s=3600,
        preflight=lambda read: probes.append(read) or [], reader=lambda: ("ok", None), on_enforce=lambda: registered.append(1))
    state.refresh()
    state.refresh()
    assert registered == [] and probes == [] and state.effective.mode == "off"


def test_the_lifecycle_does_nothing_in_off_when_enforce_never_ran(ns, env):
    module = env.module(effective=effective_from(ns, "no row"))
    module._context_by_task["task-1"] = (1, "x")   # pylint: disable=W0212
    with Spies(ns) as spies:
        ns.adm.complete_context(module, "task-1")
        assert ns.adm.reopen_contexts(module, 1, ["x"]) == 0
        assert ns.adm.enforce_run_deadlines(module) == 0
    spies.assert_untouched()
    assert "task-1" in module._context_by_task   # pylint: disable=W0212
