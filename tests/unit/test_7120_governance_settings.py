"""C-1009 (#7120): the governance switch - deployment keys, resolution, the snapshot and its refresh.

Real code (``utils/governance_settings.py``, ``models/governance_settings.py``, the preflight in ``utils/admission.py``)
against SQLite; only the Pylon runtime is faked (see ``fixtures/governance_api_env.py``).
"""
import itertools
import sqlite3
import sys
import textwrap
import threading
import time
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import governance_api_env as genv  # noqa: E402  pylint: disable=C0413

MAX, ENABLED, ENV_LABEL = ("ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE", "ELITEA_CREDENTIAL_GOVERNANCE_ENABLED",
                           "ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT")
MODES = ("off", "audit_only", "enforce")
RANK = {"off": 0, "audit_only": 1, "enforce": 2}


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield genv.install(PLUGIN_ROOT, tmp_path_factory.mktemp("governance_settings"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.actor = genv.Actor()
    return ns.env


def stored(ns, mode="enforce", revision_label="rev-1", pat=False, domain=False, environment=None, revision=1):
    return ns.governance_settings.Stored(
        mode=mode, policy_revision=revision_label if mode != "off" else None, admit_legacy_pat=pat,
        allow_runtime_domain_binding=domain, environment=environment, revision=revision, updated_by=1, updated_at=None)


def check(ns, check_id, status="ok", blocking=True):
    return ns.governance_settings.Check(check_id, status, "OK" if status == "ok" else "X", blocking)


# --- the deployment keys ---------------------------------------------------------------------

@pytest.mark.parametrize("environ, mode, source", [
    ({}, "enforce", "default"),
    ({MAX: ""}, "enforce", "default"),
    ({MAX: "off"}, "off", "MAX_MODE"),
    ({MAX: "audit-only"}, "audit_only", "MAX_MODE"),
    ({MAX: "audit_only"}, "audit_only", "MAX_MODE"),
    ({MAX: " ENFORCE "}, "enforce", "MAX_MODE"),
    ({ENABLED: "true"}, "enforce", "ENABLED (deprecated)"),
    ({ENABLED: "TRUE"}, "enforce", "ENABLED (deprecated)"),
    ({ENABLED: "false"}, "off", "ENABLED (deprecated)"),
    ({ENABLED: ""}, "enforce", "default"),
    ({MAX: "enforce", ENABLED: "true"}, "enforce", "MAX_MODE"),
    ({MAX: "off", ENABLED: "false"}, "off", "MAX_MODE"),
])
def test_the_ceiling_comes_from_max_mode_then_the_deprecated_alias_then_the_default(ns, environ, mode, source):
    ceiling = ns.governance_settings.read_ceiling(environ)
    assert (ceiling.max_mode, ceiling.source) == (mode, source)


@pytest.mark.parametrize("max_mode, enabled", [
    ("off", "true"), ("audit-only", "true"), ("audit-only", "false"), ("enforce", "false"),
])
def test_an_alias_that_disagrees_with_max_mode_refuses_startup(ns, max_mode, enabled):
    with pytest.raises(ValueError, match="disagree"):
        ns.governance_settings.read_ceiling({MAX: max_mode, ENABLED: enabled})


@pytest.mark.parametrize("environ", [{MAX: "on"}, {MAX: "audit"}, {MAX: "2"}, {ENABLED: "yes"}, {ENABLED: "1"}])
def test_an_invalid_deployment_value_refuses_startup(ns, environ):
    with pytest.raises(ValueError):
        ns.governance_settings.read_ceiling(environ)


@pytest.mark.parametrize("ceiling_env", [{}, {MAX: "off"}, {MAX: "audit-only"}, {MAX: "enforce"}])
@pytest.mark.parametrize("alias", [None, "true", "false"])
def test_ceiling_by_alias_table(ns, ceiling_env, alias):
    """Every pairing of MAX_MODE (unset or each value) with the alias (unset, true, false): either the ceiling that
    wins or a refusal when the two are set and disagree."""
    environ = dict(ceiling_env, **({ENABLED: alias} if alias else {}))
    alias_mode = {None: None, "true": "enforce", "false": "off"}[alias]
    max_mode = ceiling_env.get(MAX, "").replace("-", "_") or None
    if max_mode and alias_mode and max_mode != alias_mode:
        with pytest.raises(ValueError):
            ns.governance_settings.read_ceiling(environ)
        return
    ceiling = ns.governance_settings.read_ceiling(environ)
    assert ceiling.max_mode == (max_mode or alias_mode or "enforce")


def test_the_environment_label_is_the_deployments_when_set(ns):
    gs = ns.governance_settings
    assert gs.environment_label({}) is None and gs.environment_label({ENV_LABEL: ""}) is None
    assert gs.environment_label({ENV_LABEL: " prod-eu "}) == "prod-eu"
    with pytest.raises(ValueError):
        gs.environment_label({ENV_LABEL: "x" * 65})
    assert gs.environment_label({ENV_LABEL: "x" * 65}, strict=False) is None


def test_the_former_keys_are_listed_as_ignored(ns):
    gs = ns.governance_settings
    environ = {f"ELITEA_CREDENTIAL_GOVERNANCE_{name}": "x" for name in
               ("POLICY_REVISION", "MODE", "ADMIT_LEGACY_PAT", "ALLOW_RUNTIME_DOMAIN_BINDING")}
    config = {"enterprise_credential_governance": {
        "enabled": True, "policy_revision": "r", "environment": "e", "admit_legacy_pat": True,
        "allow_runtime_domain_binding": True, "context_ttl_s": 120}}
    found = gs.ignored_settings(config, environ)
    assert len(found) == 9 and "enterprise_credential_governance.context_ttl_s" not in found
    assert gs.ignored_settings({}, {}) == [] and gs.ignored_settings(None, {MAX: "off", ENABLED: "true"}) == []


def module_with(env, config=None):
    module = env.module(governed=False)
    module.descriptor = types.SimpleNamespace(config=config or {})
    return module


def test_startup_warns_once_per_ignored_key_and_once_for_the_alias_and_does_not_switch_governance_on(
        ns, env, monkeypatch):
    for name in ("POLICY_REVISION", "MODE", "ADMIT_LEGACY_PAT", "ALLOW_RUNTIME_DOMAIN_BINDING"):
        monkeypatch.setenv(f"ELITEA_CREDENTIAL_GOVERNANCE_{name}", "x")
    monkeypatch.setenv(ENABLED, "true")
    monkeypatch.delenv(MAX, raising=False)
    config = {"enterprise_credential_governance": {"enabled": True, "policy_revision": "r", "context_ttl_s": 600}}
    state = ns.governance_settings.create_state(module_with(env, config))
    warnings = [line for line in env.log.lines if "is ignored" in line or "deprecated" in line]
    assert len(warnings) == 7   # 4 env keys + 2 config keys + the alias
    assert sum(ENABLED in line and "deprecated" in line for line in warnings) == 1
    assert (state.ceiling.max_mode, state.ceiling.source) == ("enforce", "ENABLED (deprecated)")
    assert state.effective.mode == "off" and state.context_ttl_s == 600   # "true" is a ceiling, not a switch


def test_startup_refuses_a_conflicting_alias_and_an_invalid_max_mode(ns, env, monkeypatch):
    monkeypatch.setenv(MAX, "off")
    monkeypatch.setenv(ENABLED, "true")
    with pytest.raises(ValueError, match="disagree"):
        ns.governance_settings.create_state(module_with(env))
    monkeypatch.delenv(ENABLED)
    monkeypatch.setenv(MAX, "sometimes")
    with pytest.raises(ValueError, match="MAX_MODE"):
        ns.governance_settings.create_state(module_with(env))


@pytest.mark.parametrize("ttl", [59, 86401, "3600", True, 60.5])
def test_a_bad_context_ttl_refuses_startup_unless_the_ceiling_is_off(ns, env, monkeypatch, ttl):
    monkeypatch.delenv(ENABLED, raising=False)
    config = {"enterprise_credential_governance": {"context_ttl_s": ttl}}
    monkeypatch.setenv(MAX, "enforce")
    with pytest.raises(ValueError, match="context_ttl_s"):
        ns.governance_settings.create_state(module_with(env, config))
    monkeypatch.setenv(MAX, "off")
    assert ns.governance_settings.create_state(module_with(env, config)).context_ttl_s == 3600


def test_a_failing_preflight_no_longer_refuses_startup(ns, env, monkeypatch):
    monkeypatch.delenv(ENABLED, raising=False)
    monkeypatch.delenv(MAX, raising=False)
    genv.create_tables(ns)
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.insert(ns.settings_models.CredentialGovernanceSettings).values(
            mode="enforce", policy_revision="rev-1", revision=1))
        session.commit()
    env.verifier_configured = False
    ns.install_fakes(env)
    state = ns.governance_settings.create_state(module_with(env))   # nothing raised
    assert state.effective.mode == "enforce" and state.effective.unavailable is True


# --- resolve ---------------------------------------------------------------------------------

def test_resolve_named_rows(ns):
    gs = ns.governance_settings
    ok, bad = [check(ns, "identity_verifier")], [check(ns, "identity_verifier", "fail")]

    def resolved(row, ceiling="enforce", label=None, checks=ok, read="ok"):
        return gs.resolve(row, ceiling=ceiling, env_environment=label, preflight=checks, read=read)

    # no row and no table are off; the policy revision and flags are the defaults
    for read, row in (("ok", None), ("missing_table", None), ("ok", stored(ns, "off"))):
        effective = resolved(row, read=read)
        assert (effective.mode, effective.policy_revision, effective.admit_legacy_pat,
                effective.allow_runtime_domain_binding, effective.capped, effective.unavailable) == (
            "off", None, False, False, False, False)
    # the stored values are taken
    effective = resolved(stored(ns, "audit_only", "CHG-9", True, True, "stored-env"))
    assert (effective.mode, effective.policy_revision, effective.admit_legacy_pat, effective.allow_runtime_domain_binding,
            effective.environment) == ("audit_only", "CHG-9", True, True, "stored-env")
    # the deployment's label wins over the stored one
    assert resolved(stored(ns, "enforce", environment="stored-env"), label="prod-eu").environment == "prod-eu"
    # a stored mode above the ceiling is capped, flagged and kept
    effective = resolved(stored(ns, "enforce"), ceiling="audit_only")
    assert (effective.mode, effective.capped) == ("audit_only", True)
    assert resolved(stored(ns, "enforce"), ceiling="off").mode == "off"
    assert resolved(stored(ns, "audit_only"), ceiling="enforce").capped is False
    # a failing blocking check makes an on mode unavailable and leaves off alone
    assert resolved(stored(ns, "enforce"), checks=bad).unavailable is True
    assert resolved(stored(ns, "audit_only"), checks=bad).unavailable is True
    assert resolved(stored(ns, "off"), checks=bad).unavailable is False
    # a warning does not; neither do the two checks that are read elsewhere
    assert resolved(stored(ns, "enforce"), checks=[check(ns, "corporate_vault", "warn", blocking=False)]).unavailable is False
    assert resolved(stored(ns, "enforce"), checks=[check(ns, "audit_backlog", "fail")]).unavailable is False
    assert resolved(stored(ns, "enforce"), checks=[check(ns, "settings_table", "fail")]).unavailable is False
    # a row that cannot be read on a cold start is enforce, unavailable, unless the ceiling says otherwise
    effective = resolved(None, read="error")
    assert (effective.mode, effective.unavailable, effective.read) == ("enforce", True, "error")
    assert (resolved(None, ceiling="audit_only", read="error").mode,
            resolved(None, ceiling="audit_only", read="error").unavailable) == ("audit_only", True)
    effective = resolved(None, ceiling="off", read="error")
    assert (effective.mode, effective.unavailable) == ("off", False)


@pytest.mark.parametrize("ceiling, stored_mode, label, preflight_ok, read", list(itertools.product(
    MODES, (None, *MODES), (None, "prod-eu"), (True, False), ("ok", "missing_table", "error"))))
def test_resolve_table(ns, ceiling, stored_mode, label, preflight_ok, read):
    """ceiling x stored x deployment label x preflight x read: the rules that must hold in every cell."""
    gs = ns.governance_settings
    row = stored(ns, stored_mode, "CHG-1", True, True, "stored-env") if stored_mode else None
    checks = [check(ns, "identity_verifier", "ok" if preflight_ok else "fail")]
    effective = gs.resolve(row, ceiling=ceiling, env_environment=label, preflight=checks, read=read)
    # never above the ceiling
    assert RANK[effective.mode] <= RANK[ceiling]
    # what is stored is used only when it was read
    used = row if read == "ok" else None
    wanted = used.mode if used else ("enforce" if read == "error" else "off")
    assert effective.mode == min(wanted, ceiling, key=RANK.get)
    assert effective.capped == bool(used and RANK[used.mode] > RANK[ceiling])
    assert effective.policy_revision == (used.policy_revision if used else None)
    assert effective.admit_legacy_pat is bool(used) and effective.allow_runtime_domain_binding is bool(used)
    assert effective.environment == (label or (used.environment if used else None))
    assert effective.unavailable == (effective.mode != "off" and (read == "error" or not preflight_ok))
    assert effective.profile.enabled == (effective.mode == "enforce")
    assert effective.profile.profile == ("governed" if effective.mode == "enforce" else "legacy")
    assert effective.profile.policy_revision == (effective.policy_revision if effective.mode != "off" else None)


# --- the snapshot and its refresh -------------------------------------------------------------

class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class Reader:
    def __init__(self, ns, read="ok", row=None):
        self.ns, self.calls, self.answer = ns, 0, (read, row)

    def __call__(self):
        self.calls += 1
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def new_state(ns, reader, *, ceiling="enforce", checks=None, label=None, clock=None, on_enforce=None):
    gs = ns.governance_settings
    probes = []

    def preflight(read, mode=None):
        probes.append(read)
        if isinstance(checks, Exception):
            raise checks
        return list(checks or [])

    state = gs.GovernanceState(
        ceiling=gs.Ceiling(ceiling, "default"), env_environment=label, context_ttl_s=3600, preflight=preflight,
        reader=reader, clock=clock or Clock(), on_enforce=on_enforce)
    state.probes = probes
    return state


def test_before_the_first_read_the_state_is_unknown(ns):
    state = new_state(ns, Reader(ns))
    assert (state.effective.read, state.effective.mode, state.effective.unavailable) == ("error", "enforce", True)


def test_dispatch_reads_the_snapshot_and_never_the_database(ns):
    reader = Reader(ns, "ok", stored(ns, "enforce"))
    state = new_state(ns, reader)
    state.refresh()
    assert reader.calls == 1
    for _ in range(50):
        assert state.effective.mode == "enforce" and state.effective.profile.enabled
    assert reader.calls == 1


def test_the_staleness_bound_is_ten_seconds_and_a_failed_read_is_tried_again_after_five(ns):
    gs = ns.governance_settings
    assert (gs.TTL_S, gs.RETRY_S) == (10.0, 5.0)
    clock, reader = Clock(), Reader(ns, "ok", stored(ns, "off"))
    state = new_state(ns, reader, clock=clock)
    state.refresh()
    clock.now += 9.9
    assert state.tick() is False and reader.calls == 1
    clock.now += 0.1
    assert state.tick() is True and reader.calls == 2
    reader.answer = ("error", None)
    clock.now += 10
    assert state.tick() is True and reader.calls == 3
    clock.now += 4.9
    assert state.tick() is False
    clock.now += 0.1
    assert state.tick() is True and reader.calls == 4


def test_a_changed_row_is_picked_up_at_the_next_tick(ns):
    clock, reader = Clock(), Reader(ns, "ok", stored(ns, "off"))
    state = new_state(ns, reader, clock=clock)
    state.refresh()
    reader.answer = ("ok", stored(ns, "enforce", revision=2))
    assert state.effective.mode == "off"
    clock.now += 10
    state.tick()
    assert state.effective.mode == "enforce"


def test_the_changed_event_refreshes_at_once_without_waiting_for_the_ttl(ns, env):
    reader = Reader(ns, "ok", stored(ns, "off"))
    state = new_state(ns, reader)
    state.refresh()
    reader.answer = ("ok", stored(ns, "audit_only", revision=2))
    module = types.SimpleNamespace(governance_state=state)
    assert state.effective.mode == "off"
    ns.governance_settings.on_settings_changed(module, {"revision": 2})
    assert state.effective.mode == "audit_only" and reader.calls == 2
    ns.governance_settings.on_settings_changed(types.SimpleNamespace(governance_state=None), {})   # no state: no error


def test_the_event_handler_is_registered_and_only_delegates():
    text = (PLUGIN_ROOT / "events" / "governance_settings.py").read_text()
    assert '@web.event("credential_governance_settings_changed")' in text and "on_settings_changed(self, payload)" in text
    assert text.count("def ") == 1 and "class Event:" in text


def test_a_failed_read_keeps_the_last_good_row_and_a_cold_failure_is_unknown(ns):
    reader = Reader(ns, "ok", stored(ns, "audit_only"))
    state = new_state(ns, reader)
    state.refresh()
    reader.answer = ("error", None)
    state.refresh()
    assert (state.effective.mode, state.effective.read, state.effective.unavailable) == ("audit_only", "ok", False)
    cold = new_state(ns, Reader(ns, "error"))
    cold.refresh()
    assert (cold.effective.mode, cold.effective.read, cold.effective.unavailable) == ("enforce", "error", True)
    assert new_state(ns, Reader(ns, "error"), ceiling="off").refresh().mode == "off"


def test_the_preflight_runs_only_while_a_mode_is_on_and_a_failure_makes_the_state_unavailable(ns):
    clock, reader = Clock(), Reader(ns, "ok", stored(ns, "off"))
    failing = [check(ns, "security_audit", "fail")]
    state = new_state(ns, reader, checks=failing, clock=clock)
    assert state.refresh().unavailable is False and state.probes == []   # off asks nothing of the platform
    reader.answer = ("ok", stored(ns, "enforce"))
    effective = state.refresh()
    assert state.probes == ["ok"] and effective.unavailable is True
    assert state._due == clock.now + 5   # pylint: disable=W0212  an unavailable platform is read again soon
    working = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), checks=[check(ns, "security_audit")], clock=clock)
    assert working.refresh().unavailable is False and working._due == clock.now + 10   # pylint: disable=W0212


def test_a_preflight_that_raises_counts_as_failing(ns):
    state = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), checks=RuntimeError("boom"))
    assert state.refresh().unavailable is True


def test_the_enforce_hook_runs_on_enforce_only(ns):
    calls = []
    for mode, expected in (("off", 0), ("audit_only", 0), ("enforce", 1)):
        calls.clear()
        new_state(ns, Reader(ns, "ok", stored(ns, mode)), on_enforce=lambda: calls.append(1)).refresh()
        assert len(calls) == expected, mode
    assert new_state(ns, Reader(ns, "missing_table"), on_enforce=lambda: calls.append(2)).refresh().mode == "off"
    assert 2 not in calls


def test_the_ceiling_caps_the_snapshot_and_lowering_it_keeps_the_stored_row(ns):
    row = stored(ns, "enforce")
    assert new_state(ns, Reader(ns, "ok", row), ceiling="enforce").refresh().mode == "enforce"
    capped = new_state(ns, Reader(ns, "ok", row), ceiling="audit_only").refresh()
    assert (capped.mode, capped.capped) == ("audit_only", True)


def test_the_refresher_thread_refreshes_and_stops(ns):
    reader = Reader(ns, "ok", stored(ns, "off"))
    state = new_state(ns, reader, clock=time.monotonic)
    state.POLL_S = 0.005
    state.refresh()
    state._due = 0   # pylint: disable=W0212
    state.start()
    state.start()   # idempotent
    deadline = time.time() + 5
    while reader.calls < 2 and time.time() < deadline:
        time.sleep(0.01)
    state.stop()
    assert reader.calls >= 2
    assert sum(t.name == "governance-settings" for t in threading.enumerate() if t.is_alive()) <= 1


# --- reading the stored row ---------------------------------------------------------------------

def test_a_missing_table_reads_as_no_row_and_a_present_one_returns_the_row(ns, env):
    gs = ns.governance_settings
    assert gs.read_settings() == ("missing_table", None)
    genv.create_tables(ns)
    assert gs.read_settings() == ("ok", None)
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.insert(ns.settings_models.CredentialGovernanceSettings).values(
            mode="audit_only", policy_revision="CHG-7", admit_legacy_pat=True, revision=3, updated_by=9))
        session.commit()
    read, row = gs.read_settings()
    assert read == "ok" and (row.mode, row.policy_revision, row.admit_legacy_pat, row.revision, row.updated_by) == (
        "audit_only", "CHG-7", True, 3, 9)


def test_a_read_error_is_logged_by_type_only_and_reads_as_error(ns, env):
    def boom(project_id):
        raise RuntimeError("password=hunter2")
    original = ns.tools.db.get_session
    ns.tools.db.get_session = boom
    try:
        assert ns.governance_settings.read_settings() == ("error", None)
    finally:
        ns.tools.db.get_session = original
    assert any("RuntimeError" in line for line in env.log.lines)
    assert not any("hunter2" in line for line in env.log.lines)


def test_the_tables_are_not_on_the_shared_base_and_the_row_is_constrained(ns, env):
    assert ns.settings_models.METADATA is not ns.tools.db.Base.metadata
    assert not {"credential_governance_settings", "credential_governance_settings_history"} & {
        table.name for table in ns.tools.db.Base.metadata.tables.values()}
    genv.create_tables(ns)
    model = ns.settings_models.CredentialGovernanceSettings
    for bad in (dict(mode="sometimes", policy_revision="r"), dict(mode="enforce", policy_revision=None),
                dict(mode="off", scope_kind="project"), dict(mode="off", scope_id=5)):
        with ns.tools.db.get_session(None) as session:
            with pytest.raises(ns.sa.exc.IntegrityError):
                session.execute(ns.sa.insert(model).values(**bad))
                session.commit()
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.insert(model).values(mode="off"))
        session.commit()
        with pytest.raises(ns.sa.exc.IntegrityError):   # one row per scope
            session.execute(ns.sa.insert(model).values(mode="off"))
            session.commit()


def test_the_admin_task_creates_the_tables_once_and_a_dry_run_changes_nothing(ns, env):
    gs = ns.governance_settings
    dry = gs.create_tables(dry_run=True)
    assert dry["would_create"] == ["credential_governance_settings", "credential_governance_settings_history"]
    assert dry["created"] == [] and gs.read_settings()[0] == "missing_table"
    real = gs.create_tables()
    assert real["created"] == dry["would_create"] and gs.read_settings() == ("ok", None)
    again = gs.create_tables()
    assert again["created"] == [] and again["would_create"] == []   # idempotent
    text = (PLUGIN_ROOT / "methods" / "admin_tasks.py").read_text()
    module = (PLUGIN_ROOT / "module.py").read_text()
    assert "def create_governance_settings_tables(self" in text and "@web.method()" in text
    assert module.count('"create_governance_settings_tables"') == 2   # registered and unregistered
    assert 'group="R-2.0.8"' in module[module.index('"create_governance_settings_tables", self.create_governance_settings_tables'):][:200]
    assert "create_governance_settings_tables" in (PLUGIN_ROOT / "db_migrations.txt").read_text()


# --- the boot-time preflight and the module wiring ------------------------------------------------

def test_a_failing_preflight_at_boot_with_the_mode_on_is_logged_and_audited(ns, env):
    gs = ns.governance_settings
    module = types.SimpleNamespace(governance_state=new_state(
        ns, Reader(ns, "ok", stored(ns, "enforce")), checks=[check(ns, "security_audit", "fail")]))
    module.governance_state.refresh()
    gs.record_boot_preflight(module)
    [event] = env.denied
    assert (event["event_type"], event["reason_code"], event["outcome"], event["caller_ref"]) == (
        "lifecycle", "GOVERNANCE_PREFLIGHT_FAILED", "failed", "system:boot")
    assert any("required service is unavailable" in line for line in env.log.lines)


def test_no_boot_event_when_off_or_when_the_preflight_passes(ns, env):
    gs = ns.governance_settings
    for mode, checks in (("off", [check(ns, "security_audit", "fail")]), ("enforce", [check(ns, "security_audit")])):
        module = types.SimpleNamespace(governance_state=new_state(ns, Reader(ns, "ok", stored(ns, mode)), checks=checks))
        module.governance_state.refresh()
        gs.record_boot_preflight(module)
    assert env.denied == []


def test_a_failure_to_audit_the_boot_preflight_is_logged_not_raised(ns, env):
    module = types.SimpleNamespace(governance_state=new_state(
        ns, Reader(ns, "ok", stored(ns, "enforce")), checks=[check(ns, "security_audit", "fail")]))
    module.governance_state.refresh()
    env.autocommit_raises = RuntimeError("outbox")
    ns.governance_settings.record_boot_preflight(module)
    assert any("was not recorded" in line for line in env.log.lines)


def test_the_module_answers_from_the_snapshot(ns, env):
    source = (PLUGIN_ROOT / "module.py").read_text()
    block = source[source.index("    @property\n    def governance_effective"):source.index("    def _start_governance")]
    namespace = {"AdmissionSettings": ns.adm.AdmissionSettings}
    exec(compile("class Holder:\n" + block, "module.py", "exec"), namespace)   # pylint: disable=W0122
    holder = namespace["Holder"]()
    reader = Reader(ns, "ok", stored(ns, "enforce", "CHG-5", True, False, "stored-env"))
    holder.governance_state = new_state(ns, reader, label="prod-eu")
    holder.governance_state.refresh()
    assert holder.governance_profile.enabled is True and holder.governance_profile.policy_revision == "CHG-5"
    assert holder.admission_settings == ns.adm.AdmissionSettings(
        allow_runtime_domain_binding=False, admit_legacy_pat=True, context_ttl_s=3600, environment="prod-eu",
        allow_development_runs=True)   # not chosen under enforce: on (#7227 D14)
    reader.answer = ("ok", stored(ns, "audit_only", "CHG-6"))
    holder.governance_state.refresh()
    assert holder.governance_profile.enabled is False and holder.governance_profile.policy_revision == "CHG-6"
    assert holder.governance_effective.mode == "audit_only"
    reader.answer = ("ok", stored(ns, "off"))
    holder.governance_state.refresh()
    assert holder.governance_profile.enabled is False and holder.governance_profile.policy_revision is None


def test_the_module_registers_the_two_permissions_with_the_super_admin_pin():
    text = (PLUGIN_ROOT / "module.py").read_text()
    assert '("governance.settings.view", {"super_admin": True, "admin": True})' in text
    assert '("governance.settings.manage", {"super_admin": True, "admin": False})' in text
    block = text[text.index('("governance.settings.view"'):][:900]
    assert '"default": {"super_admin": False, "admin": False, "viewer": False, "editor": False, "system": False}' in block
    assert textwrap.dedent(block).count('"viewer": False, "editor": False') >= 1


# --- the handlers are registered before enforce is published (#7120 review) -------------------------------------

def test_the_handlers_are_registered_before_an_enforce_snapshot_is_published(ns):
    seen = []
    state = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), on_enforce=lambda: seen.append(state.effective.mode))
    assert state.effective.mode == "enforce" and state.effective.read == "error"   # the unknown cold-start state
    state.refresh()
    assert seen == ["enforce"] and state.effective.read == "ok"
    reader = Reader(ns, "ok", stored(ns, "off"))
    switching = new_state(ns, reader, on_enforce=lambda: seen.append(switching.effective.mode))
    switching.refresh()
    reader.answer = ("ok", stored(ns, "enforce", revision=2))
    seen.clear()
    switching.refresh()
    assert seen == ["off"]   # when the hook ran, the snapshot in force was still the old one


def test_a_registration_that_fails_publishes_enforce_as_unavailable_and_is_tried_again_at_the_next_tick(ns):
    clock, outcomes = Clock(), [RuntimeError("worker bus not up"), None]

    def register():
        outcome = outcomes.pop(0)
        if outcome is not None:
            raise outcome
    state = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), clock=clock, on_enforce=register)
    effective = state.refresh()
    assert (effective.mode, effective.unavailable) == ("enforce", True)   # governed runs are refused, readably
    assert state.effective is effective and state._due == clock.now + 5   # pylint: disable=W0212
    clock.now += 5
    assert state.tick() is True
    assert state.effective.unavailable is False and outcomes == []   # registered; admitting again


def test_the_module_reports_whether_the_required_handlers_are_registered():
    text = (PLUGIN_ROOT / "module.py").read_text()
    ensure = text[text.index("def _ensure_governed_handlers_registered"):text.index("def _unregister_governed_handlers")]
    # the validate and the single-slot release are required; the batch release is optional (see the perf tests)
    assert 'return self._governed_handlers["validate"] and self._governed_handlers["release"]' in ensure
    settings = (PLUGIN_ROOT / "utils" / "governance_settings.py").read_text()
    assert "if ensure is not None and ensure() is False:" in settings


# --- a switch that vanishes is not a switch to off ----------------------------------------------------------------

@pytest.mark.parametrize("mode", ["audit_only", "enforce"])
@pytest.mark.parametrize("gone", [("missing_table", None), ("ok", None)], ids=["table dropped", "row deleted"])
def test_a_switch_that_vanishes_while_the_mode_was_on_keeps_the_last_state_and_is_audited_once(ns, env, mode, gone):
    clock, reader = Clock(), Reader(ns, "ok", stored(ns, mode))
    reported = []
    state = new_state(ns, reader, clock=clock)
    state._on_unreadable = reported.append   # pylint: disable=W0212
    state.refresh()
    reader.answer = gone
    for _ in range(3):
        effective = state.refresh()
        assert (effective.mode, effective.policy_revision, effective.read) == (mode, "rev-1", "ok")   # not off
    assert len(reported) == 1 and reported[0].mode == mode   # once, not every five seconds
    assert any("stored switch is gone" in line for line in env.log.lines)
    assert state._due == clock.now + 5   # pylint: disable=W0212  and it is looked at again soon
    reader.answer = ("ok", stored(ns, "off", revision=3))   # a real, readable off row ends it
    assert state.refresh().mode == "off"
    reader.answer = ("ok", stored(ns, mode, revision=4))
    state.refresh()
    reader.answer = gone
    state.refresh()
    assert len(reported) == 2   # and a later loss is reported again


def test_the_lost_switch_is_written_to_the_audit_trail(ns, env, monkeypatch):
    monkeypatch.delenv(ENABLED, raising=False)
    monkeypatch.delenv(MAX, raising=False)
    genv.create_tables(ns)
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.insert(ns.settings_models.CredentialGovernanceSettings).values(
            mode="audit_only", policy_revision="CHG-3", revision=1))
        session.commit()
    state = ns.governance_settings.create_state(module_with(env))
    assert state.effective.mode == "audit_only"
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.delete(ns.settings_models.CredentialGovernanceSettings))
        session.commit()
    state.refresh()
    [event] = env.denied
    assert (event["event_type"], event["reason_code"], event["outcome"], event["policy_revision"]) == (
        "lifecycle", "GOVERNANCE_SETTINGS_UNREADABLE", "failed", "CHG-3")
    assert state.effective.mode == "audit_only"


def test_a_replica_that_never_saw_the_mode_on_reads_a_missing_switch_as_off(ns, env):
    reported = []
    for read in (("missing_table", None), ("ok", None), ("ok", stored(ns, "off"))):
        state = new_state(ns, Reader(ns, *read))
        state._on_unreadable = reported.append   # pylint: disable=W0212
        assert state.refresh().mode == "off"
    assert reported == []


# --- refreshes are serialised --------------------------------------------------------------------------------------

def test_a_refresh_that_read_the_row_before_a_change_cannot_publish_it_after_the_refresh_that_read_the_change(ns):
    """The PUT commits and refreshes while the refresher is between its read and its swap."""
    in_read, release = threading.Event(), threading.Event()
    answers = [("ok", stored(ns, "off", revision=1)), ("ok", stored(ns, "enforce", revision=2))]
    calls = []

    def reader():
        calls.append(1)
        answer = answers[min(len(calls), 2) - 1] if len(calls) > 1 else answers[0]
        if len(calls) == 1:
            in_read.set()
            assert release.wait(5)   # the refresher holds the old row while the change is committed
        return answer

    state = new_state(ns, reader)
    refresher = threading.Thread(target=state.refresh)
    refresher.start()
    assert in_read.wait(5)
    put = threading.Thread(target=state.refresh)   # the PUT's refresh, after its commit
    put.start()
    time.sleep(0.05)
    assert len(calls) == 1   # it waits for the refresh in flight: one read at a time
    release.set()
    refresher.join(5)
    put.join(5)
    assert len(calls) == 2
    assert state.effective.mode == "enforce" and state.revision == 2   # the change is what stays published


def test_the_hook_runs_inside_the_lock_so_a_registration_is_never_raced(ns):
    state = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), on_enforce=lambda: None)
    held = []
    state._on_enforce = lambda: held.append(state._lock.locked())   # pylint: disable=W0212
    state.refresh()
    assert held == [True]


# --- the refresher thread ----------------------------------------------------------------------------------------

def test_the_refresher_is_not_started_when_the_ceiling_is_off(ns):
    state = new_state(ns, Reader(ns, "ok", stored(ns, "off")), ceiling="off")
    state.start()
    assert state._thread is None   # pylint: disable=W0212  the mode cannot leave off: nothing to refresh
    on = new_state(ns, Reader(ns, "ok", stored(ns, "off")), ceiling="audit_only")
    on.POLL_S = 0.005
    on.start()
    assert on._thread is not None   # pylint: disable=W0212
    on.stop()


def test_stop_joins_the_refresher_with_a_timeout_and_the_hook_is_not_run_after_it(ns):
    calls = []
    state = new_state(ns, Reader(ns, "ok", stored(ns, "enforce")), on_enforce=lambda: calls.append(1))
    state.POLL_S = 0.005
    state.start()
    thread = state._thread   # pylint: disable=W0212
    state.stop()
    assert not thread.is_alive()   # joined, not left to the daemon flag
    before = len(calls)
    state.refresh()
    assert len(calls) == before and state.effective.mode == "enforce"   # no registration once stopped
    assert state.JOIN_S == 2.0
    state.stop()   # idempotent, and a stop from the thread itself does not join itself


# --- the audit schema --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("mode, unavailable", [("audit_only", True), ("enforce", False)])
def test_a_snapshot_is_unavailable_for_audit_only_but_not_enforce_when_the_audit_schema_is_outdated(
        ns, env, mode, unavailable):
    env.audit_has_would_deny = False
    module = env.module(governed=False)
    state = genv.state_for(ns, module)
    state._reader = lambda: ("ok", stored(ns, mode))   # pylint: disable=W0212
    assert state.refresh().unavailable is unavailable


# --- governed runs that outlive a restart ------------------------------------------------------------------------

def attached(*schemas):
    """A SQLite session with each schema attached as its own database (a project schema), and the context table in it."""
    engine = ns_engine()
    connection = engine.connect()
    for name in schemas:
        connection.exec_driver_sql(f"ATTACH DATABASE ':memory:' AS {name}")
        connection.exec_driver_sql(
            f"CREATE TABLE {name}.execution_security_context (id INTEGER PRIMARY KEY, state TEXT, expires_at TEXT)")
    return connection


def ns_engine():
    import sqlalchemy
    return sqlalchemy.create_engine("sqlite://")


def put_context(connection, schema, state, expires_at):
    connection.exec_driver_sql(
        f"INSERT INTO {schema}.execution_security_context (state, expires_at) VALUES (?, ?)", (state, expires_at))


def test_a_live_context_in_any_project_schema_is_found_and_finished_ones_are_not(ns):
    gs = ns.governance_settings
    connection = attached("p_1", "p_2", "p_3")
    session = ns.sa_orm.Session(bind=connection)
    now = "2026-10-20 12:00:00"
    assert gs.live_context_exists(session, ["p_1", "p_2", "p_3"], now) is False
    put_context(connection, "p_1", "completed", "2026-10-20 13:00:00")   # ended
    put_context(connection, "p_2", "revoked", "2026-10-20 13:00:00")
    put_context(connection, "p_3", "running", "2026-10-20 11:00:00")    # expired
    assert gs.live_context_exists(session, ["p_1", "p_2", "p_3"], now) is False
    put_context(connection, "p_3", "admitted", "2026-10-20 13:00:00")
    assert gs.live_context_exists(session, ["p_1", "p_2", "p_3"], now) is True
    assert gs.live_context_exists(session, [], now) is False


def test_the_lookup_is_chunked_stops_at_the_first_hit_and_only_quotes_project_schema_names(ns, monkeypatch):
    gs = ns.governance_settings
    monkeypatch.setattr(gs, "LIVE_CHUNK", 2)
    seen = []

    class Session:
        def __init__(self, hit_on):
            self.hit_on = hit_on

        def execute(self, statement, params):
            seen.append(str(statement))
            return types.SimpleNamespace(first=lambda: (1,) if len(seen) == self.hit_on else None)

    names = ["p_1", "p_2", "p_3", "p_4", "p_5", 'p_1"; DROP TABLE x; --', "public", "p_", "P_7", "p_8 "]
    assert gs.live_context_exists(Session(hit_on=2), names, "now") is True
    assert len(seen) == 2 and all(" UNION ALL " in text for text in seen)   # chunks of 2, stopped at the second
    text = "\n".join(seen)
    assert "DROP" not in text and "public" not in text and '"p_1"' in text and '"p_5"' not in text
    seen.clear()
    assert gs.live_context_exists(Session(hit_on=0), names, "now") is False and len(seen) == 3   # 5 valid names / 2


def test_one_catalog_query_lists_the_project_schemas_and_a_failure_answers_no(ns, env):
    gs = ns.governance_settings
    queries = []

    class Session:
        def execute(self, statement, params=None):
            queries.append(str(statement))
            if len(queries) == 1:
                return types.SimpleNamespace(scalars=lambda: iter(["p_1", "p_2"]))
            return types.SimpleNamespace(first=lambda: (1,))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    original = ns.tools.db.get_session
    ns.tools.db.get_session = lambda project_id: Session()
    try:
        assert gs.has_live_contexts() is True
    finally:
        ns.tools.db.get_session = original
    assert "information_schema.tables" in queries[0] and "execution_security_context" in queries[0] and len(queries) == 2
    ns.tools.db.get_session = lambda project_id: (_ for _ in ()).throw(RuntimeError("db down: password=hunter2"))
    try:
        assert gs.has_live_contexts() is False
    finally:
        ns.tools.db.get_session = original
    assert any("RuntimeError" in line for line in env.log.lines) and not any("hunter2" in line for line in env.log.lines)


def test_booting_into_off_with_live_runs_registers_the_handlers_otherwise_nothing():
    text = (PLUGIN_ROOT / "module.py").read_text()
    start = text[text.index("    def _start_governance"):text.index("    def _ensure_governed_handlers_registered")]
    assert start.index("state.refresh()") < start.index("has_live_contexts()") < start.index("state.start()")
    assert "if not self.governed_handlers_registered and has_live_contexts():" in start
    assert "_ensure_governed_handlers_registered()" in start
