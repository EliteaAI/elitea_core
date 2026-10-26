"""C-1009 (#7120): the governance switch endpoints.

GET/PUT /api/v2/elitea_core/governance_settings/administration and
GET /api/v2/elitea_core/governance_settings_history/administration, run from the real files against SQLite with the
platform's authorization decided from the permissions and recommended roles the endpoints declare (see
``fixtures/governance_api_env.py``).
"""
import json
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import governance_api_env as genv  # noqa: E402  pylint: disable=C0413
from fixtures.governance_api_env import Actor  # noqa: E402  pylint: disable=C0413

JWT = "ey" + "JhbGciOiJIUzI1NiJ9.e30.c2lnbmF0dXJl"   # JWT-shaped, built here so no scanner sees a token in the source


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield genv.install(PLUGIN_ROOT, tmp_path_factory.mktemp("governance_api"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.actor = Actor()
    ns.roles = {1: ["super_admin"], 2: ["admin"], 3: [], 4: ["editor"]}
    return ns.env


@pytest.fixture
def module(ns, env):
    genv.create_tables(ns)
    module = genv.api_module(ns)
    module.governance_state.refresh()
    return module


def body(**over):
    values = {"mode": "audit_only", "policy_revision": "CHG-1", "admit_legacy_pat": False,
              "allow_runtime_domain_binding": False, "environment": None, "change_note": "start measuring",
              "expected_revision": 0}
    values.update(over)
    return values


def put(ns, module, payload):
    return genv.call(ns, module, "put", payload)


def get(ns, module):
    return genv.call(ns, module, "get")


def rows(ns, env, model):
    return env.query(None, getattr(ns.settings_models, model))


def reasons(env):
    return [event["reason_code"] for _, event in env.recorded]


# --- GET --------------------------------------------------------------------------------------

def test_get_without_the_tables_reads_off_and_says_how_to_create_them(ns, env):
    module = genv.api_module(ns)
    result, status = get(ns, module)
    assert status == 200
    assert set(result) == {"effective", "stored", "locks", "ceiling", "preflight", "table_present", "columns_present", "read",
                           "descriptions"}
    assert result["table_present"] is False and result["read"] == "missing_table" and result["stored"] is None
    assert result["effective"] == {"mode": "off", "policy_revision": None, "admit_legacy_pat": False,
                                   "allow_runtime_domain_binding": False, "allow_development_runs": False,
                                   "environment": None, "capped": False, "unavailable": False}
    table = next(item for item in result["preflight"]["checks"] if item["id"] == "settings_table")
    assert (table["status"], table["code"], table["blocking"]) == ("fail", "MISSING_TABLE", True)
    assert table["message"].startswith("Settings storage is not set up. Run the task create_governance_settings_tables")
    assert result["preflight"]["ok"] is False and result["preflight"]["state"] == "not_ready"


def test_get_shape_with_a_stored_row(ns, env, module):
    assert put(ns, module, body())[1] == 200
    result, status = get(ns, module)
    assert status == 200 and result["table_present"] is True and result["read"] == "ok"
    assert result["effective"]["mode"] == "audit_only" and result["effective"]["policy_revision"] == "CHG-1"
    stored = result["stored"]
    assert set(stored) == {"mode", "policy_revision", "admit_legacy_pat", "allow_runtime_domain_binding",
                           "allow_development_runs", "environment",
                           "revision", "updated_by", "updated_at"}
    assert (stored["mode"], stored["revision"], stored["updated_by"]) == ("audit_only", 1, 1)
    assert result["ceiling"] == {"max_mode": "enforce", "source": "default"} and result["locks"] == {}
    assert [item["id"] for item in result["preflight"]["checks"]] == [
        "identity_verifier", "auth_rpc_mode", "security_audit", "audit_schema", "audit_backlog", "credential_facts",
        "master_key", "settings_table", "corporate_vault", "governed_worker"]
    assert all(set(item) == {"id", "status", "code", "blocking", "message"} for item in result["preflight"]["checks"])
    json.dumps(result)   # the whole answer is plain JSON


def test_get_reads_the_row_fresh_not_the_snapshot(ns, env, module):
    put(ns, module, body())
    with ns.tools.db.get_session(None) as session:   # another replica changes the row; no event arrived here yet
        session.execute(ns.sa.update(ns.settings_models.CredentialGovernanceSettings).values(
            mode="enforce", revision=2))
        session.commit()
    assert module.governance_state.effective.mode == "audit_only"
    assert get(ns, module)[0]["effective"]["mode"] == "enforce"


def test_get_flags_a_capped_mode_an_unavailable_platform_and_a_locked_label(ns, env):
    genv.create_tables(ns)
    with ns.tools.db.get_session(None) as session:
        session.execute(ns.sa.insert(ns.settings_models.CredentialGovernanceSettings).values(
            mode="enforce", policy_revision="CHG-2", environment="stored-env", revision=4))
        session.commit()
    module = genv.api_module(ns, ceiling="audit_only", env_environment="prod-eu")
    result, _ = get(ns, module)
    assert result["effective"]["mode"] == "audit_only" and result["effective"]["capped"] is True
    assert result["stored"]["mode"] == "enforce"   # the stored row is kept
    assert result["ceiling"]["max_mode"] == "audit_only"
    assert result["effective"]["environment"] == "prod-eu"
    assert result["locks"] == {"environment": {"env_var": "ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT"}}
    env.verifier_configured = False
    ns.install_fakes(env)
    result, _ = get(ns, module)
    assert result["effective"]["unavailable"] is True and result["preflight"]["ok"] is False
    assert [item["id"] for item in result["preflight"]["checks"] if item["status"] == "fail"] == ["identity_verifier"]


def test_get_shows_what_is_enforced_when_the_row_cannot_be_read_just_now(ns, env, module):
    put(ns, module, body())
    original = ns.tools.db.get_session
    ns.tools.db.get_session = lambda project_id: (_ for _ in ()).throw(RuntimeError("db down"))
    try:
        result, status = get(ns, module)
    finally:
        ns.tools.db.get_session = original
    assert status == 200 and result["read"] == "error" and result["stored"] is None
    assert result["effective"]["mode"] == "audit_only"   # the snapshot in force, not a guess from nothing
    put_result, put_status = (None, None)
    ns.tools.db.get_session = lambda project_id: (_ for _ in ()).throw(RuntimeError("db down"))
    try:
        put_result, put_status = put(ns, module, body(expected_revision=1))
    finally:
        ns.tools.db.get_session = original
    assert (put_status, put_result["code"]) == (503, "DEPENDENCY_UNAVAILABLE")


def test_get_returns_the_texts_the_page_shows(ns, env, module):
    texts = get(ns, module)[0]["descriptions"]
    assert texts["modes"]["off"]["label"] == "Off"
    assert texts["modes"]["audit_only"]["label"] == "Audit only"
    assert texts["modes"]["enforce"]["label"] == "Enforce"
    assert texts["modes"]["audit_only"]["tooltip"].endswith(
        "Sub-agent runs and runs started by schedules or webhooks are not evaluated in this mode.")
    assert texts["modes"]["off"]["tooltip"].startswith("Agents run exactly as they do without governance")
    assert texts["fields"]["policy_revision"]["tooltip"].endswith("Required for audit-only and enforce.")
    assert texts["fields"]["change_note"]["placeholder"] == "Why are you changing this? (recorded in the history)"
    assert "ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE" in texts["locks"]["max_mode"]
    assert texts["preflight"]["unavailable"].startswith("Governance is on but a required service is unavailable.")
    assert set(texts["checks"]) == {"identity_verifier", "auth_rpc_mode", "security_audit", "audit_schema",
                                    "audit_backlog", "credential_facts", "master_key", "settings_table",
                                    "corporate_vault", "governed_worker", "preflight"}   # incl. the fallback check
    assert texts["checks"]["preflight"] == "The preflight could not run."
    assert texts["switch_dialog"]["off"].endswith("Runs already admitted keep their admission.")


# --- PUT --------------------------------------------------------------------------------------

def test_put_creates_the_row_the_history_the_audit_events_and_tells_the_other_replicas(ns, env, module):
    result, status = put(ns, module, body(admit_legacy_pat=True))
    assert status == 200
    [row] = rows(ns, env, "CredentialGovernanceSettings")
    assert (row.mode, row.policy_revision, row.admit_legacy_pat, row.allow_runtime_domain_binding, row.revision,
            row.updated_by, row.scope_kind, row.scope_id) == ("audit_only", "CHG-1", True, False, 1, 1, "platform", 0)
    [history] = rows(ns, env, "CredentialGovernanceSettingsHistory")
    assert (history.revision, history.mode, history.policy_revision, history.admit_legacy_pat, history.change_note,
            history.changed_by) == (1, "audit_only", "CHG-1", True, "start measuring", 1)
    # the audit trail: the change and, because the mode moved, the new mode - in the transaction of the change
    assert reasons(env) == ["GOVERNANCE_SETTINGS_CHANGED", "GOVERNANCE_MODE_AUDIT_ONLY"]
    for _, event in env.recorded:
        assert (event["event_type"], event["outcome"], event["caller_ref"], event["policy_revision"]) == (
            "lifecycle", "success", "operator:1", "CHG-1")
    assert ns.fired == [("credential_governance_settings_changed",
                         {"revision": 1, "old_mode": "off", "new_mode": "audit_only"})]
    # this replica reads the new state at once, and the answer is the new state
    assert module.governance_state.effective.mode == "audit_only" and module.governance_state.effective.admit_legacy_pat
    assert result["effective"]["mode"] == "audit_only" and result["stored"]["revision"] == 1


def test_put_is_compare_and_set_on_the_revision(ns, env, module):
    assert put(ns, module, body())[1] == 200
    stale, status = put(ns, module, body(mode="off", policy_revision=None))   # still expects "no row"
    assert (status, stale["code"]) == (409, "REVISION_CONFLICT")
    assert stale["error"] == ns.governance_settings.MESSAGES["REVISION_CONFLICT"]
    assert rows(ns, env, "CredentialGovernanceSettings")[0].mode == "audit_only"
    result, status = put(ns, module, body(mode="off", policy_revision=None, expected_revision=1, change_note="roll back"))
    assert status == 200 and result["stored"]["revision"] == 2
    wrong, status = put(ns, module, body(expected_revision=1))   # the row is at 2 now
    assert (status, wrong["code"]) == (409, "REVISION_CONFLICT")
    assert [(r.revision, r.mode) for r in rows(ns, env, "CredentialGovernanceSettingsHistory")] == [
        (1, "audit_only"), (2, "off")]


def test_put_with_a_revision_of_a_row_that_does_not_exist_is_a_conflict(ns, env, module):
    result, status = put(ns, module, body(expected_revision=3))
    assert (status, result["code"]) == (409, "REVISION_CONFLICT") and rows(ns, env, "CredentialGovernanceSettings") == []


def test_a_concurrent_first_write_is_a_conflict_not_an_error(ns, env, module):
    env.fail_commit_once = ns.sa.exc.IntegrityError("insert", {}, Exception("duplicate"))
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (409, "REVISION_CONFLICT")
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and ns.fired == []


def test_a_change_that_keeps_the_mode_writes_only_the_settings_event(ns, env, module):
    put(ns, module, body())
    env.recorded.clear()
    assert put(ns, module, body(policy_revision="CHG-2", expected_revision=1, change_note="new ticket"))[1] == 200
    assert reasons(env) == ["GOVERNANCE_SETTINGS_CHANGED"]
    assert env.recorded[0][1]["policy_revision"] == "CHG-2"
    assert ns.fired[-1][1] == {"revision": 2, "old_mode": "audit_only", "new_mode": "audit_only"}


@pytest.mark.parametrize("mode", ["off", "audit_only", "enforce"])
def test_every_mode_writes_its_own_mode_event(ns, env, module, mode):
    revision = None if mode == "off" else "CHG-1"
    # an "off" first write does not move the mode (it was off): only the settings event
    assert put(ns, module, body(mode=mode, policy_revision=revision))[1] == 200
    assert reasons(env) == (["GOVERNANCE_SETTINGS_CHANGED"] if mode == "off"
                            else ["GOVERNANCE_SETTINGS_CHANGED", f"GOVERNANCE_MODE_{mode.upper()}"])


def test_put_audit_failure_rolls_the_whole_change_back(ns, env, module):
    env.record_raises = RuntimeError("outbox full: token=abc123def456")
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (503, "AUDIT_UNAVAILABLE")
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and rows(ns, env, "CredentialGovernanceSettingsHistory") == []
    assert ns.fired == [] and module.governance_state.effective.mode == "off"
    assert not any("abc123def456" in line for line in env.log.lines)
    assert any("RuntimeError" in line for line in env.log.lines)


def test_put_audit_failure_on_a_change_leaves_the_stored_row_untouched(ns, env, module):
    put(ns, module, body())
    env.record_raises = RuntimeError("down")
    result, status = put(ns, module, body(mode="off", policy_revision=None, expected_revision=1))
    assert (status, result["code"]) == (503, "AUDIT_UNAVAILABLE")
    [row] = rows(ns, env, "CredentialGovernanceSettings")
    assert (row.mode, row.revision) == ("audit_only", 1) and len(rows(ns, env, "CredentialGovernanceSettingsHistory")) == 1


def test_put_commit_failure_is_a_dependency_error_and_changes_nothing(ns, env, module):
    env.fail_commit_once = RuntimeError("io error: password=hunter2")
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (503, "DEPENDENCY_UNAVAILABLE")
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and ns.fired == []
    assert not any("hunter2" in line for line in env.log.lines)


def test_put_without_the_tables_asks_for_the_migration(ns, env):
    module = genv.api_module(ns)
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (409, "MIGRATION_REQUIRED")
    assert "create_governance_settings_tables" in result["error"]


def test_put_above_the_ceiling_is_refused_and_a_lower_mode_is_not(ns, env):
    genv.create_tables(ns)
    module = genv.api_module(ns, ceiling="audit_only")
    result, status = put(ns, module, body(mode="enforce"))
    assert (status, result["code"], result["max_mode"]) == (409, "MODE_ABOVE_CEILING", "audit_only")
    assert rows(ns, env, "CredentialGovernanceSettings") == []
    assert put(ns, module, body(mode="audit_only"))[1] == 200
    off = genv.api_module(ns, ceiling="off")
    assert put(ns, off, body(mode="audit_only", expected_revision=1))[0]["code"] == "MODE_ABOVE_CEILING"
    assert put(ns, off, body(mode="off", policy_revision=None, expected_revision=1))[1] == 200


def test_put_environment_is_locked_when_the_deployment_sets_it(ns, env):
    genv.create_tables(ns)
    module = genv.api_module(ns, env_environment="prod-eu")
    result, status = put(ns, module, body(environment="other"))
    assert (status, result["code"], result["field"]) == (409, "FIELD_LOCKED", "environment")
    assert put(ns, module, body(environment="prod-eu"))[1] == 200          # the same value is no change
    assert put(ns, module, body(environment=None, expected_revision=1))[1] == 200   # nor is leaving it out
    assert get(ns, module)[0]["effective"]["environment"] == "prod-eu"
    assert {event["environment"] for _, event in env.recorded} == {"prod-eu"}


def test_put_environment_is_stored_and_recorded_when_the_deployment_does_not_set_it(ns, env, module):
    result, status = put(ns, module, body(environment="  qa-1 "))
    assert status == 200 and result["effective"]["environment"] == "qa-1" and result["locks"] == {}
    assert rows(ns, env, "CredentialGovernanceSettings")[0].environment == "qa-1"
    assert {event["environment"] for _, event in env.recorded} == {"qa-1"}


@pytest.mark.parametrize("mode", ["audit_only", "enforce"])
@pytest.mark.parametrize("revision", [None, "", "   "])
def test_put_needs_a_policy_revision_for_every_mode_above_off(ns, env, module, mode, revision):
    result, status = put(ns, module, body(mode=mode, policy_revision=revision))
    assert (status, result["code"]) == (400, "POLICY_REVISION_REQUIRED")
    assert rows(ns, env, "CredentialGovernanceSettings") == []


def test_put_off_needs_no_policy_revision(ns, env, module):
    assert put(ns, module, body(mode="off", policy_revision=None))[1] == 200


@pytest.mark.parametrize("break_it, failed", [
    (lambda env: setattr(env, "verifier_configured", False), "identity_verifier"),
    (lambda env: setattr(env, "auth_mode", "local"), "auth_rpc_mode"),
    (lambda env: setattr(env, "has_audit_method", False), "security_audit"),
    (lambda env: setattr(env, "audit_allowed", False), "audit_backlog"),
    (lambda env: setattr(env, "facts_version", 1), "credential_facts"),
])
def test_put_refuses_to_turn_the_mode_on_while_a_blocking_check_fails_but_allows_off(ns, env, module, break_it, failed):
    break_it(env)
    ns.install_fakes(env)
    for mode in ("audit_only", "enforce"):
        result, status = put(ns, module, body(mode=mode))
        assert (status, result["code"]) == (409, "PREFLIGHT_FAILED") and failed in result["failed_checks"]
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and ns.fired == []
    result, status = put(ns, module, body(mode="off", policy_revision=None))
    if failed == "security_audit":   # the one thing a change cannot do without: record itself
        assert (status, result["code"]) == (503, "AUDIT_UNAVAILABLE")
    else:
        assert status == 200   # rolling back is never blocked by the preflight


def test_put_a_warning_does_not_block(ns, env, module):
    env.capable = False
    ns.install_fakes(env)
    result, status = put(ns, module, body())
    assert status == 200
    assert {item["id"]: item["status"] for item in result["preflight"]["checks"]}["governed_worker"] == "warn"
    assert result["preflight"]["state"] == "ready_with_warnings"


@pytest.mark.parametrize("payload", [
    "not an object", ["mode"], None,
    {**body(), "extra": 1},
    {**body(), "scope_kind": "project"},
    body(mode="sometimes"), body(mode=None), body(mode=1),
    body(admit_legacy_pat="true"), body(admit_legacy_pat=1), body(allow_runtime_domain_binding=None),
    body(change_note=""), body(change_note="   "), body(change_note="x" * 257), body(change_note=5),
    body(policy_revision="x" * 129), body(environment="x" * 65), body(policy_revision=5),
    body(expected_revision=-1), body(expected_revision="0"), body(expected_revision=True), body(expected_revision=0.5),
])
def test_put_rejects_a_malformed_body_with_a_fixed_message(ns, env, module, payload):
    result, status = put(ns, module, payload)
    assert (status, result["code"], result["error"]) == (400, "INVALID_REQUEST", "The request is not valid.")
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and env.recorded == []


def put_raw(ns, module, raw, content_length=None):
    ns.request.args, ns.request.body, ns.request.content_length, ns.request.raw = {}, None, content_length, raw
    ns.env.g.auth = types.SimpleNamespace(type=ns.actor.kind, id=ns.actor.id)
    stream = ns.request.start()
    instance = ns.api_settings.AdminAPI()
    instance.module = module
    return instance.put(), stream


def test_put_reads_at_most_the_limit_plus_one_whatever_the_framing_and_refuses_more_with_413(ns, env, module):
    limit = ns.governance_settings.MAX_BODY_BYTES
    big = json.dumps({**body(), "change_note": "x" * limit}).encode()   # a chunked body: no length to trust
    ((result, status), stream) = put_raw(ns, module, big)
    assert (status, result["code"], result["error"]) == (413, "PAYLOAD_TOO_LARGE", "The request is too large.")
    assert stream.reads == [limit + 1]   # never more than the limit and one byte of it was taken from the wire
    ((result, status), stream) = put_raw(ns, module, big, content_length=len(big))   # a declared length is refused unread
    assert (status, result["code"]) == (413, "PAYLOAD_TOO_LARGE") and stream.reads == []
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and env.recorded == []


def test_put_accepts_a_body_right_at_the_limit_and_parses_what_it_read(ns, env, module):
    raw = json.dumps(body()).encode()
    padded = raw[:-1] + b" " * (ns.governance_settings.MAX_BODY_BYTES - len(raw)) + b"}"
    assert len(padded) == ns.governance_settings.MAX_BODY_BYTES
    ((result, status), _) = put_raw(ns, module, padded)
    assert status == 200 and result["effective"]["mode"] == "audit_only"


@pytest.mark.parametrize("raw", [b"", b"{", b"not json", b"\xff\xfe", b"[1, 2]", b"null", b'"text"'])
def test_put_refuses_a_body_that_is_not_a_json_object(ns, env, module, raw):
    ((result, status), _) = put_raw(ns, module, raw)
    assert (status, result["code"]) == (400, "INVALID_REQUEST") and rows(ns, env, "CredentialGovernanceSettings") == []


def test_put_reports_a_bounded_list_of_fields(ns, env, module):
    huge = {f"unknown_{index}_" + "x" * 200: 1 for index in range(40)}
    result, status = put(ns, module, {**body(), **huge})
    assert status == 400 and len(result["fields"]) <= ns.governance_settings.MAX_REPORTED_FIELDS
    assert all(len(item["field"]) <= 64 for item in result["fields"])


def test_put_needs_an_interactive_session_an_api_token_cannot_change_the_switch(ns, env, module):
    ns.actor = Actor(1, ["super_admin"], kind="token")   # a super administrator's own token
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (403, "FORBIDDEN")
    assert result["error"] == "The governance switch needs an interactive session; an API token cannot change it."
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and env.recorded == [] and ns.fired == []
    assert get(ns, module)[1] == 200 and history(ns, module)[1] == 200   # reading is not restricted
    ns.actor = Actor(1, ["super_admin"])
    assert put(ns, module, body())[1] == 200
    for odd in (None, "user ", "TOKEN", ""):   # anything but a session is refused
        ns.actor = Actor(1, ["super_admin"], kind=odd)
        assert put(ns, module, body(expected_revision=1))[1] == 403, odd


def test_a_put_refreshes_this_replica_once_its_own_event_is_skipped_by_revision(ns, env, module):
    state, count = module.governance_state, []
    original = state.refresh
    state.refresh = lambda: count.append(1) or original()
    module.context.event_manager.fire_event = lambda name, payload: (
        ns.fired.append((name, payload)), ns.governance_settings.on_settings_changed(module, payload))   # delivered at once
    assert put(ns, module, body())[1] == 200
    assert len(count) == 1 and state.effective.mode == "audit_only" and state.revision == 1
    count.clear()
    ns.governance_settings.on_settings_changed(module, {"revision": 2})   # a change made on another replica
    assert len(count) == 1


def test_put_requires_every_field_except_the_two_optional_ones(ns, env, module):
    for name in ("mode", "admit_legacy_pat", "allow_runtime_domain_binding", "change_note", "expected_revision"):
        payload = body()
        del payload[name]
        assert put(ns, module, payload)[0]["code"] == "INVALID_REQUEST", name
    payload = body()
    del payload["policy_revision"], payload["environment"]
    assert put(ns, module, {**payload, "mode": "off"})[1] == 200


@pytest.mark.parametrize("field", ["change_note", "policy_revision", "environment"])
def test_put_refuses_a_secret_shaped_value_and_never_echoes_it(ns, env, module, field):
    result, status = put(ns, module, body(**{field: f"see {JWT}"}))
    assert (status, result["code"]) == (400, "INVALID_REQUEST")
    assert JWT not in json.dumps(result) and not any(JWT in line for line in env.log.lines)
    assert rows(ns, env, "CredentialGovernanceSettings") == []


def test_the_change_note_is_kept_in_the_history_and_not_in_the_audit_events(ns, env, module):
    put(ns, module, body(change_note="Ticket CHG-1042: start measuring"))
    assert rows(ns, env, "CredentialGovernanceSettingsHistory")[0].change_note == "Ticket CHG-1042: start measuring"
    assert "CHG-1042: start" not in json.dumps([event for _, event in env.recorded])


def test_put_returns_the_state_as_the_get_does(ns, env, module):
    result, _ = put(ns, module, body())
    again, _ = get(ns, module)
    for key in ("effective", "stored", "locks", "ceiling", "preflight", "table_present"):
        assert result[key] == again[key], key


# --- history ----------------------------------------------------------------------------------

def history(ns, module, **args):
    return genv.call(ns, module, "get", handler="history", **args)


def test_history_lists_the_changes_newest_first_with_who_and_when(ns, env, module):
    put(ns, module, body(change_note="one"))
    ns.actor = Actor(2, ["super_admin"])
    ns.roles[2] = ["super_admin"]
    put(ns, module, body(mode="enforce", policy_revision="CHG-2", expected_revision=1, change_note="two"))
    result, status = history(ns, module)
    assert status == 200 and result["total"] == 2 and result["table_present"] is True
    [newest, oldest] = result["rows"]
    assert set(newest) == {"revision", "mode", "policy_revision", "flags", "environment", "change_note", "changed_by",
                           "changed_at"}
    assert (newest["revision"], newest["mode"], newest["policy_revision"], newest["change_note"]) == (
        2, "enforce", "CHG-2", "two")
    assert newest["changed_by"] == {"id": 2, "email": "ops@example.test"}
    assert oldest["changed_by"] == {"id": 1, "email": "root@example.test"} and oldest["revision"] == 1
    assert newest["flags"] == {"admit_legacy_pat": False, "allow_runtime_domain_binding": False,
                               "allow_development_runs": None}
    assert newest["changed_at"] is not None


def test_history_paging_and_bounds(ns, env, module):
    for index in range(3):
        put(ns, module, body(expected_revision=index, change_note=f"n{index}"))
    assert [row["revision"] for row in history(ns, module, limit="2")[0]["rows"]] == [3, 2]
    assert [row["revision"] for row in history(ns, module, limit="2", offset="2")[0]["rows"]] == [1]
    assert history(ns, module, limit="5000")[0]["total"] == 3          # clamped, not an error
    assert len(history(ns, module, limit="0")[0]["rows"]) == 1          # at least one
    for bad in ("x", "1.5", ""):
        result, status = history(ns, module, limit=bad)
        assert (status, result["code"]) == (400, "INVALID_REQUEST")
    assert history(ns, module, offset="-4")[0]["rows"][0]["revision"] == 3


def test_history_without_the_tables_is_empty(ns, env):
    result, status = history(ns, genv.api_module(ns))
    assert (status, result) == (200, {"total": 0, "rows": [], "table_present": False})


def test_history_survives_an_unreadable_user_lookup(ns, env, module):
    put(ns, module, body())

    def boom(user_ids=None):
        raise RuntimeError("auth down")
    ns.tools.auth.list_users = boom
    try:
        result, status = history(ns, module)
    finally:
        ns.tools.auth.list_users = lambda user_ids=None: [dict(u) for u in ns.users.values() if u["id"] in (user_ids or [])]
    assert status == 200 and result["rows"][0]["changed_by"] == {"id": 1, "email": None}


# --- authorization --------------------------------------------------------------------------------

ACTORS = {
    "super admin": (Actor(1, ["super_admin"]), {"get": 200, "put": 200, "history": 200}),
    "platform admin": (Actor(2, ["admin"]), {"get": 200, "put": 403, "history": 200}),
    "project admin only": (Actor(3, []), {"get": 403, "put": 403, "history": 403}),
    "project member": (Actor(4, ["editor"]), {"get": 403, "put": 403, "history": 403}),
    "viewer role": (Actor(4, ["viewer"]), {"get": 403, "put": 403, "history": 403}),
    "unauthenticated": (Actor(0, [], authenticated=False), {"get": 401, "put": 401, "history": 401}),
    "token without the permission": (Actor(1, ["super_admin"], scope={"models.projects.view"}),
                                     {"get": 403, "put": 403, "history": 403}),
}


@pytest.mark.parametrize("name", ACTORS)
def test_authorization_matrix(ns, env, module, name):
    actor, expected = ACTORS[name]
    ns.actor = actor
    assert get(ns, module)[1] == expected["get"]
    result, status = put(ns, module, body())
    assert status == expected["put"], result
    assert history(ns, module)[1] == expected["history"]
    written = rows(ns, env, "CredentialGovernanceSettings")
    assert bool(written) == (expected["put"] == 200)   # a refused write leaves nothing, not even an audit event
    if expected["put"] != 200:
        assert env.recorded == [] and ns.fired == []


def test_a_token_scoped_to_the_permissions_is_allowed_exactly_as_its_user(ns, env, module):
    ns.actor = Actor(1, ["super_admin"], scope={"governance.settings.view", "governance.settings.manage"})
    assert [get(ns, module)[1], put(ns, module, body())[1], history(ns, module)[1]] == [200, 200, 200]
    ns.actor = Actor(1, ["super_admin"], scope={"governance.settings.view"})
    assert [get(ns, module)[1], put(ns, module, body(expected_revision=1))[1], history(ns, module)[1]] == [200, 403, 200]


def test_the_switch_is_pinned_to_the_super_admin_role_beyond_the_permission(ns, env, module):
    """A role matrix edit that gives the permission to platform admins does not let them change the switch."""
    ns.actor = Actor(2, ["admin"], extra={"governance.settings.manage"})
    result, status = put(ns, module, body())
    assert (status, result["code"]) == (403, "FORBIDDEN")
    assert result["error"] == ns.governance_settings.MESSAGES["FORBIDDEN"]
    assert rows(ns, env, "CredentialGovernanceSettings") == [] and env.recorded == [] and ns.fired == []
    # nor does a user whose role lookup fails, or a caller that is not a real user
    ns.actor = Actor(1, ["super_admin"])
    ns.tools.auth.get_user_roles = lambda user_id, mode: (_ for _ in ()).throw(RuntimeError("auth down"))
    try:
        assert put(ns, module, body())[1] == 403
    finally:
        ns.tools.auth.get_user_roles = lambda user_id, mode: list(ns.roles.get(user_id, []))
    for odd in (0, -1, None, True, "1"):
        ns.actor = Actor(odd, ["super_admin"])
        assert put(ns, module, body())[1] == 403, odd
    assert rows(ns, env, "CredentialGovernanceSettings") == []


def test_the_super_admin_role_is_read_in_administration_mode_for_the_caller(ns, env):
    seen = []
    ns.tools.auth.get_user_roles = lambda user_id, mode: seen.append((user_id, mode)) or ["super_admin"]
    try:
        assert ns.governance_settings.is_super_admin(1) is True
    finally:
        ns.tools.auth.get_user_roles = lambda user_id, mode: list(ns.roles.get(user_id, []))
    assert seen == [(1, "administration")]
    assert ns.governance_settings.is_super_admin(2) is False
    ns.roles[5] = [{"name": "super_admin"}]
    assert ns.governance_settings.is_super_admin(5) is True


def test_the_endpoints_declare_the_permissions_and_roles_of_the_design(ns):
    settings = ns.api_settings.AdminAPI
    declared = {"get": settings.get.declared, "put": settings.put.declared,
                "history": ns.api_history.AdminAPI.get.declared}
    assert declared["get"]["permissions"] == ["governance.settings.view"] == declared["history"]["permissions"]
    assert declared["put"]["permissions"] == ["governance.settings.manage"]
    assert declared["get"]["recommended_roles"] == {
        "administration": {"super_admin": True, "admin": True, "viewer": False, "editor": False}}
    assert declared["history"]["recommended_roles"] == declared["get"]["recommended_roles"]
    assert declared["put"]["recommended_roles"] == {
        "administration": {"super_admin": True, "admin": False, "viewer": False, "editor": False}}
    for api in (ns.api_settings, ns.api_history):
        assert list(api.API.mode_handlers) == ["administration"] and api.API.url_params == ["<string:mode>"]


def test_the_files_register_openapi_without_exposing_the_endpoints_to_users_or_mcp():
    for name in ("governance_settings", "governance_settings_history"):
        text = (PLUGIN_ROOT / "api" / "v2" / f"{name}.py").read_text()
        assert text.count("mcp_tool=False") == text.count("@register_openapi(")
        assert text.count("available_to_users=False") == text.count("@register_openapi(")
        assert text.index("@register_openapi(") < text.index("@auth.decorators.check_api(")   # openapi sits above


def test_every_refusal_is_a_fixed_message_with_a_code_and_no_input(ns):
    gs = ns.governance_settings
    assert set(gs.STATUS) == set(gs.MESSAGES)
    for code, message in gs.MESSAGES.items():
        error = gs.SettingsError(code)
        assert error.body()["code"] == code and error.body()["error"] == message and error.status == gs.STATUS[code]
        assert "<" not in message and "{" not in message
    assert {gs.STATUS[c] for c in ("INVALID_REQUEST", "POLICY_REVISION_REQUIRED")} == {400}
    assert {gs.STATUS[c] for c in ("REVISION_CONFLICT", "FIELD_LOCKED", "MODE_ABOVE_CEILING", "PREFLIGHT_FAILED",
                                   "MIGRATION_REQUIRED")} == {409}
    assert {gs.STATUS[c] for c in ("DEPENDENCY_UNAVAILABLE", "AUDIT_UNAVAILABLE")} == {503}


# --- allow_development_runs (#7213): stored when chosen, the default of the mode otherwise, added to an older table ----------

def test_put_stores_the_development_choice_when_made_and_null_when_not(ns, env, module):
    result, status = put(ns, module, body(mode="enforce", policy_revision="CHG-7"))
    assert status == 200 and result["stored"]["allow_development_runs"] is None
    assert result["effective"]["allow_development_runs"] is False       # not chosen under enforce: off
    assert module.governance_state.effective.allow_development_runs is False
    result, status = put(ns, module, body(mode="enforce", policy_revision="CHG-7", allow_development_runs=True,
                                          expected_revision=1, change_note="let developers test"))
    assert status == 200 and result["stored"]["allow_development_runs"] is True
    assert result["effective"]["allow_development_runs"] is True
    assert module.governance_state.effective.allow_development_runs is True
    result, status = put(ns, module, body(expected_revision=2, change_note="back to measuring"))   # audit only, not chosen
    assert status == 200 and result["stored"]["allow_development_runs"] is None
    assert result["effective"]["allow_development_runs"] is True        # not chosen under audit only: on
    history_, _ = history(ns, module)
    assert [row["flags"]["allow_development_runs"] for row in history_["rows"]] == [None, True, None]
    assert put(ns, module, body(allow_development_runs="yes", expected_revision=3))[0]["code"] == "INVALID_REQUEST"


def drop_development_columns(ns, env):
    """The tables as they were before #7213 (SQLite 3.35+ drops a column)."""
    with env.get_session(None) as session:
        for table in ("credential_governance_settings", "credential_governance_settings_history"):
            session.execute(ns.sa.text(f"ALTER TABLE public.{table} DROP COLUMN allow_development_runs"))
        session.commit()
    ns.governance_settings._COLUMNS_SEEN.clear()  # pylint: disable=W0212


def test_a_table_from_before_the_column_reads_with_the_default_and_the_task_adds_the_column(ns, env, module):
    assert put(ns, module, body())[1] == 200
    drop_development_columns(ns, env)
    read, stored = ns.governance_settings.read_settings()
    assert read == "ok" and stored.mode == "audit_only" and stored.allow_development_runs is None
    result, status = get(ns, module)
    assert status == 200 and result["columns_present"] is False and result["effective"]["allow_development_runs"] is True
    result, status = put(ns, module, body(expected_revision=1, change_note="again"))
    assert (status, result["code"]) == (409, "MIGRATION_REQUIRED")
    assert ns.governance_settings.create_tables(dry_run=True) == {
        "created": [], "would_create": [], "added_columns": [],
        "would_add_columns": ["credential_governance_settings.allow_development_runs",
                              "credential_governance_settings_history.allow_development_runs"], "dry_run": True}
    assert ns.governance_settings.create_tables()["added_columns"] == [
        "credential_governance_settings.allow_development_runs",
        "credential_governance_settings_history.allow_development_runs"]
    assert ns.governance_settings.create_tables() == {"created": [], "would_create": [], "added_columns": [],
                                                      "would_add_columns": [], "dry_run": False}   # idempotent
    result, status = put(ns, module, body(expected_revision=1, change_note="again", allow_development_runs=False))
    assert status == 200 and result["columns_present"] is True and result["stored"]["allow_development_runs"] is False
    history_, _ = history(ns, module)
    assert [row["flags"]["allow_development_runs"] for row in history_["rows"]] == [False, None]
