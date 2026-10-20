"""C-1009 (#6756): governed run admission - request from the trusted caller, the AND sequence, the
context row and its audit event in one transaction, and the flag-off guarantee.

The real models, admission code and statements run against SQLite files (see
``fixtures/admission_env.py``); only the Pylon runtime, the identity verifier and the audit methods
are fakes.
"""
import copy
import inspect
import json
import sys
import textwrap
import types
from collections import OrderedDict
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.admission_env import principal  # noqa: E402  pylint: disable=C0413

COOKIE = "cookie-secret-value"
RUN = "run-1"
UNSET = object()


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("admission"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


def contexts(env, ns, project_id=1):
    return env.query(project_id, ns.models.ExecutionSecurityContext)


def admit(env, ns, module=None, run_id=RUN, **request):
    return ns.adm.admit_execution(module or env.module(), env.request(**request), run_id=run_id)


def denied_code(env, ns, **kwargs):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(env, ns, **kwargs)
    return err.value


def verify_rejection(code="IDENTITY_SIGNATURE_INVALID"):
    return {"ok": False, "reason_code": code, "message": "fixed text"}


# --- settings ---------------------------------------------------------------------------------

def config(**over):
    section = {"enabled": True, "policy_revision": "rev-1", **over}
    return {"enterprise_credential_governance": section}


def test_defaults_when_the_section_is_absent_or_governance_is_off(ns):
    defaults = ns.adm.AdmissionSettings()
    assert (defaults.allow_runtime_domain_binding, defaults.admit_legacy_pat, defaults.context_ttl_s,
            defaults.environment) == (False, False, 3600, None)
    assert ns.adm.load_admission_settings({}) == defaults
    assert ns.adm.load_admission_settings({"enterprise_credential_governance": None}) == defaults
    # nothing is validated while the flag is off, so a half-edited section cannot stop a legacy platform
    junk = {"enterprise_credential_governance": {"enabled": False, "context_ttl_s": "soon", "admit_legacy_pat": 5}}
    assert ns.adm.load_admission_settings(junk) == defaults


def test_enabled_settings_are_read(ns):
    settings = ns.adm.load_admission_settings(config(
        allow_runtime_domain_binding=True, admit_legacy_pat=True, context_ttl_s=60, environment="prod-eu"))
    assert settings == ns.adm.AdmissionSettings(True, True, 60, "prod-eu")
    assert ns.adm.load_admission_settings(config(context_ttl_s=86400)).context_ttl_s == 86400
    # config.yml quotes the ${ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT:} expansion: unset arrives as ""
    assert ns.adm.load_admission_settings(config(environment="")).environment is None


@pytest.mark.parametrize("over", [
    {"context_ttl_s": 59}, {"context_ttl_s": 86401}, {"context_ttl_s": "3600"}, {"context_ttl_s": True},
    {"context_ttl_s": 60.5}, {"allow_runtime_domain_binding": "yes"}, {"admit_legacy_pat": 1},
    {"environment": " "}, {"environment": "x" * 65}, {"environment": 7},
])
def test_invalid_settings_fail_startup_when_enabled(ns, over):
    with pytest.raises(ValueError):
        ns.adm.load_admission_settings(config(**over))


def test_flag_off_startup_check_runs_nothing(ns, env):
    env.poison()
    ns.adm.check_governed_startup(env.module(governed=False))


def test_governed_startup_passes_when_the_platform_can_prove_and_record(ns, env):
    ns.adm.check_governed_startup(env.module())


@pytest.mark.parametrize("break_it, expected", [
    (lambda env: setattr(env, "auth_mode", "local"), "auth_mode"),
    (lambda env: setattr(env, "verifier_configured", False), "identity_verifier"),
    (lambda env: setattr(env, "has_audit_method", False), "security_audit_record"),
])
def test_governed_startup_refuses_what_it_cannot_prove(ns, env, break_it, expected):
    break_it(env)
    ns.install_fakes(env)
    with pytest.raises(ValueError, match=expected):
        ns.adm.check_governed_startup(env.module())
    assert any(expected in line for line in env.log.lines)


def test_governed_startup_refuses_when_the_verifier_is_missing_altogether(ns, env):
    ns.tools.auth.identity_verifier = None  # an auth plugin that predates the verifier
    with pytest.raises(ValueError, match="identity_verifier"):
        ns.adm.check_governed_startup(env.module())


# --- the request is built from the trusted caller only ----------------------------------------

def request_from(ns, env, module=None, **over):
    values = dict(sid="sid-1", project_id=1, binding_project_id=1, version_row=types.SimpleNamespace(id=70, application_id=7),
                  run_kind="interactive", entry_point="chat_sio")
    values.update(over)
    return ns.pd_admission.admission_request_from(module or env.module(), **values)


def holder(kind="user", ident="42", reference=COOKIE):
    return types.SimpleNamespace(type=kind, id=ident, reference=reference)


def test_flag_off_builds_no_request_and_reads_nothing(ns, env):
    env.poison()
    assert request_from(ns, env, env.module(governed=False)) is None


def test_a_connection_holder_becomes_the_caller(ns, env):
    env.sio_users["sid-1"] = holder()
    request = request_from(ns, env)
    assert (request.caller_type, request.caller_id) == ("user", 42)
    assert (request.application_id, request.version_id) == (7, 70)  # from the row, not from any payload
    assert request.caller_reference.get_secret_value() == COOKIE


def test_the_session_reference_never_reaches_a_dump_or_a_repr(ns, env):
    env.sio_users["sid-1"] = holder()
    request = request_from(ns, env)
    for text in (repr(request), str(request), request.model_dump_json(), json.dumps(request.model_dump(), default=str)):
        assert COOKIE not in text
    assert "caller_reference" not in request.model_dump()


def test_a_token_holder_is_a_token_caller_without_a_reference(ns, env):
    env.sio_users["sid-1"] = holder("token", "7", "ref")
    request = request_from(ns, env)
    assert (request.caller_type, request.caller_id, request.caller_reference) == ("token", 7, None)


@pytest.mark.parametrize("sio_holder", [
    holder("public", "-"), holder("user", "-"), holder("user", None), holder("system", "1"), None,
])
def test_anything_else_is_the_public_caller(ns, env, sio_holder):
    if sio_holder is not None:
        env.sio_users["sid-1"] = sio_holder
    request = request_from(ns, env)
    assert (request.caller_type, request.caller_id, request.caller_reference) == ("public", None, None)


def test_an_http_caller_comes_from_the_request_when_there_is_no_sid(ns, env):
    env.in_request = True
    env.g.auth = holder("user", "43", "http-cookie")
    request = request_from(ns, env, sid=None, entry_point="predict_api")
    assert (request.caller_type, request.caller_id, request.entry_point) == ("user", 43, "predict_api")


def test_no_sid_and_no_http_request_is_the_public_caller(ns, env):
    env.g.auth = holder("user", "43")
    assert request_from(ns, env, sid=None).caller_type == "public"  # a cross-pylon call carries no proof


def test_a_missing_version_row_gives_no_ids(ns, env):
    env.sio_users["sid-1"] = holder()
    request = request_from(ns, env, version_row=None)
    assert (request.application_id, request.version_id) == (None, None)


def test_the_request_has_no_slot_for_client_supplied_identity(ns, env):
    parameters = inspect.signature(ns.pd_admission.admission_request_from).parameters
    assert not {"user_id", "user_context", "agent_id", "application_id", "version_id"} & set(parameters)
    with pytest.raises(Exception):
        env.request(user_context={"user_id": 1})
    with pytest.raises(Exception):
        env.request(user_id=1)


# --- the AND sequence -------------------------------------------------------------------------

def seed(env):
    env.seed_agent()


def test_all_steps_pass_one_row_and_one_allow_event_in_the_same_session(ns, env):
    seed(env)
    view = admit(env, ns)
    rows = contexts(env, ns)
    assert len(rows) == 1 and str(rows[0].id) == view.context_ref
    row = rows[0]
    assert (row.state, row.attempt, row.run_id, row.schema_version, row.profile) == ("admitted", 1, RUN, 1, "governed")
    assert (row.project_id, row.binding_project_id, row.application_id, row.version_id) == (1, 1, 7, 70)
    assert row.caller == {"kind": "user", "ref": "user:42", "user_id": 42}
    assert row.subject == {"kind": "delegation", "ref": "idp:tid-1:oid-42", "delegation_ref": "uti-42"}
    assert row.revocation_epochs == {"user": 3, "token": 0, "binding": 0, "project": None, "credential": None}
    assert (row.tenant, row.policy_revision, row.entry_point) == ("tid-1", "rev-1", "chat_sio")
    assert row.definition_digest == env.digests[70] and str(row.agent_binding_ref) == view.agent_binding_ref
    # one allow event, written through the very session that inserted the row, committed once
    [(event_session, event)] = env.recorded
    [(_, write_session)] = [entry for entry in env.sessions if entry[1] is event_session]
    assert write_session.commits == 1
    assert event == {
        "event_type": "admission", "decision": "allow", "reason_code": "ADMITTED", "tenant": "tid-1",
        "project_id": 1, "application_id": 7, "caller_ref": "user:42", "user_ref": "idp:tid-1:oid-42",
        "agent_binding_ref": view.agent_binding_ref, "run_id": RUN, "attempt": 1, "policy_revision": "rev-1",
        "revocation_epoch": 0,
    }
    assert env.denied == []


def test_expiry_follows_the_configured_ttl(ns, env):
    seed(env)
    view = admit(env, ns, module=env.module(context_ttl_s=120, environment="prod"))
    delta = (view.expires_at - ns.adm._now()).total_seconds()  # pylint: disable=W0212
    assert 100 < delta <= 120
    assert contexts(env, ns)[0].environment == "prod"
    assert env.recorded[0][1]["environment"] == "prod"


def test_verification_gets_the_session_reference_and_only_admitted_kinds(ns, env):
    seed(env)
    admit(env, ns)
    [(evidence, kinds)] = env.verify_calls
    assert evidence == {"kind": "session_user", "user_id": 42, "auth_reference": COOKIE}
    assert kinds == frozenset({"session_user"})


FAILING_STEPS = [
    ("audit backlog", "AUDIT_UNAVAILABLE", lambda env: setattr(env, "audit_allowed", False), {}),
    ("audit gate raises", "AUDIT_UNAVAILABLE", lambda env: setattr(env, "audit_allowed_raises", True), {}),
    ("system run", "MANDATE_REQUIRED", lambda env: None, {"run_kind": "system"}),
    ("public caller", "IDENTITY_EVIDENCE_MISSING", lambda env: None, {"caller_type": "public", "caller_id": None,
                                                                      "caller_reference": None}),
    ("verifier rejects", "IDENTITY_SIGNATURE_INVALID",
     lambda env: setattr(env, "verify_result", verify_rejection("IDENTITY_SIGNATURE_INVALID")), {}),
    ("identity revoked", "IDENTITY_REVOKED",
     lambda env: setattr(env, "verify_result", verify_rejection("IDENTITY_REVOKED")), {}),
    ("verifier unreachable", "DEPENDENCY_UNAVAILABLE",
     lambda env: setattr(env, "verify_raises", RuntimeError("auth down")), {}),
    ("verifier answers nonsense", "DEPENDENCY_UNAVAILABLE", lambda env: setattr(env, "verify_result", "ok"), {}),
    ("verifier code is not a code", "DEPENDENCY_UNAVAILABLE",
     lambda env: setattr(env, "verify_result", verify_rejection("tok.en with spaces")), {}),
    ("verified for another user", "IDENTITY_SUBJECT_UNLINKED",
     lambda env: setattr(env, "verify_result", {"ok": True, "principal": principal(43)}), {}),
    ("asserted user", "MANDATE_REQUIRED",
     lambda env: setattr(env, "verify_result", {"ok": True, "principal": principal(42, verified=False)}), {}),
    ("no tenant in the evidence", "IDENTITY_EVIDENCE_MISSING",
     lambda env: setattr(env, "verify_result", {"ok": True, "principal": principal(42, tid=None)}), {}),
    ("no version", "DENIED_BINDING", lambda env: None, {"version_id": None, "application_id": None}),
    ("version does not exist", "DENIED_BINDING", lambda env: None, {"version_id": 99}),
    ("version of another application", "DENIED_BINDING", lambda env: None, {"application_id": 8}),
    ("no active binding", "DENIED_BINDING", lambda env: _set_binding_state(env, "suspended"), {}),
    ("digest differs", "BINDING_DIGEST_MISMATCH", lambda env: env.digests.update({70: "adv1:" + "f" * 64}), {}),
    ("definition vanished", "DENIED_BINDING", lambda env: env.digests.pop(70), {}),
    ("audit write fails", "DEPENDENCY_UNAVAILABLE", lambda env: setattr(env, "record_raises", RuntimeError("full")), {}),
    ("commit fails", "DEPENDENCY_UNAVAILABLE", lambda env: setattr(env, "fail_commit_once", RuntimeError("io")), {}),
]


def _set_binding_state(env, state):
    with env.ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(env.ns.sa.update(env.ns.models.AgentIdentityBinding).values(state=state))
        session.commit()


@pytest.mark.parametrize("name, code, break_it, request_over", FAILING_STEPS, ids=[s[0] for s in FAILING_STEPS])
def test_each_step_failing_alone_denies_with_its_code_and_leaves_no_row(ns, env, name, code, break_it, request_over):
    seed(env)
    break_it(env)
    before = ns.adm.ADMISSION_DECISION.total("deny", code)
    error = denied_code(env, ns, **request_over)
    assert (error.reason_code, error.message) == (code, ns.adm.MESSAGES[code])
    assert contexts(env, ns) == []
    # audited once on its own transaction, counted once, logged without the cookie
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("admission", "deny", code)
    assert event["project_id"] == 1 and event["run_id"] == RUN
    assert ns.adm.ADMISSION_DECISION.total("deny", code) == before + 1
    assert f"security_metric name=admission_decision_total decision=deny reason_code={code}" in env.log.lines
    assert not [line for line in env.log.lines if COOKIE in line]
    assert COOKIE not in json.dumps(event, default=str) and COOKIE not in error.message


def test_a_failed_audit_write_rolls_the_row_back(ns, env):
    seed(env)
    env.record_raises = RuntimeError("outbox full")
    denied_code(env, ns)
    assert contexts(env, ns) == []
    assert env.recorded == []


def test_a_denied_run_whose_audit_cannot_be_written_stays_denied(ns, env):
    seed(env)
    env.audit_allowed = False
    env.autocommit_raises = RuntimeError("audit down")
    assert denied_code(env, ns).reason_code == "AUDIT_UNAVAILABLE"
    assert contexts(env, ns) == []
    assert any("AUDIT_UNAVAILABLE" in line and "was not recorded" in line for line in env.log.lines)


def test_the_steps_run_in_order_and_stop_at_the_first_failure(ns, env):
    seed(env)
    # audit comes before everything: an unavailable audit trail never reaches the verifier
    env.audit_allowed = False
    denied_code(env, ns, run_kind="system")
    assert env.verify_calls == []
    # a system run is refused before identity is verified
    env.audit_allowed = True
    assert denied_code(env, ns, run_kind="system").reason_code == "MANDATE_REQUIRED"
    assert env.verify_calls == []
    # identity comes before the binding lookup
    env.verify_result = verify_rejection("IDENTITY_EXPIRED")
    assert denied_code(env, ns).reason_code == "IDENTITY_EXPIRED"
    assert env.digest_calls == 0


def test_a_legacy_token_is_refused_by_the_verifier_unless_the_operator_admits_it(ns, env):
    seed(env)
    env.verify_result = verify_rejection("IDENTITY_KIND_NOT_ADMITTED")
    error = denied_code(env, ns, caller_type="token", caller_id=7, caller_reference=None)
    assert error.reason_code == "IDENTITY_KIND_NOT_ADMITTED"
    [(evidence, kinds)] = env.verify_calls
    assert evidence == {"kind": "legacy_pat", "token_id": 7} and kinds == frozenset({"session_user"})


def test_a_legacy_token_still_needs_a_mandate_even_when_admitted(ns, env):
    seed(env)
    env.verify_result = {"ok": True, "principal": principal(42, verified=False)}
    module = env.module(admit_legacy_pat=True)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(module, env.request(caller_type="token", caller_id=7, caller_reference=None),
                               run_id=RUN)
    assert err.value.reason_code == "MANDATE_REQUIRED"
    assert env.verify_calls[0][1] == frozenset({"session_user", "legacy_pat"})
    assert contexts(env, ns) == []


def test_two_active_bindings_of_one_version_are_ambiguous(ns, env):
    seed(env)
    env.add_binding(1, 7, 70, mode="dedicated_agent")
    assert denied_code(env, ns).reason_code == "DENIED_BINDING"


def test_a_runtime_domain_binding_counts_only_with_the_explicit_allow(ns, env):
    env.add_version(1, 70, 7)
    env.add_binding(1, 7, 70, mode="runtime_domain_binding")
    assert denied_code(env, ns).reason_code == "DENIED_BINDING"
    view = admit(env, ns, module=env.module(allow_runtime_domain_binding=True))
    assert view.context_ref
    # and it never makes a second binding ambiguous when it is not allowed
    env.reset()
    env.seed_agent()
    env.add_binding(1, 7, 70, mode="runtime_domain_binding")
    assert admit(env, ns).context_ref


def test_a_binding_of_another_version_or_project_is_not_used(ns, env):
    env.add_version(1, 70, 7)
    env.add_version(1, 71, 7)
    env.digests[71] = "adv1:" + "7" * 64
    env.add_binding(1, 7, 71)  # a different version of the same agent
    assert denied_code(env, ns).reason_code == "DENIED_BINDING"
    env.add_binding(2, 7, 70)  # the right ids, in another project's schema
    assert denied_code(env, ns).reason_code == "DENIED_BINDING"


def test_a_public_agent_binds_in_its_own_project_and_the_row_lands_in_the_run_project(ns, env):
    env.seed_agent(project_id=9)
    view = admit(env, ns, project_id=3, binding_project_id=9)
    [row] = contexts(env, ns, project_id=3)
    assert str(row.id) == view.context_ref and (row.project_id, row.binding_project_id) == (3, 9)
    assert contexts(env, ns, project_id=9) == []


def test_each_admission_of_one_run_is_a_new_attempt(ns, env):
    seed(env)
    admit(env, ns)
    admit(env, ns)
    other = admit(env, ns, run_id="run-2")
    assert sorted(row.attempt for row in contexts(env, ns) if row.run_id == RUN) == [1, 2]
    assert other.attempt == 1


def test_a_concurrent_duplicate_attempt_reads_the_maximum_again(ns, env):
    seed(env)
    admit(env, ns)
    env.fail_commit_once = ns.sa.exc.IntegrityError("INSERT", {}, Exception("duplicate attempt"))
    view = admit(env, ns)
    assert view.attempt == 2
    assert sorted(row.attempt for row in contexts(env, ns)) == [1, 2]


def test_nothing_secret_reaches_the_row_the_events_or_the_view(ns, env):
    seed(env)
    view = admit(env, ns)
    [row] = contexts(env, ns)
    dumped = json.dumps({column.name: getattr(row, column.name) for column in row.__table__.columns}, default=str)
    dumped += json.dumps(env.recorded[0][1], default=str) + view.model_dump_json() + "\n".join(env.log.lines)
    assert COOKIE not in dumped
    assert "eyJ" not in dumped


def test_the_audit_event_matches_the_tracing_schema(ns, env):
    seed(env)
    admit(env, ns)  # the audit fake validates every event against SecurityAuditEventV1
    denied_code(env, ns, run_kind="system")
    assert len(env.recorded) == 1 and len(env.denied) == 1


# --- two users x two agents -------------------------------------------------------------------

def two_users_two_agents(ns, env):
    env.add_version(1, 70, 7)
    env.add_version(1, 80, 8)
    a1 = env.add_binding(1, 7, 70)
    a2 = env.add_binding(1, 8, 80)
    refs = {}
    for user in (42, 43):
        env.verify_result = {"ok": True, "principal": principal(user)}
        for app, version in ((7, 70), (8, 80)):
            refs[(user, app)] = admit(env, ns, run_id=f"run-{user}-{app}", caller_id=user, application_id=app,
                                      version_id=version)
    return a1, a2, refs


def test_four_runs_get_four_contexts_each_with_exactly_its_user_and_binding(ns, env):
    a1, a2, refs = two_users_two_agents(ns, env)
    rows = {str(row.id): row for row in contexts(env, ns)}
    assert len(rows) == 4
    for (user, app), view in refs.items():
        row = rows[view.context_ref]
        assert row.caller["user_id"] == user and row.run_id == f"run-{user}-{app}"
        assert row.application_id == app and row.agent_binding_ref == (a1 if app == 7 else a2)
        assert row.subject["ref"] == f"idp:tid-1:oid-{user}"


def test_suspending_one_binding_revokes_only_its_live_contexts(ns, env):
    a1, _, refs = two_users_two_agents(ns, env)
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.adm.revoke_binding_contexts(session, a1) == 2
        session.commit()
    states = {(row.caller["user_id"], row.application_id): (row.state, row.revoked_reason)
              for row in contexts(env, ns)}
    assert states[(42, 7)] == states[(43, 7)] == ("revoked", "EPOCH_MISMATCH")
    assert states[(42, 8)] == states[(43, 8)] == ("admitted", None)
    # a second pass has nothing live left to revoke
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.adm.revoke_binding_contexts(session, a1) == 0


def test_the_same_user_running_the_same_agent_twice_gets_two_contexts_with_distinct_runs(ns, env):
    seed(env)
    first = admit(env, ns, run_id="run-a")
    second = admit(env, ns, run_id="run-b")
    assert first.context_ref != second.context_ref
    assert {row.run_id for row in contexts(env, ns)} == {"run-a", "run-b"}


def test_a_client_application_id_cannot_borrow_another_agents_binding(ns, env):
    env.add_version(1, 70, 7)
    env.add_version(1, 80, 8)
    env.add_binding(1, 8, 80)  # the other agent is approved, version 70 is not
    # the client names agent 8 but version 70: the version row says agent 7
    assert denied_code(env, ns, application_id=8, version_id=70).reason_code == "DENIED_BINDING"
    assert contexts(env, ns) == []


def test_user_id_and_user_context_in_the_payload_do_not_choose_the_caller(ns, env):
    # the only identity in the request is the holder of the connection; its id is what the row records
    seed(env)
    env.sio_users["sid-1"] = holder("user", "42")
    request = request_from(ns, env)
    view = ns.adm.admit_execution(env.module(), request, run_id=RUN)
    assert contexts(env, ns)[0].caller["user_id"] == 42 and view.context_ref
    # a payload claiming user 43 cannot be expressed: the verifier is asked about user 42 only
    assert env.verify_calls[0][0]["user_id"] == 42


# --- dispatch ---------------------------------------------------------------------------------

def call_kwargs(run_id=RUN, envelope="governed", **extra):
    payload = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": run_id}
    if envelope == "governed":
        payload["envelope"] = {"schema_version": 1, "security_profile": "governed",
                               "required_capabilities": ["task_envelope.v1"], "context_ref": None}
    elif envelope == "legacy":
        payload["envelope"] = {"schema_version": 1, "security_profile": "legacy", "required_capabilities": [],
                               "context_ref": None}
    payload.update(extra)
    return {"args": ["s", "m"], "kwargs": payload, "pool": "agents", "meta": {"project_id": 1}}


@pytest.mark.parametrize("with_request", [False, True])
def test_flag_off_dispatch_changes_nothing_and_calls_nothing(ns, env, with_request):
    env.poison()
    call = call_kwargs(envelope="legacy")
    before = copy.deepcopy(call)
    request = env.request() if with_request else None
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", call, request) is None
    assert call == before


def test_tasks_that_are_not_admitted_pass_untouched_even_when_governed(ns, env):
    env.poison()
    call = call_kwargs()
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(), "indexer_validator", call, None) is None
    assert call == before


def test_dispatch_admits_and_puts_the_context_ref_in_the_envelope_and_the_meta(ns, env):
    seed(env)
    call = call_kwargs()
    call["meta"]["traceparent"] = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    context_ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    [row] = contexts(env, ns)
    assert context_ref == str(row.id)
    assert call["kwargs"]["envelope"]["context_ref"] == context_ref and call["meta"]["context_ref"] == context_ref
    assert row.run_id == RUN and row.trace_ref == call["meta"]["traceparent"]
    assert env.recorded[0][1]["traceparent"] == row.trace_ref
    assert set(call["kwargs"]["envelope"]) == {"schema_version", "security_profile", "required_capabilities",
                                                 "context_ref"}


def test_a_child_or_reconcile_dispatch_inherits_the_parents_context_without_a_new_admission(ns, env):
    env.poison()
    call = call_kwargs()
    call["kwargs"]["envelope"]["context_ref"] = "11111111-1111-1111-1111-111111111111"
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", call, None) is None
    assert call["meta"]["context_ref"] == "11111111-1111-1111-1111-111111111111"
    assert call["kwargs"]["envelope"]["context_ref"] == "11111111-1111-1111-1111-111111111111"


def test_a_governed_dispatch_without_an_admission_request_is_refused_and_audited(ns, env):
    seed(env)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(), None)
    assert err.value.reason_code == "IDENTITY_EVIDENCE_MISSING"
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["project_id"], event["run_id"]) == (
        "deny", "IDENTITY_EVIDENCE_MISSING", 1, RUN)
    assert contexts(env, ns) == []


@pytest.mark.parametrize("envelope", ["legacy", None])
def test_a_governed_dispatch_that_cannot_carry_a_context_is_refused(ns, env, envelope):
    seed(env)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(envelope=envelope), env.request())
    assert err.value.reason_code == "STALE_CONTEXT"
    assert contexts(env, ns) == []


def test_a_denied_dispatch_leaves_the_call_unmarked(ns, env):
    seed(env)
    env.verify_result = verify_rejection()
    call = call_kwargs()
    with pytest.raises(ns.exceptions.AdmissionDenied):
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert call["kwargs"]["envelope"]["context_ref"] is None and "context_ref" not in call["meta"]


# --- the start_task wrapper in module.py ------------------------------------------------------

def build_wrapper(ns, env, module, original):
    """The real ``_maintenance_gated_start_task`` source from module.py, run against the real gates."""
    source = (PLUGIN_ROOT / "module.py").read_text()
    text = source[source.index("def _maintenance_gated_start_task"):
                  source.index("self.task_node.start_task = _maintenance_gated_start_task")]
    text = textwrap.dedent("        " + text).replace("from .utils.", f"from {admission_env.PKG}.utils.")
    gate = types.ModuleType(f"{admission_env.PKG}.utils.maintenance_gate")
    gate.is_maintenance_active = lambda: False
    budget = types.ModuleType(f"{admission_env.PKG}.utils.budget_door")
    budget.closed_budget_scope = lambda *a, **k: None
    budget.dispatch_may_use_llm = lambda kwargs: False
    budget.dispatch_owner = lambda kwargs: (None, None)
    budget.dispatch_uses_own_model = lambda *a, **k: False
    sys.modules[gate.__name__], sys.modules[budget.__name__] = gate, budget
    module.worker_capabilities = types.SimpleNamespace(has_capable_worker=lambda required: True)
    namespace = {
        "self": module, "log": ns.log, "_original_start_task": original,
        "apply_envelope_gate": ns.caps.apply_envelope_gate, "stamp_predict_run_id": ns.run_id.stamp_predict_run_id,
        "admit_dispatch": ns.adm.admit_dispatch, "track_context": ns.adm.track_context,
    }
    exec(compile(text, "module.py", "exec"), namespace)  # pylint: disable=W0122
    return namespace["_maintenance_gated_start_task"]


class Original:
    def __init__(self, task_id="task-1"):
        self.calls, self.task_id = [], task_id

    def __call__(self, *args, **kwargs):
        self.calls.append((args, copy.deepcopy(kwargs)))
        return self.task_id


def dispatch_args():
    return ("indexer_agent",), {"args": ["thread", 5], "kwargs": {"project_id": 1, "message_id": 5},
                                "pool": "agents", "meta": {"project_id": 1, "task_name": "indexer_agent"}}


def test_flag_off_the_wrapper_output_is_identical_with_or_without_a_request(ns, env):
    outputs = []
    for admission in (UNSET, None, env.request()):
        original = Original()
        wrapper = build_wrapper(ns, env, env.module(governed=False), original)
        args, kwargs = dispatch_args()
        if admission is not UNSET:
            kwargs["admission"] = admission
        env.poison()
        assert wrapper(*args, **kwargs) == "task-1"
        env.unpoison()
        outputs.append(original.calls)
    assert outputs[0] == outputs[1] == outputs[2]
    [(args, kwargs)] = outputs[0]
    assert "admission" not in kwargs and kwargs["kwargs"]["envelope"]["security_profile"] == "legacy"
    assert kwargs["kwargs"]["envelope"]["context_ref"] is None and "context_ref" not in kwargs["meta"]
    assert json.dumps(outputs[0], sort_keys=True) == json.dumps(outputs[2], sort_keys=True)


def test_governed_the_wrapper_admits_then_dispatches_and_tracks_the_task(ns, env):
    seed(env)
    original, module = Original("task-9"), env.module()
    wrapper = build_wrapper(ns, env, module, original)
    args, kwargs = dispatch_args()
    assert wrapper(*args, admission=env.request(), **kwargs) == "task-9"
    [(_, sent)] = original.calls
    [row] = contexts(env, ns)
    assert "admission" not in sent
    assert sent["kwargs"]["envelope"]["security_profile"] == "governed"
    assert sent["kwargs"]["envelope"]["context_ref"] == str(row.id) == sent["meta"]["context_ref"]
    assert row.run_id == sent["kwargs"]["_elitea_predict_run_id"]
    assert module._context_by_task == OrderedDict({"task-9": (1, str(row.id))})  # pylint: disable=W0212
    # the task stops: the context is closed
    ns.adm.complete_context(module, "task-9")
    assert contexts(env, ns)[0].state == "completed" and contexts(env, ns)[0].ended_at is not None
    assert module._context_by_task == OrderedDict()  # pylint: disable=W0212


def test_governed_a_refused_admission_never_reaches_the_worker(ns, env):
    seed(env)
    env.verify_result = verify_rejection("IDENTITY_REVOKED")
    original = Original()
    wrapper = build_wrapper(ns, env, env.module(), original)
    args, kwargs = dispatch_args()
    with pytest.raises(ns.exceptions.AdmissionDenied):
        wrapper(*args, admission=env.request(), **kwargs)
    with pytest.raises(ns.exceptions.AdmissionDenied):
        wrapper(*args, **kwargs)  # an entry point that built no request
    assert original.calls == [] and contexts(env, ns) == []


def test_governed_a_task_that_did_not_start_revokes_its_context_as_not_dispatched(ns, env):
    seed(env)
    module = env.module()
    wrapper = build_wrapper(ns, env, module, Original(task_id=None))
    args, kwargs = dispatch_args()
    assert wrapper(*args, admission=env.request(), **kwargs) is None
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")
    assert module._context_by_task == OrderedDict()  # pylint: disable=W0212


def test_the_wrapper_pops_admission_first_and_admits_between_stamping_and_dispatch():
    source = (PLUGIN_ROOT / "module.py").read_text()
    wrapper = source[source.index("def _maintenance_gated_start_task"):
                     source.index("self.task_node.start_task = _maintenance_gated_start_task")]
    lines = [line.strip() for line in wrapper.splitlines()[1:3]]
    assert lines[0].startswith('admission = kwargs.pop("admission", None)')
    order = [wrapper.index(marker) for marker in (
        "apply_envelope_gate(", "stamp_predict_run_id(kwargs)", "admit_dispatch(self,", "_original_start_task(*args, **kwargs)",
        "track_context(self, task_id, admission.project_id, context_ref)")]
    assert order == sorted(order)
    eval_wrapper = source[source.index("def _maintenance_gated_eval_start_task"):source.index("self.eval_task_node.start_task = ")]
    assert "admit" not in eval_wrapper  # the eval node runs locally in Main


# --- hooks at the entry points ----------------------------------------------------------------

def source_of(relative):
    return (PLUGIN_ROOT / relative).read_text()


def test_predict_sio_builds_the_request_from_the_loaded_row_and_translates_a_denial():
    text = source_of("rpc/application.py")
    chat, llm = text[text.index("def predict_sio("):text.index("def predict_sio_llm(")], text[text.index("def predict_sio_llm("):]
    assert "admission_version_row = application_version" in chat
    assert 'entry_point="chat_sio"' in chat and "version_row=admission_version_row" in chat
    assert "binding_project_id=application_project_id" in chat
    assert 'run_kind="system" if is_system_user else "interactive"' in chat
    assert "admission=admission," in chat
    assert chat.index("except AdmissionDenied as denied:") < chat.index("except MaintenanceInProgressError:")
    assert 'error={"code": denied.reason_code, "error": denied.message}' in chat
    assert 'entry_point="chat_sio_llm"' in llm and "version_row=None" in llm and "admission=admission," in llm
    assert "except AdmissionDenied as denied:" in llm
    # no client-supplied identity feeds the request
    for part in (chat, llm):
        call = part[part.index("admission_request_from("):part.index(")\n", part.index("admission_request_from("))]
        assert "user_id=" not in call and "user_context" not in call and "data[" not in call


def test_do_predict_builds_the_http_request_and_webhooks_are_system_runs():
    text = source_of("methods/predict.py")
    assert 'entry_point="predict_api"' in text and "sid=None" in text and "version_row=application_version" in text
    assert 'run_kind="system" if webhook_signature is not None else "interactive"' in text
    assert "admission=admission," in text and "binding_project_id=project_id" in text
    for route in ("api/v2/predict.py", "api/v2/webhook.py"):
        route_text = source_of(route)
        assert 'return {"code": e.reason_code, "error": e.message}, 403' in route_text


def test_pipeline_run_is_untouched_so_a_governed_ad_hoc_pipeline_has_no_admission():
    assert "admission" not in source_of("methods/pipeline_run.py")


def test_task_completion_closes_the_context_for_governed_runs_only():
    text = source_of("methods/task_callbacks.py")
    assert text.index('if status != "stopped":') < text.index("complete_context(self, task_id)")
    assert text.index("complete_context(self, task_id)") < text.index("self._maybe_handle_parallel_dispatch(task_id)")


def test_module_registers_and_unregisters_the_worker_bus_handler_and_the_admin_task():
    text = source_of("module.py")
    assert text.count('name="restricted_validate_execution_context"') == 2
    assert "rpc_node.register(\n                    self.validate_execution_context," in text
    assert "rpc_node.unregister(\n                        self.validate_execution_context," in text
    assert text.index("self._ensure_agent_identity_schema()\n        check_governed_startup(self)") > 0
    for call in ("register_admin_task", "unregister_admin_task"):
        assert f'{call}(\n                "manage_agent_identity_binding", self.manage_agent_identity_binding' in text
    assert '"manage_agent_identity_binding", self.manage_agent_identity_binding,\n                group="R-2.0.8"' in text
    assert "self.admission_settings = load_admission_settings(self.descriptor.config)" in text
    assert "self._context_by_task = OrderedDict()" in text


def test_the_worker_bus_handler_delegates_to_validate_context():
    text = source_of("methods/restricted.py")
    assert "def validate_execution_context(self, project_id, context_ref, caller):" in text
    assert "return validate_context(self, project_id, context_ref, caller)" in text


# --- counters, the bound on the completion map, jwt ------------------------------------------

def test_every_decision_is_counted_once_with_its_labels(ns, env):
    seed(env)
    allow, deny = ns.adm.ADMISSION_DECISION.total("allow", "ADMITTED"), ns.adm.ADMISSION_DECISION.total("deny", "MANDATE_REQUIRED")
    admit(env, ns)
    denied_code(env, ns, run_kind="system")
    assert ns.adm.ADMISSION_DECISION.total("allow", "ADMITTED") == allow + 1
    assert ns.adm.ADMISSION_DECISION.total("deny", "MANDATE_REQUIRED") == deny + 1
    assert env.log.lines == [
        "security_metric name=admission_decision_total decision=allow reason_code=ADMITTED",
        "security_metric name=admission_decision_total decision=deny reason_code=MANDATE_REQUIRED",
    ]


def test_the_completion_map_is_bounded_and_drops_the_oldest(ns, env, monkeypatch):
    monkeypatch.setattr(ns.adm, "MAX_TRACKED_CONTEXTS", 3)
    module = env.module()
    for index in range(5):
        ns.adm.track_context(module, f"task-{index}", 1, f"ctx-{index}")
    assert list(module._context_by_task) == ["task-2", "task-3", "task-4"]  # pylint: disable=W0212


def test_completion_of_an_unknown_task_or_with_the_flag_off_does_nothing(ns, env):
    ns.adm.complete_context(env.module(), "never-tracked")
    module = env.module(governed=False)
    module._context_by_task["task-1"] = (1, "x")  # pylint: disable=W0212
    env.poison()
    ns.adm.complete_context(module, "task-1")
    assert "task-1" in module._context_by_task  # pylint: disable=W0212


def test_jwt_is_never_imported_by_the_governed_code(ns):
    assert "jwt" not in sys.modules
