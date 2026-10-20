"""C-1009 (#6946): the trusted release of credential material to a governed worker (``restricted_release_credential``).

Real models, statements, live checks (``validate_context``) and sealed transport; the secrets plugin's store read, the
facts methods and Redis are fakes that record their calls. See ``fixtures/admission_env.py``.
"""
import json
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, Credential, dispatch, dumped, run_to_running, sealed_tool_settings, seeded,
)


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("credential_release"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


class Run:
    """An admitted, sealed and validated run of one credential, and the requests its worker would make."""

    def __init__(self, ns, env, **over):
        self.ns, self.env = ns, env
        self.cred = seeded(env, **over)
        self.call, self.ref = dispatch(env, ns, self.cred)
        run_to_running(env, ns, self.ref)
        self.slot = dict(sealed_tool_settings(self.call)["credential_slot"])
        self.private, self.public = ns.keypair()
        env.denied.clear()
        env.recorded.clear()
        env.release_calls.clear()
        env.redis.calls.clear()
        env.log.lines.clear()

    def request(self, **over):
        values = dict(project_id=1, context_ref=self.ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
                      field="credentials", slot=dict(self.slot), recipient_key=self.public)
        values.update(over)
        return values

    def release(self, **over):
        return self.ns.credential_release.release_credential(self.env.module(), **self.request(**over))

    def aad(self, reply, **over):
        parts = dict(project="1", ref=self.ref, toolkit="1", field="credentials", decision=self.slot["decision_id"],
                     release=reply["release_id"])
        parts.update(over)
        return "|".join(("v1", parts["project"], parts["ref"], parts["toolkit"], parts["field"], parts["decision"],
                         parts["release"])).encode()

    def open(self, reply, key=None, **over):
        return json.loads(self.ns.transport.open_sealed(reply["sealed"], key or self.private, self.aad(reply, **over)))

    def row(self):
        [row] = self.env.query(1, self.ns.models.ExecutionSecurityContext)
        return row


@pytest.fixture
def run(ns, env):
    return Run(ns, env)


def denied(run, reply, code):
    """A refusal: the code, a fixed readable message, no sealed answer, no value anywhere, and an audit deny event."""
    env = run.env
    assert reply["ok"] is False and reply["reason_code"] == code and "sealed" not in reply
    assert reply["message"] == run.ns.adm.MESSAGES.get(code, run.ns.adm.GENERIC_MESSAGE)
    assert SECRET not in dumped(reply) and SECRET not in "\n".join(env.log.lines)
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("credential_acquired", "deny", code)
    assert SECRET not in dumped(event)
    return event


def unaudited(run, reply, code):
    """A request nobody admitted anything behind: refused with the code and counted, but no audit row is written."""
    env = run.env
    assert reply["ok"] is False and reply["reason_code"] == code and "sealed" not in reply
    assert env.denied == [] and env.recorded == [] and env.release_calls == []
    assert any(f"credential_release_total decision=rejected_request reason_code={code}" in line for line in env.log.lines)
    return reply


@pytest.fixture
def sealed(ns):
    """For the tests that seal or open a release: skipped when this run has no sealed transport."""
    ns.need_transport()


# --- an allowed release -----------------------------------------------------------------------

def test_a_release_is_sealed_to_the_requester_and_bound_to_its_context(ns, env, run, sealed):
    reply = run.release()
    assert reply["ok"] is True and reply["fields"] == ["token"] and set(reply) == {
        "ok", "release_id", "fields", "expires_at", "sealed"}
    body = run.open(reply)
    assert body["fields"] == {"token": f"{SECRET}-token"} and body["generation"] == 1
    assert body["expires_at"] == reply["expires_at"]
    assert SECRET not in dumped(reply)   # the reply carries it encrypted only
    # the store was read for exactly the slot's reference and generation, with the release id as its decision id
    [(project_id, ref_id, generation, release_id, actor_ref, allow_validated)] = env.release_calls
    assert (project_id, ref_id, generation, release_id, actor_ref, allow_validated) == (
        1, run.cred.ref_id, 1, reply["release_id"], "indexer_worker:indexer_agent", False)


def test_the_reply_opens_only_with_the_requesters_key_and_the_exact_binding(ns, env, run, sealed):
    reply = run.release()
    other_private, _ = ns.keypair()
    with pytest.raises(ns.transport.TransportError):
        run.open(reply, key=other_private)
    for over in ({"project": "2"}, {"ref": str(uuid.uuid4())}, {"toolkit": "2"}, {"field": "other"},
                 {"decision": str(uuid.uuid4())}, {"release": str(uuid.uuid4())}):
        with pytest.raises(ns.transport.TransportError):
            run.open(reply, **over)
    assert run.open(reply)["generation"] == 1


def test_each_release_has_its_own_id_and_its_own_ciphertext(ns, env, run, sealed):
    first, second = run.release(), run.release()
    assert first["release_id"] != second["release_id"] and first["sealed"]["ct"] != second["sealed"]["ct"]


def test_the_release_event_is_committed_with_the_decision_ids_and_no_value(ns, env, run, sealed):
    reply = run.release()
    [(session, event)] = env.recorded
    entry = run.row().allowed_bindings[0]
    assert event == {
        "event_type": "credential_acquired", "decision": "allow", "reason_code": "CREDENTIAL_RELEASED",
        "event_id": reply["release_id"], "tenant": "tid-1", "project_id": 1, "application_id": 7,
        "caller_ref": "indexer_worker:indexer_agent", "user_ref": "idp:tid-1:oid-42",
        "agent_binding_ref": str(run.cred.binding), "run_id": RUN, "attempt": 1, "toolkit_instance_id": "1",
        "credential_ref": run.cred.ref_id, "credential_generation": 1, "revocation_epoch": entry["reference_epoch"],
        "policy_revision": "rev-1"}
    [(_, writer)] = [item for item in env.sessions if item[1] is session]
    assert writer.commits == 1
    assert env.denied == [] and SECRET not in dumped(event)
    assert ns.credential_release.CREDENTIAL_RELEASE.total("allow", "CREDENTIAL_RELEASED") >= 1
    assert any("credential_release_total decision=allow reason_code=CREDENTIAL_RELEASED" in l for l in env.log.lines)


def test_the_expiry_is_the_contexts_or_five_minutes_whichever_is_sooner(ns, env, run, sealed):
    reply = run.release()
    from datetime import datetime
    expires = datetime.fromisoformat(reply["expires_at"])
    now = ns.adm._now()   # pylint: disable=W0212
    assert expires <= now + timedelta(seconds=301) and expires <= ns.adm._aware(run.row().expires_at)   # pylint: disable=W0212


def test_a_failing_audit_commit_means_no_reply_material(ns, env, run, sealed):
    env.record_raises = RuntimeError("outbox full")
    reply = run.release()
    denied(run, reply, "AUDIT_UNAVAILABLE")
    assert env.recorded == []


def test_the_audit_trail_being_unavailable_refuses_before_the_store_is_read_and_records_nothing(ns, env, run):
    env.audit_allowed = False
    unaudited(run, run.release(), "AUDIT_UNAVAILABLE")
    env.audit_allowed, env.audit_allowed_raises = True, True
    unaudited(run, run.release(), "AUDIT_UNAVAILABLE")


# --- the request ------------------------------------------------------------------------------

@pytest.mark.parametrize("name, over", [
    ("no project", {"project_id": None}), ("project as text", {"project_id": "1"}), ("project bool", {"project_id": True}),
    ("neither ref", {"context_ref": None}), ("both refs", {"probe_ref": str(uuid.uuid4())}),
    ("ref not a uuid", {"context_ref": "not-a-uuid"}),
    ("caller missing", {"caller": None}), ("caller kind", {"caller": {**CALLER, "kind": "browser"}}),
    ("caller task", {"caller": {**CALLER, "task_name": 5}}), ("caller run", {"caller": {**CALLER, "run_id": 7}}),
    ("toolkit", {"toolkit_instance_id": ""}), ("toolkit chars", {"toolkit_instance_id": "a|b"}),
    ("field", {"field": None}), ("field chars", {"field": "a b"}),
    ("slot missing", {"slot": None}), ("slot version", {"slot": {"v": 2}}),
    ("slot extra key", {"slot": {"v": 1, "extra": 1}}),
    ("recipient missing", {"recipient_key": None}), ("recipient not base64", {"recipient_key": "***"}),
    ("recipient short", {"recipient_key": "AAAA"}),
])
def test_a_request_that_is_not_well_formed_names_nothing_and_reads_nothing(ns, env, run, name, over):
    event_reply = run.release(**over)
    assert event_reply["ok"] is False and event_reply["reason_code"] == "STALE_CONTEXT", name
    assert env.release_calls == [] and env.redis.calls == [] and "sealed" not in event_reply
    assert env.denied == [] and env.recorded == []   # nothing was admitted behind it: counted, not audited


def test_slot_shapes_that_cannot_be_a_slot_are_stale(ns, env, run):
    for change in ({"fields": []}, {"fields": ["token", "token"]}, {"fields": ["a b"]}, {"fields": "token"},
                   {"fields": ["x"] * 33}, {"credential_generation": 0}, {"credential_generation": True},
                   {"credential_ref": "x"}, {"decision_id": None}, {"decision_id": "not-a-uuid"},
                   {"probe_ref": str(uuid.uuid4())}):
        reply = run.release(slot={**run.slot, **change})
        assert reply["reason_code"] == "STALE_CONTEXT", change
    assert env.release_calls == []


def test_the_handler_never_raises_whatever_it_is_given(ns, env):
    module = env.module()
    for garbage in ({}, {"project_id": []}, {"slot": []}, {"caller": 1}, {"project_id": 1, "context_ref": {}}):
        assert ns.credential_release.release_credential(module, **garbage)["ok"] is False
    assert ns.credential_release.release_credential(module, unknown_extra=1)["ok"] is False


# --- the context ------------------------------------------------------------------------------

def test_an_unknown_context_is_stale_and_not_audited(ns, env, run):
    unaudited(run, run.release(context_ref=str(uuid.uuid4())), "STALE_CONTEXT")


def test_a_context_the_worker_has_not_validated_is_not_released_to(ns, env):
    other = Run.__new__(Run)
    other.ns, other.env = ns, env
    other.cred = seeded(env)
    call, ref = dispatch(env, ns, other.cred)   # admitted, never validated: still "admitted"
    other.ref, other.slot = ref, dict(sealed_tool_settings(call)["credential_slot"])
    other.private, other.public = ns.keypair()
    env.denied.clear()
    denied(other, other.release(), "STALE_CONTEXT")
    assert env.release_calls == []


@pytest.mark.parametrize("state, reason, code", [
    ("completed", None, "STALE_CONTEXT"), ("revoked", None, "STALE_CONTEXT"),
    ("revoked", "NOT_DISPATCHED", "STALE_CONTEXT"), ("revoked", "EPOCH_MISMATCH", "EPOCH_MISMATCH"),
    ("revoked", "DENIED_GRANT", "DENIED_GRANT"), ("revoked", "CREDENTIAL_REFERENCE_INACTIVE",
                                                  "CREDENTIAL_REFERENCE_INACTIVE"),
])
def test_a_context_that_ended_is_refused_with_the_reason_it_ended(ns, env, run, state, reason, code):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.ExecutionSecurityContext).values(state=state, revoked_reason=reason))
        session.commit()
    denied(run, run.release(), code)
    assert env.release_calls == []


def test_an_expired_context_is_stale_at_the_boundary(ns, env, run, sealed):
    expires = ns.adm._aware(run.row().expires_at)   # pylint: disable=W0212
    ns.adm._now = lambda: expires - timedelta(seconds=1)   # pylint: disable=W0212
    assert run.release()["ok"] is True
    env.denied.clear()
    ns.adm._now = lambda: expires   # pylint: disable=W0212
    denied(run, run.release(), "STALE_CONTEXT")


@pytest.mark.parametrize("caller", [
    {**CALLER, "task_name": "indexer_validator"}, {**CALLER, "run_id": "run-2"}, {**CALLER, "run_id": None},
    {**CALLER, "task_name": "indexer_governed_check_connection"},
])
def test_only_the_worker_of_this_run_is_released_to(ns, env, run, caller):
    denied(run, run.release(caller=caller), "STALE_CONTEXT")
    assert env.release_calls == [] and run.row().state == "running"   # another run's guess does not end this run


def test_another_project_is_not_this_context(ns, env, run):
    unaudited(run, run.release(project_id=2), "STALE_CONTEXT")


def test_a_schema_or_profile_it_does_not_know_is_stale(ns, env, run):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.ExecutionSecurityContext).values(schema_version=2))
        session.commit()
    denied(run, run.release(), "STALE_CONTEXT")


# --- the use the context was admitted for ------------------------------------------------------

@pytest.mark.parametrize("name, over", [
    ("other toolkit", {"toolkit_instance_id": "2"}), ("other field", {"field": "other"}),
])
def test_a_use_the_context_was_not_admitted_for_is_refused(ns, env, run, name, over):
    denied(run, run.release(**over), "CREDENTIAL_REFERENCE_REQUIRED")
    assert env.release_calls == [] and env.redis.calls == []   # and it did not count against the context


@pytest.mark.parametrize("change", [
    {"decision_id": str(uuid.uuid4())}, {"credential_ref": str(uuid.uuid4())}, {"credential_generation": 2},
])
def test_a_slot_that_is_not_the_decisions_is_refused(ns, env, run, change):
    denied(run, run.release(slot={**run.slot, **change}), "CREDENTIAL_REFERENCE_REQUIRED")
    assert env.release_calls == []


def test_a_slot_asking_for_other_fields_than_it_was_sealed_with_is_still_this_decision(ns, env, run):
    # the fields are not part of the decision: the store decides what exists, and nothing absent is released
    reply = run.release(slot={**run.slot, "fields": ["token", "missing"]})
    denied(run, reply, "STORE_NOT_FOUND")


# --- a bounded number of releases per use --------------------------------------------------------

def test_the_seventeenth_release_of_a_use_is_refused(ns, env, run, sealed):
    limit = ns.credential_release.MAX_RELEASES_PER_FIELD
    assert limit == 16
    for _ in range(limit):
        assert run.release()["ok"] is True
    env.denied.clear()
    denied(run, run.release(), "RATE_LIMITED")
    assert len(env.release_calls) == limit   # the refused one did not read the store
    [counter] = [key for key in env.redis.ttls if key.startswith(ns.credential_release.COUNTER_KEY)]
    assert counter == f"{ns.credential_release.COUNTER_KEY}{run.ref}|1|credentials"
    assert 0 < env.redis.ttls[counter] <= 3600


def test_each_use_of_a_context_has_its_own_budget(ns, env, sealed):
    first = seeded(env)
    second = Credential(env, first.binding, tool_id=2, config_id=6, field="other")
    call, ref = dispatch(env, ns, first, second)
    run_to_running(env, ns, ref)
    private, public = ns.keypair()
    slots = [sealed_tool_settings(call, 0)["credential_slot"], sealed_tool_settings(call, 1, "other")["credential_slot"]]

    def release(index, toolkit, field):
        return ns.credential_release.release_credential(
            env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER),
            toolkit_instance_id=toolkit, field=field, slot=dict(slots[index]), recipient_key=public)

    limit = ns.credential_release.MAX_RELEASES_PER_FIELD
    assert all(release(0, "1", "credentials")["ok"] for _ in range(limit))
    assert release(0, "1", "credentials")["reason_code"] == "RATE_LIMITED"
    assert release(1, "2", "other")["ok"] is True   # the fan-out's other use is not starved by this one


def test_the_counter_failing_is_a_dependency_refusal_not_a_pass(ns, env, run):
    env.redis.raises = RuntimeError("redis down")
    denied(run, run.release(), "DEPENDENCY_UNAVAILABLE")
    assert env.release_calls == []


# --- uuids in any other spelling are not this uuid (they would each get a counter of their own) ---

@pytest.mark.parametrize("spell", [
    lambda text: text.upper(), lambda text: text.replace("-", ""), lambda text: f"urn:uuid:{text}",
    lambda text: "{" + text + "}",
])
def test_a_uuid_spelled_any_other_way_names_nothing(ns, env, run, spell):
    for field in ("context_ref",):
        reply = run.release(**{field: spell(run.ref)})
        assert reply["reason_code"] == "STALE_CONTEXT"
    for key in ("credential_ref", "decision_id"):
        reply = run.release(slot={**run.slot, key: spell(run.slot[key])})
        assert reply["reason_code"] == "STALE_CONTEXT", key
    assert env.release_calls == [] and env.redis.calls == [] and env.denied == []


# --- the live checks of the admission (a refusal ends the run, as validate_context does) ----------

def test_a_user_whose_epoch_moved_stops_the_run_at_the_release(ns, env, run):
    env.epochs[42] = {"active": True, "user": 9, "token": 0}
    denied(run, run.release(), "EPOCH_MISMATCH")
    assert (run.row().state, run.row().revoked_reason) == ("revoked", "EPOCH_MISMATCH")
    assert env.release_calls == []


def test_a_suspended_binding_stops_the_run_at_the_release(ns, env, run):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).values(state="suspended"))
        session.commit()
    denied(run, run.release(), "DENIED_BINDING")
    assert run.row().state == "revoked"


def test_a_credential_suspended_after_admission_stops_the_run_before_the_store_is_read(ns, env, run):
    run.cred.reference["state"] = "suspended"
    denied(run, run.release(), "CREDENTIAL_REFERENCE_INACTIVE")
    assert (run.row().state, run.row().revoked_reason) == ("revoked", "CREDENTIAL_REFERENCE_INACTIVE")
    assert env.release_calls == []


def test_a_grant_withdrawn_after_admission_stops_the_run(ns, env, run):
    env.grants[(1, run.cred.key())]["state"] = "revoked"
    denied(run, run.release(), "DENIED_GRANT")
    assert run.row().revoked_reason == "DENIED_GRANT" and env.release_calls == []


def test_a_credential_regenerated_after_admission_stops_the_run(ns, env, run):
    run.cred.reference["generation"] = 2
    denied(run, run.release(), "CREDENTIAL_GENERATION_MISMATCH")
    assert run.row().revoked_reason == "CREDENTIAL_GENERATION_MISMATCH" and env.release_calls == []


def test_identity_that_cannot_be_read_is_unknown_not_revoked(ns, env, run):
    env.epochs_raise = True
    denied(run, run.release(), "DEPENDENCY_UNAVAILABLE")
    assert run.row().state == "running"


# --- the store read ---------------------------------------------------------------------------

@pytest.mark.parametrize("code", ["STORE_DENIED", "STORE_SEALED", "STORE_UNAVAILABLE", "STORE_TLS_INVALID",
                                  "STORE_NOT_FOUND", "STORE_VERSION_CONFLICT", "DEPENDENCY_UNAVAILABLE"])
def test_a_store_refusal_is_passed_on_with_its_fixed_text(ns, env, run, code):
    env.release_reply = {"ok": False, "reason_code": code}
    denied(run, run.release(), code)
    assert run.row().state == "running"   # a store outage does not revoke the run


@pytest.mark.parametrize("code", ["CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH"])
def test_a_reference_that_changed_between_the_recheck_and_the_read_ends_the_run(ns, env, run, code):
    env.release_reply = {"ok": False, "reason_code": code}
    denied(run, run.release(), code)
    assert (run.row().state, run.row().revoked_reason) == ("revoked", code)


def test_a_store_that_raises_or_answers_nonsense_is_a_dependency_refusal(ns, env, run):
    env.release_raises = RuntimeError("vault says " + SECRET)
    denied(run, run.release(), "DEPENDENCY_UNAVAILABLE")
    env.release_raises, env.release_reply = None, "nonsense"
    env.denied.clear()
    denied(run, run.release(), "DEPENDENCY_UNAVAILABLE")


def test_a_value_at_another_generation_than_the_slot_is_refused(ns, env, run):
    from types import SimpleNamespace
    from pydantic import SecretStr
    env.release_reply = {"ok": True, "value": SimpleNamespace(fields={"token": SecretStr(SECRET)}, generation=2)}
    denied(run, run.release(), "CREDENTIAL_GENERATION_MISMATCH")


def test_nothing_partial_is_released_when_a_requested_field_is_absent(ns, env, run):
    env.material[run.cred.ref_id] = {}
    denied(run, run.release(), "STORE_NOT_FOUND")
    assert env.recorded == []


def test_a_reply_that_cannot_be_sealed_is_a_refusal(ns, env, run):
    transport = sys.modules["shared_secret_stores_6946.transport"]   # the real one, or the stand-in that cannot seal
    original = transport.seal
    transport.seal = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sealing failed"))
    try:
        denied(run, run.release(), "DEPENDENCY_UNAVAILABLE")
    finally:
        transport.seal = original
    assert env.recorded == []


# --- nothing leaks ----------------------------------------------------------------------------

def test_no_value_reaches_a_log_an_event_a_metric_or_the_context_row(ns, env, run, sealed):
    assert run.release()["ok"] is True
    env.release_raises = RuntimeError(SECRET)
    run.release()
    env.release_raises = None
    for key in ("token",):
        env.material[run.cred.ref_id][key] = SECRET
    run.release()
    seen = dumped([event for _, event in env.recorded]) + dumped(env.denied) + "\n".join(env.log.lines)
    seen += dumped([(r.allowed_bindings, r.subject, r.revocation_epochs) for r in env.query(1, ns.models.ExecutionSecurityContext)])
    assert SECRET not in seen


# --- a probe's release ------------------------------------------------------------------------

PROBE_CALLER = {"kind": "indexer_worker", "task_name": "indexer_governed_check_connection", "run_id": None}


def probe_request(run, probe_ref=None, **over):
    probe_ref = probe_ref or str(uuid.uuid4())
    ticket = {"project_id": 1, "credential_ref": run.cred.ref_id, "credential_generation": 1,
              "task_name": "indexer_governed_check_connection", "user_id": 42}
    run.env.redis.values[ns_key(run) + probe_ref] = json.dumps(ticket)
    slot = {"v": 1, "credential_ref": run.cred.ref_id, "credential_generation": 1, "probe_ref": probe_ref,
            "fields": ["token"]}
    values = dict(project_id=1, context_ref=None, probe_ref=probe_ref, caller=dict(PROBE_CALLER),
                  toolkit_instance_id="probe", field="configuration", slot=slot, recipient_key=run.public)
    values.update(over)
    return values


def ns_key(run):
    return run.ns.credential_release.PROBE_KEY


def test_a_probe_ticket_releases_once_and_the_reply_is_bound_to_the_probe(ns, env, run, sealed):
    request = probe_request(run)
    reply = ns.credential_release.release_credential(env.module(), **request)
    assert reply["ok"] is True
    aad = "|".join(("v1", "1", request["probe_ref"], "probe", "configuration", "", reply["release_id"])).encode()
    assert json.loads(ns.transport.open_sealed(reply["sealed"], run.private, aad))["fields"] == {"token": f"{SECRET}-token"}
    assert env.release_calls[-1][5] is True   # a probe may read a validated reference
    [(_, event)] = env.recorded
    assert (event["event_type"], event["decision"], event["caller_ref"], event["user_ref"], event["toolkit_instance_id"]) == (
        "credential_acquired", "allow", "indexer_worker:indexer_governed_check_connection", "user:42", "probe")
    # replay: the ticket was consumed by the first release
    env.recorded.clear()
    again = ns.credential_release.release_credential(env.module(), **request)
    assert again["reason_code"] == "STALE_CONTEXT" and "sealed" not in again


@pytest.mark.parametrize("name, ticket_change, over", [
    ("other credential", {"credential_ref": str(uuid.uuid4())}, {}),
    ("other generation", {"credential_generation": 2}, {}),
    ("other project", {"project_id": 2}, {}),
    ("other task", {"task_name": "indexer_agent"}, {}),
    ("no user", {"user_id": None}, {}),
])
def test_a_ticket_for_another_probe_is_refused_and_still_consumed(ns, env, run, name, ticket_change, over):
    request = probe_request(run)
    key = ns_key(run) + request["probe_ref"]
    ticket = json.loads(env.redis.values[key])
    env.redis.values[key] = json.dumps({**ticket, **ticket_change})
    reply = ns.credential_release.release_credential(env.module(), **request)
    assert reply["reason_code"] == "STALE_CONTEXT", name
    assert key not in env.redis.values and env.release_calls == []


@pytest.mark.parametrize("over", [
    {"toolkit_instance_id": "1"}, {"field": "credentials"},
    {"caller": {**PROBE_CALLER, "task_name": "indexer_agent"}}, {"caller": {**PROBE_CALLER, "run_id": "run-1"}},
])
def test_a_probe_request_must_be_the_probe_shape(ns, env, run, over):
    reply = ns.credential_release.release_credential(env.module(), **probe_request(run, **over))
    assert reply["reason_code"] == "STALE_CONTEXT" and env.release_calls == []


def test_an_unknown_ticket_and_a_failing_redis_are_refused(ns, env, run):
    request = probe_request(run)
    env.redis.values.clear()
    assert ns.credential_release.release_credential(env.module(), **request)["reason_code"] == "STALE_CONTEXT"
    env.redis.raises = RuntimeError("down")
    assert ns.credential_release.release_credential(env.module(), **probe_request(run))["reason_code"] == "DEPENDENCY_UNAVAILABLE"


def test_a_probe_cannot_use_a_run_slot_and_a_run_cannot_use_a_probe_ticket(ns, env, run):
    request = probe_request(run, context_ref=run.ref, probe_ref=None)
    assert ns.credential_release.release_credential(env.module(), **request)["ok"] is False
    swapped = run.request(probe_ref=str(uuid.uuid4()), context_ref=None, slot={**run.slot, "probe_ref": "x"})
    assert ns.credential_release.release_credential(env.module(), **swapped)["ok"] is False


# --- the ticket of a probe is single-use even under a race ---------------------------------------

def test_two_workers_racing_for_one_ticket_cannot_both_get_past_it(ns, env, run):
    import threading
    request = probe_request(run)
    replies, barrier = [], threading.Barrier(8)

    def ask():
        barrier.wait()
        replies.append(ns.credential_release.release_credential(env.module(), **request))

    threads = [threading.Thread(target=ask) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    past = [reply for reply in replies if reply.get("reason_code") != "STALE_CONTEXT"]
    assert len(replies) == 8 and len(past) == 1   # one got the ticket (and its release), seven found it spent
    assert len(env.release_calls) <= 1


def test_the_ticket_is_read_and_deleted_in_one_transaction(ns, env, run):
    request = probe_request(run)
    ns.credential_release.release_credential(env.module(), **request)
    names = [call[0] for call in env.redis.calls]
    assert names[:2] == ["get", "delete"]   # the pipeline of the release: nothing between the read and the removal


def test_an_unknown_ticket_is_not_audited_but_a_ticket_for_another_probe_is(ns, env, run):
    request = probe_request(run)
    env.redis.values.clear()
    unaudited(run, ns.credential_release.release_credential(env.module(), **request), "STALE_CONTEXT")
    request = probe_request(run)
    key = ns_key(run) + request["probe_ref"]
    env.redis.values[key] = json.dumps({**json.loads(env.redis.values[key]), "credential_generation": 2})
    reply = ns.credential_release.release_credential(env.module(), **request)
    assert reply["reason_code"] == "STALE_CONTEXT"
    [event] = env.denied
    assert (event["event_type"], event["decision"]) == ("credential_acquired", "deny")


# --- the cost of a release ---------------------------------------------------------------------

def test_a_release_is_one_read_transaction_one_audit_transaction_one_secrets_call_and_one_counter(ns, env, run, sealed):
    statements = []
    engine = env.engine(1)
    ns.sa.event.listen(engine, "before_cursor_execute", lambda conn, cursor, statement, *a: statements.append(statement))
    sessions, redis_calls = len(env.sessions), len(env.redis.calls)
    secrets_facts = len(env.secrets_calls)
    assert run.release()["ok"] is True
    opened = env.sessions[sessions:]
    assert len(opened) == 2                         # the read, and the audit event
    assert sum(item[1].commits for item in opened) == 1   # only the audit commits: the live checks write nothing
    selects = [text for text in statements if text.lstrip().upper().startswith("SELECT")]
    assert len(selects) == 2                        # the context row and its binding
    assert not [text for text in statements if text.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))]
    assert len(env.release_calls) == 1              # one secrets call, which is the one store read
    assert len(env.redis.calls) - redis_calls == 2  # the counter (INCR + EXPIRE, one round trip)
    assert len(env.secrets_calls) - secrets_facts == 2   # the recheck of this one use: its reference, its grant


# --- a fan-out: the parent stops, the children and the reconcile run keep its context -------------

def test_a_child_cannot_release_under_the_completed_context_of_its_parked_parent_until_it_is_reopened(ns, env, run, sealed):
    """The finding: ``task_status_changed`` completes the context of a stopped task before the parallel dispatch launches
    the children, who run under the same context and the same run id. They fail ``validate_context`` (and so would a
    release) until the context is reopened for them."""
    module = env.module()
    module._context_by_task["parent-task"] = (1, run.ref)
    ns.adm.complete_context(module, "parent-task")
    assert run.row().state == "completed"
    assert ns.adm.validate_context(module, 1, run.ref, CALLER)["reason_code"] == "STALE_CONTEXT"
    env.denied.clear()
    denied(run, run.release(), "STALE_CONTEXT")
    assert ns.adm.reopen_contexts(module, 1, [run.ref, run.ref, None]) == 1
    assert run.row().state == "running"
    env.denied.clear()
    assert ns.adm.validate_context(module, 1, run.ref, CALLER)["ok"] is True
    assert run.release()["ok"] is True


@pytest.mark.parametrize("state, reason", [("revoked", "DENIED_GRANT"), ("admitted", None), ("running", None)])
def test_only_a_completed_context_is_reopened(ns, env, run, state, reason):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.ExecutionSecurityContext).values(state=state, revoked_reason=reason))
        session.commit()
    assert ns.adm.reopen_contexts(env.module(), 1, [run.ref]) == 0
    assert run.row().state == state and run.row().revoked_reason == reason


def test_an_expired_context_is_not_reopened_and_flag_off_nothing_is(ns, env, run):
    module = env.module()
    module._context_by_task["parent-task"] = (1, run.ref)
    ns.adm.complete_context(module, "parent-task")
    expires = ns.adm._aware(run.row().expires_at)   # pylint: disable=W0212
    ns.adm._now = lambda: expires   # pylint: disable=W0212
    assert ns.adm.reopen_contexts(module, 1, [run.ref]) == 0
    ns.adm._now = ns.real_now   # pylint: disable=W0212
    env.poison()
    assert ns.adm.reopen_contexts(env.module(governed=False), 1, [run.ref]) == 0
    env.unpoison()
    assert run.row().state == "completed"
