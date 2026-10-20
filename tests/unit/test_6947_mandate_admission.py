"""C-1009 (#6947, WP-1C PR C): admission of an autonomous (scheduled / webhook) run under a mandate, the credential
decision for a mandate subject, the re-check at the worker, the time limit and the owner event.

Real models and statements on SQLite files (see ``fixtures/admission_env.py`` and ``fixtures/mandate_env.py``);
only the Pylon runtime, the identity service and the audit methods are fakes.
"""
import re
import sys
import threading
import time
import types
import uuid
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER, add_mandate, mandate_row  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
CALLER = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN}
TRIGGER = "schedule:version:70"
OWNER_REF = "idp:tid-1:oid-5"
TITLE = "github-main-credential"
UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-")
NEW_CODES = ("MANDATE_INACTIVE", "MANDATE_EXPIRED", "RUNTIME_LIMIT_EXCEEDED")


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_admission"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    return mandate_env.prepare(ns, ns.env)


@pytest.fixture
def agent(ns, env):
    """The agent (project 1, app 7, version 70) with its active binding and an active mandate of owner 5."""
    binding = env.seed_agent()
    mandate = add_mandate(env, ns, state="active", binding_ref=binding, operations=("run:schedule", "run:webhook"))
    return types.SimpleNamespace(binding=binding, mandate=mandate)


def request(ns, mandate, **over):
    values = dict(project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="mandate",
                  entry_point="schedule", caller_type="trigger", mandate_ref=str(mandate), trigger_ref=TRIGGER)
    values.update(over)
    return ns.AdmissionRequest(**values)


def admit(ns, env, agent, run_id=RUN, payload=None, module=None, **over):
    return ns.adm.admit_execution(module or env.module(), request(ns, agent.mandate, **over), run_id=run_id,
                                  payload=payload)


def refusal(ns, env, agent, **kwargs):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(ns, env, agent, **kwargs)
    return err.value


def contexts(env, ns, project_id=1):
    return env.query(project_id, ns.models.ExecutionSecurityContext)


def update(env, model, where, project_id=1, **values):
    with env.ns.sa_orm.Session(env.engine(project_id)) as session:
        session.execute(env.ns.sa.update(model).where(where).values(**values))
        session.commit()


def update_mandate(env, ns, mandate, project_id=1, **values):
    update(env, ns.Mandate, ns.Mandate.id == mandate, project_id=project_id, **values)


# --- the request and the messages ---------------------------------------------------------------

def test_the_request_carries_the_mandate_and_the_trigger_and_nothing_secret(ns, env, agent):
    asked = request(ns, agent.mandate)
    assert (asked.run_kind, asked.entry_point, asked.caller_type, asked.caller_id) == ("mandate", "schedule", "trigger", None)
    assert asked.mandate_ref == str(agent.mandate) and asked.trigger_ref == TRIGGER and asked.caller_reference is None
    for field, value in (("run_kind", "robot"), ("entry_point", "cron"), ("caller_type", "robot")):
        with pytest.raises(Exception):
            request(ns, agent.mandate, **{field: value})
    interactive = ns.AdmissionRequest(project_id=1, binding_project_id=1, run_kind="interactive", entry_point="chat_sio",
                                      caller_type="user", caller_id=1)
    assert interactive.mandate_ref is None and interactive.trigger_ref is None


def test_the_new_codes_have_fixed_readable_messages_without_ids_and_revoke_a_context(ns):
    texts = {code: ns.adm.MESSAGES[code] for code in (*NEW_CODES, "AUTHORIZATION_REQUIRED", "DENIED_OPERATION",
                                                     "MANDATE_REQUIRED")}
    assert texts["MANDATE_REQUIRED"] == "No approved mandate or delegation covers this run; it was not started."
    assert texts["MANDATE_INACTIVE"] == ("The mandate for this autonomous run is suspended or revoked; "
                                         "the run was not started.")
    assert texts["MANDATE_EXPIRED"] == "The mandate for this autonomous run has expired; renew it to run again."
    assert texts["RUNTIME_LIMIT_EXCEEDED"] == "The run exceeded the time its mandate allows and was stopped."
    assert texts["DENIED_OPERATION"] == "The mandate does not allow this kind of trigger; the run was not started."
    assert texts["AUTHORIZATION_REQUIRED"].startswith("This schedule runs on a user's behalf")
    for message in texts.values():
        assert not UUID_PATTERN.search(message) and not re.search(r"\d", message)
    assert set(NEW_CODES) <= ns.adm.REVOKE_CODES


# --- allow --------------------------------------------------------------------------------------

def test_an_active_mandate_is_admitted_with_one_context_and_one_commit(ns, env, agent):
    view = admit(ns, env, agent)
    [row] = contexts(env, ns)
    assert str(row.id) == view.context_ref and row.state == "admitted" and row.attempt == 1
    assert (row.entry_point, row.run_id, row.version_id, row.application_id) == ("schedule", RUN, 70, 7)
    assert row.caller == {"kind": "trigger", "ref": TRIGGER, "user_id": OWNER}
    assert row.subject["kind"] == "mandate" and row.subject["ref"] == f"mandate:{agent.mandate}"
    assert row.subject["owner"] == OWNER_REF and row.subject["delegation_ref"] is None
    assert row.subject["evidence"] == {"type": "asserted_user", "issuer": "elitea:mandate", "subject": OWNER_REF,
                                       "mandate_ref": str(agent.mandate)}
    assert row.revocation_epochs["mandate"] == 0 and row.revocation_epochs["user"] == 1
    assert row.revocation_epochs["token"] is None and row.revocation_epochs["binding"] == 0
    assert row.tenant == "tid-1" and str(row.agent_binding_ref) == str(agent.binding)
    assert sum(session.commits for _, session in env.sessions) == 1
    [(_, event)] = env.recorded
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("admission", "allow", "ADMITTED")
    assert event["mandate_ref"] == str(agent.mandate) and event["user_ref"] == OWNER_REF
    assert event["caller_ref"] == TRIGGER and event["run_id"] == RUN
    assert env.verify_calls == [] and env.denied == []   # nobody is verified: the mandate is the authority


@pytest.mark.parametrize("entry_point, operation", [("schedule", "run:schedule"), ("webhook", "run:webhook")])
def test_each_trigger_kind_needs_its_own_operation(ns, env, entry_point, operation):
    binding = env.seed_agent()
    mandate = add_mandate(env, ns, state="active", binding_ref=binding, operations=(operation,))
    agent = types.SimpleNamespace(mandate=mandate)
    assert admit(ns, env, agent, entry_point=entry_point).run_id == RUN
    other = "webhook" if entry_point == "schedule" else "schedule"
    assert refusal(ns, env, agent, entry_point=other, run_id="run-2").reason_code == "DENIED_OPERATION"


def test_the_context_expires_at_the_nearest_of_the_ttl_the_runtime_limit_and_the_validity(ns, env):
    cases = (
        ({}, 600),                                                                    # max_runtime_s 600 < ttl 3600
        ({"runtime": 7200}, 3600),                                                    # the context ttl
        ({"runtime": 7200, "valid_until": NOW + timedelta(seconds=90)}, 90),          # the validity window
    )
    for index, (over, seconds) in enumerate(cases):
        mandate_env.prepare(ns, env)   # a clean harness for each case
        binding = env.seed_agent()
        agent = types.SimpleNamespace(mandate=add_mandate(env, ns, state="active", binding_ref=binding, **over))
        view = admit(ns, env, agent, module=env.module(context_ttl_s=3600), run_id=f"run-{index}")
        assert view.expires_at == NOW + timedelta(seconds=seconds), over


def test_two_firings_are_two_runs_and_two_contexts(ns, env, agent):
    first, second = admit(ns, env, agent, run_id="run-1"), admit(ns, env, agent, run_id="run-2")
    assert first.context_ref != second.context_ref and {row.run_id for row in contexts(env, ns)} == {"run-1", "run-2"}
    assert len([event for _, event in env.recorded if event["event_type"] == "admission"]) == 2


# --- refusal matrix -----------------------------------------------------------------------------

def deny_event(env):
    [event] = env.denied
    return event


def set_state(state, **more):
    return lambda ns, env, agent: update_mandate(env, ns, agent.mandate, state=state, **more)


REFUSALS = {
    "draft": (set_state("draft", approved_by=None, approval_ref=None, policy_revision=None), "MANDATE_INACTIVE"),
    "pending": (set_state("pending_approval", approved_by=None, approval_ref=None, policy_revision=None),
                "MANDATE_INACTIVE"),
    "suspended": (set_state("suspended", suspended_reason="MANUAL"), "MANDATE_INACTIVE"),
    "revoked": (set_state("revoked"), "MANDATE_INACTIVE"),
    "expired": (set_state("expired"), "MANDATE_EXPIRED"),
    "other_agent_version": (lambda ns, env, agent: update_mandate(env, ns, agent.mandate, agent_version=71),
                            "MANDATE_REQUIRED"),
    "other_application": (lambda ns, env, agent: update_mandate(env, ns, agent.mandate, application_id=8),
                          "MANDATE_REQUIRED"),
    "operation_not_allowed": (lambda ns, env, agent: update_mandate(env, ns, agent.mandate,
                                                                   allowed_operations=["run:webhook"]),
                              "DENIED_OPERATION"),
    "binding_replaced": (lambda ns, env, agent: update_mandate(env, ns, agent.mandate, agent_binding_ref=uuid.uuid4()),
                         "DENIED_BINDING"),
    "binding_retired": (lambda ns, env, agent: update(env, env.ns.models.AgentIdentityBinding,
                                                      env.ns.models.AgentIdentityBinding.id == agent.binding,
                                                      state="retired"), "DENIED_BINDING"),
    "definition_changed": (lambda ns, env, agent: env.digests.__setitem__(70, "adv1:" + "f" * 64),
                           "BINDING_DIGEST_MISMATCH"),
    "owner_unlinked": (lambda ns, env, agent: env.idp_links.clear(), "IDENTITY_SUBJECT_UNLINKED"),
    "owner_linked_to_another_user": (lambda ns, env, agent: env.idp_links.update(
        {("https://issuer.example", "tid-1", "oid-5"): {"id": 6}}), "IDENTITY_SUBJECT_UNLINKED"),
    "owner_link_unreadable": (lambda ns, env, agent: setattr(env, "idp_raises", RuntimeError("auth down")),
                              "DEPENDENCY_UNAVAILABLE"),
    "owner_verdict_unreadable": (lambda ns, env, agent: env.epochs.pop(OWNER), "DEPENDENCY_UNAVAILABLE"),
    "owner_lost_predict_permission": (lambda ns, env, agent: env.permissions.update({OWNER: {"credentials.use"}}),
                                      "DENIED_RBAC"),
    "permissions_unreadable": (lambda ns, env, agent: setattr(env, "permissions_raise", RuntimeError("down")),
                               "DEPENDENCY_UNAVAILABLE"),
    "audit_unavailable": (lambda ns, env, agent: setattr(env, "audit_allowed", False), "AUDIT_UNAVAILABLE"),
}


@pytest.mark.parametrize("name", sorted(REFUSALS))
def test_a_mandate_run_that_does_not_hold_is_refused_with_a_fixed_message_and_an_audited_deny(ns, env, agent, name):
    change, code = REFUSALS[name]
    change(ns, env, agent)
    error = refusal(ns, env, agent)
    assert error.reason_code == code and error.message == ns.adm.MESSAGES[code]
    assert contexts(env, ns) == [] and env.recorded == []
    event = deny_event(env)
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("admission", "deny", code)
    assert event["caller_ref"] == TRIGGER and event["mandate_ref"] == str(agent.mandate)
    assert f"security_metric name=admission_decision_total decision=deny reason_code={code}" in env.log.lines
    assert not UUID_PATTERN.search(error.message) and env.verify_calls == []


def test_a_mandate_the_table_does_not_hold_is_refused_as_required(ns, env, agent):
    assert refusal(ns, env, agent, mandate_ref=str(uuid.uuid4())).reason_code == "MANDATE_REQUIRED"
    assert refusal(ns, env, agent, mandate_ref="not-a-uuid", run_id="run-2").reason_code == "MANDATE_REQUIRED"
    assert refusal(ns, env, agent, mandate_ref=None, run_id="run-3").reason_code == "MANDATE_REQUIRED"


@pytest.mark.parametrize("over", [
    {"caller_type": "user", "caller_id": 5}, {"caller_type": "public"}, {"trigger_ref": None},
    {"entry_point": "chat_sio"}, {"entry_point": "predict_api"}, {"version_id": None}, {"application_id": None},
])
def test_a_request_that_is_not_a_trigger_request_is_not_a_mandate_run(ns, env, agent, over):
    assert refusal(ns, env, agent, **over).reason_code == "MANDATE_REQUIRED"
    assert contexts(env, ns) == []


def test_a_system_run_without_a_mandate_is_still_refused_as_required(ns, env, agent):
    asked = ns.AdmissionRequest(project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="system",
                                entry_point="chat_sio", caller_type="public")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(env.module(), asked, run_id=RUN)
    assert err.value.reason_code == "MANDATE_REQUIRED"


def test_a_mandate_run_without_a_run_id_is_not_admitted(ns, env, agent):
    assert refusal(ns, env, agent, run_id=None).reason_code == "DEPENDENCY_UNAVAILABLE"


def test_an_overdue_mandate_is_expired_lazily_with_its_runs_revoked_and_the_event_recorded(ns, env, agent):
    admit(ns, env, agent, run_id="run-0")
    update_mandate(env, ns, agent.mandate, valid_until=NOW - timedelta(seconds=1))
    assert refusal(ns, env, agent).reason_code == "MANDATE_EXPIRED"
    row = mandate_row(env, ns, agent.mandate)
    assert (row.state, row.revocation_epoch, row.state_changed_by) == ("expired", 1, None)
    [context] = contexts(env, ns)
    assert (context.state, context.revoked_reason) == ("revoked", "MANDATE_EXPIRED")
    [(_, event)] = env.recorded[-1:]
    assert (event["event_type"], event["reason_code"], event["mandate_ref"]) == ("lifecycle", "MANDATE_EXPIRED",
                                                                                 str(agent.mandate))
    assert event["revocation_epoch"] == 1
    # once expired, the next firing is refused as expired without writing again
    env.recorded.clear()
    assert refusal(ns, env, agent, run_id="run-2").reason_code == "MANDATE_EXPIRED" and env.recorded == []


def test_the_validity_boundary_is_the_instant_itself(ns, env, agent):
    update_mandate(env, ns, agent.mandate, valid_until=NOW + timedelta(seconds=1))
    assert admit(ns, env, agent).run_id == RUN
    update_mandate(env, ns, agent.mandate, valid_until=NOW)
    assert refusal(ns, env, agent, run_id="run-2").reason_code == "MANDATE_EXPIRED"


def test_an_expiry_that_cannot_be_written_still_refuses_the_run(ns, env, agent):
    update_mandate(env, ns, agent.mandate, valid_until=NOW - timedelta(seconds=1))
    env.record_raises = RuntimeError("outbox down")
    assert refusal(ns, env, agent).reason_code == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, agent.mandate).state == "active"   # rolled back with its event; the next check retries


def test_an_inactive_owner_suspends_the_mandate_and_revokes_its_live_runs(ns, env, agent):
    admit(ns, env, agent, run_id="run-0")
    env.epochs[OWNER]["active"] = False
    assert refusal(ns, env, agent).reason_code == "MANDATE_INACTIVE"
    row = mandate_row(env, ns, agent.mandate)
    assert (row.state, row.suspended_reason, row.revocation_epoch) == ("suspended", "OWNER_SUSPENDED", 1)
    [context] = contexts(env, ns)
    assert (context.state, context.revoked_reason) == ("revoked", "MANDATE_INACTIVE")
    [(_, event)] = env.recorded[-1:]
    assert (event["event_type"], event["reason_code"]) == ("lifecycle", "MANDATE_OWNER_SUSPENDED")
    assert event["mandate_ref"] == str(agent.mandate) and event["revocation_epoch"] == 1
    # the next firing finds it suspended and writes nothing more
    env.recorded.clear()
    assert refusal(ns, env, agent, run_id="run-2").reason_code == "MANDATE_INACTIVE" and env.recorded == []


def test_a_role_change_of_the_owner_does_not_suspend_the_mandate(ns, env, agent):
    env.epochs[OWNER]["user"] = 9   # a role change bumps the epoch and leaves the user active
    assert admit(ns, env, agent).run_id == RUN
    assert mandate_row(env, ns, agent.mandate).state == "active"
    [row] = contexts(env, ns)
    assert row.revocation_epochs["user"] == 9


def test_the_other_owners_mandates_are_left_alone(ns, env, agent):
    other = add_mandate(env, ns, state="active", owner=6, version=71, application=8)
    env.epochs[OWNER]["active"] = False
    refusal(ns, env, agent)
    assert mandate_row(env, ns, other).state == "active"


@pytest.mark.parametrize("verdict", [{"user": 1}, {"user": 1, "active": None}, {"user": 1, "active": "yes"},
                                     {"user": 1, "active": 0}, {}])
def test_only_an_explicit_inactive_verdict_suspends_a_mandate_a_vague_one_is_unknown(ns, env, agent, verdict):
    env.epochs[OWNER] = verdict
    assert refusal(ns, env, agent).reason_code == "DEPENDENCY_UNAVAILABLE"
    assert mandate_row(env, ns, agent.mandate).state == "active" and env.recorded == []


def test_a_vague_owner_verdict_at_validation_is_unknown_not_a_revocation(ns, env, agent):
    ref = admitted(ns, env, agent)
    env.epochs[OWNER] = {"user": 1}
    assert validate(ns, env, ref)["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert mandate_row(env, ns, agent.mandate).state == "active" and context_row(env, ns).state == "admitted"


def test_an_owner_link_the_identity_service_refuses_as_malformed_names_nobody(ns, env, agent):
    env.idp_raises = ValueError("not a valid oid")
    assert refusal(ns, env, agent).reason_code == "IDENTITY_SUBJECT_UNLINKED"
    env.idp_raises = RuntimeError("auth down")
    assert refusal(ns, env, agent, run_id="run-2").reason_code == "DEPENDENCY_UNAVAILABLE"


# --- credentials for a mandate subject ----------------------------------------------------------

class MandateCredential:
    """One governed credential of a toolkit of the agent, its facts, and a grant keyed by ``grant_subject``."""

    def __init__(self, env, agent, *, grant_subject="mandate", ownership_class="project_shared", custodian=None,
                 grant_kind="auto", tool_id=1, field="credentials", config_id=5):
        self.uuid, self.ref_id, self.grant_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        self.tool_id, self.field, self.env, self.binding = tool_id, field, env, agent.binding
        env.add_toolkit(1, 70, tool_id, "github", {"url": "https://github.example", field: {
            "elitea_title": TITLE, "private": False, "configuration_uuid": self.uuid, "configuration_project_id": 1}})
        env.configurations[(1, self.uuid)] = {
            "id": config_id, "project_id": 1, "type": "github", "section": "credentials", "generation": 3}
        env.references.append({
            "project_id": 1, "configuration_id": config_id, "ref_id": self.ref_id, "state": "active", "generation": 1,
            "configuration_generation": 3, "store_kind": "database", "ownership_class": ownership_class,
            "revocation_epoch": 2, "custodian_user_id": custodian})
        if grant_subject is not None:
            subject = f"mandate:{agent.mandate}" if grant_subject == "mandate" else grant_subject
            grant = {"grant_id": self.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
                     "valid_until": None, "operations": ["*"]}
            if grant_kind != "auto":
                grant["subject_kind"] = grant_kind
            env.grants[(1, "|".join((str(agent.binding), str(tool_id), self.ref_id, "*", subject)))] = grant

    @property
    def payload(self):
        settings = {"url": "https://github.example", self.field: {
            "elitea_title": TITLE, "configuration_uuid": self.uuid, "configuration_project_id": 1,
            "configuration_type": "github", "configuration_generation": 3, "token": "plain-token-value"}}
        return {"application": {"version_details": {"tools": [{"id": self.tool_id, "type": "github",
                                                               "settings": settings}]}}}


def test_a_credential_granted_to_the_mandate_is_allowed_and_recorded_with_the_mandate(ns, env, agent):
    credential = MandateCredential(env, agent)
    admit(ns, env, agent, payload=credential.payload)
    [row] = contexts(env, ns)
    [entry] = row.allowed_bindings
    assert entry["credential_ref"] == credential.ref_id and entry["grant_id"] == credential.grant_id
    decisions = [event for _, event in env.recorded if event["event_type"] == "decision"]
    assert len(decisions) == 1 and decisions[0]["reason_code"] == "CREDENTIAL_USE_ALLOWED"
    assert decisions[0]["mandate_ref"] == str(agent.mandate) and decisions[0]["user_ref"] == OWNER_REF
    assert decisions[0]["caller_ref"] == TRIGGER
    assert env.permission_calls == [(OWNER, "default", 1)] * 2   # the predict right, then credentials.use


def test_a_grant_made_to_the_person_is_not_a_grant_to_the_mandate(ns, env, agent):
    credential = MandateCredential(env, agent, grant_subject=OWNER_REF)
    assert refusal(ns, env, agent, payload=credential.payload).reason_code == "DENIED_GRANT"
    assert contexts(env, ns) == []


@pytest.mark.parametrize("kind", ["user_delegation", None, "", "other"])
def test_a_grant_whose_kind_is_not_mandate_is_not_a_mandate_grant(ns, env, agent, kind):
    credential = MandateCredential(env, agent, grant_kind=kind)
    assert refusal(ns, env, agent, payload=credential.payload).reason_code == "DENIED_GRANT"


def test_an_interactive_grant_must_say_user_delegation(ns, env):
    binding = env.seed_agent()
    agent = types.SimpleNamespace(binding=binding, mandate=uuid.uuid4())
    credential = MandateCredential(env, agent, grant_subject="idp:tid-1:oid-42", grant_kind="mandate")
    asked = env.request()
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(env.module(), asked, run_id=RUN, payload=credential.payload)
    assert err.value.reason_code == "DENIED_GRANT"


def test_a_personal_credential_is_usable_by_a_mandate_only_when_the_owner_is_its_custodian(ns, env, agent):
    stranger = MandateCredential(env, agent, ownership_class="personal", custodian=6)
    assert refusal(ns, env, agent, payload=stranger.payload).reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"
    nobody = MandateCredential(env, agent, ownership_class="personal", custodian=None, tool_id=2, field="other",
                               config_id=6)
    assert refusal(ns, env, agent, payload=nobody.payload, run_id="run-2").reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_a_personal_credential_of_the_owner_is_allowed_for_the_mandate(ns, env, agent):
    credential = MandateCredential(env, agent, ownership_class="personal", custodian=OWNER)
    assert admit(ns, env, agent, payload=credential.payload).run_id == RUN


def test_a_personal_credential_is_still_refused_to_an_interactive_run_even_for_its_custodian(ns, env):
    binding = env.seed_agent()
    agent = types.SimpleNamespace(binding=binding, mandate=uuid.uuid4())
    credential = MandateCredential(env, agent, grant_subject="idp:tid-1:oid-42", ownership_class="personal",
                                   custodian=42)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(env.module(), env.request(), run_id=RUN, payload=credential.payload)
    assert err.value.reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_the_owner_must_hold_credentials_use_for_the_mandate_to_use_a_credential(ns, env, agent):
    credential = MandateCredential(env, agent)
    env.permissions[OWNER] = {"models.applications.predict.post"}
    assert refusal(ns, env, agent, payload=credential.payload).reason_code == "DENIED_RBAC"


def test_an_agent_without_credentials_does_not_ask_for_the_credentials_permission(ns, env, agent):
    env.permissions[OWNER] = {"models.applications.predict.post"}
    assert admit(ns, env, agent).run_id == RUN
    assert env.permission_calls == [(OWNER, "default", 1)]   # only the predict permission, once


def test_a_credential_refusal_names_no_secret_and_no_id_in_the_message(ns, env, agent):
    credential = MandateCredential(env, agent, grant_subject=None)
    error = refusal(ns, env, agent, payload=credential.payload)
    assert error.reason_code == "DENIED_GRANT"
    values = [error.message, str(error), *env.log.lines]
    for secret in (credential.uuid, TITLE, "plain-token-value", "https://github.example"):
        assert not [value for value in values if secret in value]


# --- validate_context for a mandate run ---------------------------------------------------------

def validate(ns, env, ref, caller=CALLER):
    return ns.adm.validate_context(env.module(), 1, ref, caller)


def admitted(ns, env, agent, **kwargs):
    view = admit(ns, env, agent, **kwargs)
    env.denied.clear()
    env.recorded.clear()
    env.log.lines.clear()
    return view.context_ref


def context_row(env, ns):
    [row] = contexts(env, ns)
    return row


def test_a_live_mandate_context_is_accepted_and_runs(ns, env, agent):
    ref = admitted(ns, env, agent)
    result = validate(ns, env, ref)
    assert result["ok"] is True and result["context"]["context_ref"] == ref
    assert context_row(env, ns).state == "running"


@pytest.mark.parametrize("state, extra, code", [
    ("suspended", {"suspended_reason": "MANUAL", "revocation_epoch": 1}, "MANDATE_INACTIVE"),
    ("revoked", {"revocation_epoch": 1}, "MANDATE_INACTIVE"),
    ("expired", {"revocation_epoch": 1}, "MANDATE_EXPIRED"),
])
def test_a_mandate_that_left_active_after_admission_revokes_the_run_with_its_code(ns, env, agent, state, extra, code):
    ref = admitted(ns, env, agent)
    update_mandate(env, ns, agent.mandate, state=state, **extra)
    result = validate(ns, env, ref)
    assert result == {"ok": False, "reason_code": code, "message": ns.adm.MESSAGES[code]}
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", code)
    event = deny_event(env)
    assert (event["event_type"], event["reason_code"], event["mandate_ref"]) == ("decision", code, str(agent.mandate))


def test_a_mandate_that_ran_out_of_time_is_expired_by_the_validation(ns, env, agent):
    ref = admitted(ns, env, agent)
    update_mandate(env, ns, agent.mandate, valid_until=NOW - timedelta(seconds=1))
    assert validate(ns, env, ref)["reason_code"] == "MANDATE_EXPIRED"
    row = mandate_row(env, ns, agent.mandate)
    assert (row.state, row.revocation_epoch) == ("expired", 1)
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", "MANDATE_EXPIRED")


def test_a_mandate_epoch_that_moved_after_admission_revokes_the_run(ns, env, agent):
    ref = admitted(ns, env, agent)
    update_mandate(env, ns, agent.mandate, revocation_epoch=3)
    assert validate(ns, env, ref)["reason_code"] == "EPOCH_MISMATCH"


def test_a_mandate_now_bound_to_another_binding_revokes_the_run(ns, env, agent):
    ref = admitted(ns, env, agent)
    update_mandate(env, ns, agent.mandate, agent_binding_ref=uuid.uuid4())
    assert validate(ns, env, ref)["reason_code"] == "DENIED_BINDING"


def test_a_vanished_mandate_revokes_the_run(ns, env, agent):
    ref = admitted(ns, env, agent)
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.delete(ns.Mandate))
        session.commit()
    assert validate(ns, env, ref)["reason_code"] == "MANDATE_INACTIVE"


def test_an_inactive_owner_suspends_the_mandate_when_a_worker_starts(ns, env, agent):
    ref = admitted(ns, env, agent)
    env.epochs[OWNER]["active"] = False
    assert validate(ns, env, ref)["reason_code"] == "MANDATE_INACTIVE"
    row = mandate_row(env, ns, agent.mandate)
    assert (row.state, row.suspended_reason, row.revocation_epoch) == ("suspended", "OWNER_SUSPENDED", 1)
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", "MANDATE_INACTIVE")


def test_a_role_change_of_the_owner_after_admission_is_an_epoch_mismatch_and_leaves_the_mandate(ns, env, agent):
    ref = admitted(ns, env, agent)
    env.epochs[OWNER]["user"] = 9
    assert validate(ns, env, ref)["reason_code"] == "EPOCH_MISMATCH"
    assert mandate_row(env, ns, agent.mandate).state == "active"


def test_a_restart_after_the_runtime_limit_is_stale(ns, env, agent):
    ref = admitted(ns, env, agent)
    ns.adm._now = lambda: NOW + timedelta(seconds=601)  # pylint: disable=W0212
    assert validate(ns, env, ref)["reason_code"] == "STALE_CONTEXT"


def test_an_edit_of_the_task_meta_cannot_extend_a_context(ns, env, agent):
    ref = admitted(ns, env, agent)
    update_context = env.ns.sa.update(ns.models.ExecutionSecurityContext).values(
        expires_at=NOW + timedelta(seconds=10))
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(update_context)
        session.commit()
    ns.adm._now = lambda: NOW + timedelta(seconds=11)  # pylint: disable=W0212
    assert validate(ns, env, ref, caller={**CALLER, "context_deadline": "2999-01-01T00:00:00+00:00"})[
        "reason_code"] == "STALE_CONTEXT"


def test_a_credential_grant_revoked_after_admission_revokes_a_mandate_run(ns, env, agent):
    credential = MandateCredential(env, agent)
    ref = admitted(ns, env, agent, payload=credential.payload)
    env.grants.clear()
    assert validate(ns, env, ref)["reason_code"] == "DENIED_GRANT"


def test_a_mandate_grant_that_turned_into_a_person_grant_revokes_the_run(ns, env, agent):
    credential = MandateCredential(env, agent)
    ref = admitted(ns, env, agent, payload=credential.payload)
    for grant in env.grants.values():
        grant["subject_kind"] = "user_delegation"
    assert validate(ns, env, ref)["reason_code"] == "DENIED_GRANT"


def test_a_mandate_context_with_intact_credentials_is_accepted(ns, env, agent):
    credential = MandateCredential(env, agent)
    ref = admitted(ns, env, agent, payload=credential.payload)
    assert validate(ns, env, ref)["ok"] is True


# --- the dispatch seam and the time limit -------------------------------------------------------

def dispatch(ns, env, agent, module=None, over=None):
    payload = {"project_id": 1, "message_id": 5, "envelope": {"security_profile": "governed", "context_ref": None},
               "_elitea_predict_run_id": RUN}
    call = {"args": ["thread", 5], "kwargs": payload, "pool": "agents",
            "meta": {"project_id": 1, "task_name": "indexer_agent", "context_deadline": "forged"}}
    context_ref = ns.adm.admit_dispatch(module or env.module(), "indexer_agent", call, request(ns, agent.mandate, **(over or {})))
    return context_ref, call


def test_a_mandate_dispatch_carries_the_context_and_its_deadline_for_main_only(ns, env, agent):
    context_ref, call = dispatch(ns, env, agent)
    assert call["kwargs"]["envelope"]["context_ref"] == context_ref == call["meta"]["context_ref"]
    assert call["meta"]["context_deadline"] == ns.adm.RunDeadline(NOW + timedelta(seconds=600), "RUNTIME_LIMIT_EXCEEDED")


@pytest.mark.parametrize("over, ttl, expires, deadline, code", [
    ({}, 3600, 600, 600, "RUNTIME_LIMIT_EXCEEDED"),                     # max_runtime_s 600 is the nearest
    ({"runtime": 7200}, 3600, 3600, 7200, "RUNTIME_LIMIT_EXCEEDED"),    # the ttl bounds the start window only
    ({"runtime": 7200, "valid_until": NOW + timedelta(seconds=5000)}, 3600, 3600, 5000, "MANDATE_EXPIRED"),
    ({"runtime": 7200, "valid_until": NOW + timedelta(seconds=90)}, 3600, 90, 90, "MANDATE_EXPIRED"),
    ({"runtime": 600, "valid_until": NOW + timedelta(seconds=600)}, 3600, 600, 600, "RUNTIME_LIMIT_EXCEEDED"),   # a tie
])
def test_the_run_deadline_is_not_capped_by_the_context_ttl_and_names_the_bound_that_applied(
        ns, env, over, ttl, expires, deadline, code):
    binding = env.seed_agent()
    agent = types.SimpleNamespace(mandate=add_mandate(env, ns, state="active", binding_ref=binding, **over))
    view = admit(ns, env, agent, module=env.module(context_ttl_s=ttl))
    assert view.expires_at == NOW + timedelta(seconds=expires)   # the start / restart window
    assert (view.run_deadline, view.run_deadline_code) == (NOW + timedelta(seconds=deadline), code)


def test_an_interactive_or_inherited_dispatch_never_keeps_a_deadline(ns, env):
    env.seed_agent()
    call = {"args": [], "kwargs": {"project_id": 1, "_elitea_predict_run_id": RUN,
                                   "envelope": {"security_profile": "governed", "context_ref": None}},
            "meta": {"project_id": 1, "context_deadline": "forged"}}
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert "context_deadline" not in call["meta"]
    inherited = {"args": [], "kwargs": {"project_id": 1, "envelope": {"security_profile": "governed", "context_ref": "x"}},
                 "meta": {"project_id": 1, "context_deadline": "forged"}}
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", inherited, None) is None
    assert "context_deadline" not in inherited["meta"]


def test_track_context_remembers_a_deadline_and_completion_still_closes_the_context(ns, env, agent):
    module = env.module()
    view = admit(ns, env, agent)
    deadline = ns.adm.RunDeadline(NOW + timedelta(seconds=600), "RUNTIME_LIMIT_EXCEEDED")
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=deadline)
    assert module._context_by_task["task-1"] == (1, view.context_ref, deadline)   # pylint: disable=W0212
    ns.adm.track_context(module, "task-2", 1, "other-ref")
    assert module._context_by_task["task-2"] == (1, "other-ref")   # pylint: disable=W0212
    ns.adm.complete_context(module, "task-1")
    assert context_row(env, ns).state == "completed" and "task-1" not in module._context_by_task  # pylint: disable=W0212


def sweep_setup(ns, env, agent):
    module = env.module()
    stopped = []
    module.task_node = types.SimpleNamespace(stop_task=stopped.append)
    view = admit(ns, env, agent)
    env.denied.clear()
    env.log.lines.clear()
    return module, stopped, view


def overdue(ns, seconds=-1, code="RUNTIME_LIMIT_EXCEEDED"):
    return ns.adm.RunDeadline(NOW + timedelta(seconds=seconds), code)


def broken_stop(module, calls):
    def stop(task_id):
        calls.append(task_id)
        raise RuntimeError("pool down")

    module.task_node = types.SimpleNamespace(stop_task=stop)


def test_a_run_past_its_deadline_is_stopped_once_and_its_context_revoked(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns))
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["task-1"]
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", "RUNTIME_LIMIT_EXCEEDED")
    event = deny_event(env)
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("decision", "deny", "RUNTIME_LIMIT_EXCEEDED")
    assert event["mandate_ref"] == str(agent.mandate) and event["run_id"] == RUN
    assert "security_metric name=execution_context_validation_total result=deny reason_code=RUNTIME_LIMIT_EXCEEDED" in env.log.lines
    assert ns.adm.enforce_run_deadlines(module) == 0 and stopped == ["task-1"]   # forgotten: not stopped twice
    assert "task-1" not in module._context_by_task   # pylint: disable=W0212


def test_a_run_that_outlived_the_mandate_is_revoked_as_expired_not_as_over_time(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns, code="MANDATE_EXPIRED"))
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["task-1"]
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", "MANDATE_EXPIRED")
    assert deny_event(env)["reason_code"] == "MANDATE_EXPIRED"


def test_the_deadline_is_inclusive_and_a_future_one_is_left_alone(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "later", 1, view.context_ref, deadline=overdue(ns, 1))
    assert ns.adm.enforce_run_deadlines(module, now=NOW) == 0 and stopped == []
    assert ns.adm.enforce_run_deadlines(module, now=NOW + timedelta(seconds=1)) == 1 and stopped == ["later"]


def test_runs_without_a_deadline_are_never_touched_by_the_sweep(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "interactive", 1, view.context_ref)
    assert ns.adm.enforce_run_deadlines(module, now=NOW + timedelta(days=30)) == 0
    assert stopped == [] and context_row(env, ns).state == "admitted"
    assert "interactive" in module._context_by_task   # pylint: disable=W0212


def test_a_task_that_cannot_be_stopped_stays_tracked_and_is_tried_again_on_the_next_tick(ns, env, agent):
    module, _, view = sweep_setup(ns, env, agent)
    calls = []
    broken_stop(module, calls)
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns))
    before = ns.adm.RUN_STOP.total("stop_failed", "RUNTIME_LIMIT_EXCEEDED")
    assert ns.adm.enforce_run_deadlines(module) == 0 and calls == ["task-1"]
    assert "task-1" in module._context_by_task   # pylint: disable=W0212
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("admitted", None)   # nothing recorded yet
    assert env.denied == []
    assert ns.adm.RUN_STOP.total("stop_failed", "RUNTIME_LIMIT_EXCEEDED") == before + 1
    assert len([line for line in env.log.lines if "could not be stopped" in line]) == 1
    # the pool is back: the next tick stops it, forgets it and revokes the context
    stopped = []
    module.task_node = types.SimpleNamespace(stop_task=stopped.append)
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["task-1"]
    assert "task-1" not in module._context_by_task   # pylint: disable=W0212
    assert context_row(env, ns).revoked_reason == "RUNTIME_LIMIT_EXCEEDED" and len(env.denied) == 1


def test_a_failed_stop_does_not_hold_back_the_other_runs(ns, env, agent):
    module, _, view = sweep_setup(ns, env, agent)
    calls = []

    def stop(task_id):
        calls.append(task_id)
        if task_id == "bad":
            raise RuntimeError("pool down")

    module.task_node = types.SimpleNamespace(stop_task=stop)
    ns.adm.track_context(module, "bad", 1, view.context_ref, deadline=overdue(ns))
    ns.adm.track_context(module, "good", 1, view.context_ref, deadline=overdue(ns))
    assert ns.adm.enforce_run_deadlines(module) == 1 and calls == ["bad", "good"]
    assert list(module._context_by_task) == ["bad"]   # pylint: disable=W0212


def test_a_context_that_finished_before_the_sweep_is_not_recorded_again(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns))
    update(env, ns.models.ExecutionSecurityContext, ns.models.ExecutionSecurityContext.id == uuid.UUID(view.context_ref),
           state="completed")
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["task-1"]
    assert context_row(env, ns).state == "completed" and env.denied == []


def revoke_context(env, ns, view, reason="DENIED_BINDING"):
    update(env, ns.models.ExecutionSecurityContext, ns.models.ExecutionSecurityContext.id == uuid.UUID(view.context_ref),
           state="revoked", revoked_reason=reason)


def test_a_tracked_run_whose_context_was_revoked_is_stopped_even_without_a_deadline(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "interactive", 1, view.context_ref)
    ns.adm.track_context(module, "mandate", 1, view.context_ref, deadline=overdue(ns, 600))
    assert ns.adm.enforce_run_deadlines(module) == 0 and stopped == []   # nothing revoked yet
    revoke_context(env, ns, view)
    assert ns.adm.enforce_run_deadlines(module) == 2 and sorted(stopped) == ["interactive", "mandate"]
    assert list(module._context_by_task) == []   # pylint: disable=W0212
    assert context_row(env, ns).revoked_reason == "DENIED_BINDING" and env.denied == []   # already audited when revoked
    assert ns.adm.enforce_run_deadlines(module) == 0 and len(stopped) == 2


def test_a_revoked_run_that_cannot_be_stopped_stays_tracked_for_the_next_tick(ns, env, agent):
    module, _, view = sweep_setup(ns, env, agent)
    calls = []
    broken_stop(module, calls)
    ns.adm.track_context(module, "task-1", 1, view.context_ref)
    revoke_context(env, ns, view)
    before = ns.adm.RUN_STOP.total("stop_failed", "CONTEXT_REVOKED")
    assert ns.adm.enforce_run_deadlines(module) == 0 and calls == ["task-1"] and "task-1" in module._context_by_task  # pylint: disable=W0212
    assert ns.adm.RUN_STOP.total("stop_failed", "CONTEXT_REVOKED") == before + 1
    assert ns.adm.enforce_run_deadlines(module) == 0 and calls == ["task-1", "task-1"]


def test_an_overdue_run_is_stopped_once_not_as_overdue_and_as_revoked(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns))
    revoke_context(env, ns, view, "EPOCH_MISMATCH")
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["task-1"]
    assert context_row(env, ns).revoked_reason == "EPOCH_MISMATCH" and env.denied == []   # the first reason stays


def test_the_revoked_runs_are_found_with_one_query_per_project_not_one_per_run(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)
    for index in range(5):
        ns.adm.track_context(module, f"task-{index}", 1, view.context_ref)
    env.sessions.clear()
    ns.adm.enforce_run_deadlines(module)
    assert [project for project, _ in env.sessions] == [1]


def test_an_unreadable_context_table_does_not_stop_the_deadline_part_of_the_sweep(ns, env, agent, monkeypatch):
    module, stopped, view = sweep_setup(ns, env, agent)
    ns.adm.track_context(module, "late", 1, view.context_ref, deadline=overdue(ns))
    ns.adm.track_context(module, "other", 1, view.context_ref)
    monkeypatch.setattr(ns.adm, "_revoked_among", mock.Mock(side_effect=RuntimeError("db down")))
    assert ns.adm.enforce_run_deadlines(module) == 1 and stopped == ["late"]
    assert any("could not be listed" in line for line in env.log.lines)


def test_the_sweep_reads_the_tracked_map_under_the_lock_that_dispatch_uses(ns, env, agent):
    module, stopped, view = sweep_setup(ns, env, agent)

    class Spy:
        def __init__(self):
            self.entered = 0
            self._lock = threading.Lock()

        def __enter__(self):
            self.entered += 1
            return self._lock.__enter__()

        def __exit__(self, *exc):
            return self._lock.__exit__(*exc)

    module._context_lock = spy = Spy()   # pylint: disable=W0212
    ns.adm.track_context(module, "task-1", 1, view.context_ref, deadline=overdue(ns))
    assert spy.entered == 1
    ns.adm.enforce_run_deadlines(module)
    assert spy.entered >= 3   # snapshot, then forgetting the stopped task
    ns.adm.track_context(module, "task-2", 1, view.context_ref)
    ns.adm.complete_context(module, "task-2")
    assert spy.entered >= 5


def test_flag_off_the_sweep_does_nothing_at_all(ns, env, agent):
    module = env.module(governed=False)
    module._context_by_task["task-1"] = (1, "ref", NOW - timedelta(days=1))   # pylint: disable=W0212
    env.poison()
    assert ns.adm.enforce_run_deadlines(module) == 0
    env.unpoison()
    assert "task-1" in module._context_by_task   # pylint: disable=W0212


# --- the owner event ----------------------------------------------------------------------------

def scanner_for(ns, delay=6.0, **kwargs):
    return ns.mandate_owner.OwnerScanner(delay=lambda: delay, start_worker=False, **kwargs)


def event_runner(ns, env, payload, module=None, scanner=None):
    """Deliver one event and let the (single) worker serve whatever it queued, without waiting."""
    scanner = scanner_for(ns) if scanner is None else scanner
    queued = ns.mandate_owner.on_auth_invalidate(module or env.module(), payload, scanner=scanner)
    while scanner.run_one(wait=False):
        pass
    return queued, scanner


def test_flag_off_the_owner_event_returns_at_once(ns, env, agent):
    env.poison()
    scanner = scanner_for(ns)
    queued = ns.mandate_owner.on_auth_invalidate(
        env.module(governed=False), {"kind": "user", "id": OWNER, "epoch": 2}, scanner=scanner)
    assert queued is False and len(scanner) == 0 and scanner._queue.empty()   # pylint: disable=W0212


@pytest.mark.parametrize("payload", [None, "user", {}, {"kind": "token", "id": OWNER}, {"kind": "user"},
                                     {"kind": "user", "id": "5"}, {"kind": "user", "id": True}, {"id": OWNER}])
def test_an_event_that_is_not_about_a_user_queues_nothing(ns, env, agent, payload):
    queued, scanner = event_runner(ns, env, payload)
    assert queued is False and scanner._queue.empty() and env.epoch_calls == []   # pylint: disable=W0212


def test_a_role_change_queues_one_read_and_suspends_and_scans_nothing(ns, env, agent):
    queued, _ = event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert queued is True and env.epoch_calls == [("user", OWNER)]
    assert mandate_row(env, ns, agent.mandate).state == "active"
    assert env.sessions == []   # no project was opened (the project list was not even asked): the verdict said active


def test_duplicate_events_for_one_user_coalesce_into_one_read(ns, env, agent):
    scanner = scanner_for(ns)
    module = env.module()
    payload = {"kind": "user", "id": OWNER, "epoch": 2}
    results = [ns.mandate_owner.on_auth_invalidate(module, payload, scanner=scanner) for _ in range(50)]
    assert results == [True] + [False] * 49 and len(scanner) == 1
    while scanner.run_one(wait=False):
        pass
    assert env.epoch_calls == [("user", OWNER)]
    # once the worker took the user up, a newer event queues a fresh read
    assert ns.mandate_owner.on_auth_invalidate(module, payload, scanner=scanner) is True


def test_other_users_are_queued_separately_and_served_in_order(ns, env, agent):
    scanner = scanner_for(ns)
    module = env.module()
    for user in (OWNER, 6, 7):
        assert ns.mandate_owner.on_auth_invalidate(module, {"kind": "user", "id": user}, scanner=scanner) is True
    while scanner.run_one(wait=False):
        pass
    assert env.epoch_calls == [("user", OWNER), ("user", 6), ("user", 7)]


def test_the_queue_is_bounded(ns, env, agent):
    scanner = scanner_for(ns, max_pending=3)
    module = env.module()
    results = [ns.mandate_owner.on_auth_invalidate(module, {"kind": "user", "id": user}, scanner=scanner)
               for user in range(10, 16)]
    assert results == [True] * 3 + [False] * 3 and len(scanner) == 3


def test_the_verdict_is_read_only_after_the_auth_cache_ttl_has_passed(ns, env, agent):
    clock, slept = [100.0], []
    scanner = ns.mandate_owner.OwnerScanner(
        delay=ns.mandate_owner.owner_read_delay, clock=lambda: clock[0], sleep=slept.append, start_worker=False)
    ns.tools.auth.epoch_ttl = 5.0
    try:
        assert ns.mandate_owner.owner_read_delay() == 6.0
        ns.mandate_owner.on_auth_invalidate(env.module(), {"kind": "user", "id": OWNER}, scanner=scanner)
        clock[0] += 2.0   # two seconds later the worker gets to it
        scanner.run_one()
        assert slept == [4.0]   # waits out the rest of ttl + 1 s
        ns.tools.auth.epoch_ttl = 30
        assert ns.mandate_owner.owner_read_delay() == 31.0
        for odd in (None, "5", True, -1):
            ns.tools.auth.epoch_ttl = odd
            assert ns.mandate_owner.owner_read_delay() == 6.0   # not exposed: the default ttl plus a second
    finally:
        del ns.tools.auth.epoch_ttl


def test_a_stale_active_reply_is_not_trusted_the_verdict_is_read_after_the_wait(ns, env, agent):
    """The cached verdict still says active when the event arrives; by the time the worker reads, it is fresh."""
    scanner = scanner_for(ns, sleep=lambda seconds: env.epochs[OWNER].update(active=False))
    ns.mandate_owner.on_auth_invalidate(env.module(), {"kind": "user", "id": OWNER}, scanner=scanner)
    assert env.epoch_calls == []   # nothing was read on arrival
    scanner.run_one()   # waits (the fake sleep is when the cache turns fresh), then reads
    assert env.epoch_calls == [("user", OWNER)]
    assert mandate_row(env, ns, agent.mandate).state == "suspended"


def test_an_unreadable_verdict_changes_nothing(ns, env, agent):
    env.epochs.pop(OWNER)
    event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert mandate_row(env, ns, agent.mandate).state == "active"


def test_a_verdict_without_the_active_key_changes_nothing(ns, env, agent):
    env.epochs[OWNER] = {"user": 9}
    event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert mandate_row(env, ns, agent.mandate).state == "active"


def test_a_suspended_owner_loses_their_mandates_in_every_project_and_their_live_runs(ns, env, agent):
    ns.projects[:] = [1, 2]
    env.seed_agent(project_id=2)
    second = add_mandate(env, ns, project_id=2, state="active", binding_ref=uuid.uuid4())
    other = add_mandate(env, ns, project_id=2, state="active", owner=6, version=71, application=8)
    admit(ns, env, agent)
    env.epochs[OWNER]["active"] = False
    queued, _ = event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert queued is True
    assert (mandate_row(env, ns, agent.mandate).state, mandate_row(env, ns, agent.mandate).suspended_reason) == (
        "suspended", "OWNER_SUSPENDED")
    assert mandate_row(env, ns, second, project_id=2).state == "suspended"
    assert mandate_row(env, ns, other, project_id=2).state == "active"
    assert (context_row(env, ns).state, context_row(env, ns).revoked_reason) == ("revoked", "MANDATE_INACTIVE")
    reasons = sorted(event["reason_code"] for _, event in env.recorded if event["event_type"] == "lifecycle")
    assert reasons == ["MANDATE_OWNER_SUSPENDED", "MANDATE_OWNER_SUSPENDED"]
    ns.projects[:] = [1]


def test_one_unreadable_project_does_not_stop_the_scan_of_the_others(ns, env, agent, monkeypatch):
    ns.projects[:] = [2, 1]
    real = ns.mandate_owner.suspend_owner_in_project

    def flaky(project_id, owner):
        if project_id == 2:
            raise RuntimeError("schema missing")
        return real(project_id, owner)

    monkeypatch.setattr(ns.mandate_owner, "suspend_owner_in_project", flaky)
    env.epochs[OWNER]["active"] = False
    event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert mandate_row(env, ns, agent.mandate).state == "suspended"
    assert any("project 2 could not be scanned" in line for line in env.log.lines)
    ns.projects[:] = [1]


def test_a_failing_scan_is_logged_and_never_stops_the_worker(ns, env, agent):
    env.epochs_raise = True
    queued, scanner = event_runner(ns, env, {"kind": "user", "id": OWNER, "epoch": 2})
    assert queued is True and any("suspension scan failed" in line for line in env.log.lines)
    assert len(scanner) == 0   # and the user can be queued again
    env.epochs_raise = False
    assert ns.mandate_owner.on_auth_invalidate(env.module(), {"kind": "user", "id": OWNER}, scanner=scanner) is True


def test_one_worker_thread_serves_all_events_not_one_thread_per_event(ns, env, agent):
    seen = []
    scanner = ns.mandate_owner.OwnerScanner(scan=seen.append, delay=lambda: 0.0, sleep=lambda seconds: None)
    workers = lambda: [t for t in threading.enumerate() if t.name == "mandate-owner-scan"]   # noqa: E731
    before = len(workers())
    for user in range(20, 40):
        scanner.submit(user)
    assert len(workers()) <= before + 1
    for _ in range(200):
        if len(seen) == 20:
            break
        time.sleep(0.01)
    assert seen == list(range(20, 40)) and len(workers()) <= before + 1 and scanner._worker.daemon   # pylint: disable=W0212


# --- startup ------------------------------------------------------------------------------------

def test_the_preflight_passes_with_the_mandate_dependencies(ns, env):
    assert [check.id for check in ns.adm.governance_preflight(env.module()) if check.status == "fail"] == []


@pytest.mark.parametrize("break_it, expected", [
    (lambda env, ns: delattr(ns.tools.auth, "get_user_by_idp_subject"), "identity_verifier"),
    (lambda env, ns: delattr(ns.tools.auth, "get_user_permissions"), "identity_verifier"),
    (lambda env, ns: setattr(env, "facts_version", 1), "credential_facts"),
    (lambda env, ns: setattr(env, "facts_version", None), "credential_facts"),
    (lambda env, ns: setattr(env, "facts_version", True), "credential_facts"),
    (lambda env, ns: setattr(env, "missing_modules", {"secrets"}), "credential_facts"),
])
def test_the_preflight_fails_for_a_platform_that_cannot_decide_a_mandate_run(ns, env, break_it, expected):
    break_it(env, ns)
    try:
        failed = [check.id for check in ns.adm.governance_preflight(env.module()) if check.status == "fail"]
        assert expected in failed
    finally:
        ns.install_fakes(env)
