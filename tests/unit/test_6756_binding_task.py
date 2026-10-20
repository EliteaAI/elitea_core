"""C-1009 (#6756): the ``manage_agent_identity_binding`` admin task - parser, lifecycle actions, audit.

Real models and statements on SQLite files; see ``fixtures/admission_env.py``.
"""
import importlib.util
import os
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from test_6724_chat_templates import _make_migration_stubs  # noqa: E402  pylint: disable=C0413

ONE = "project_id=1;application_id=7;version_id=70;mode=elitea_native"
APPROVE = "approval_ref=CHG-12;policy_revision=rev-1"
APPROVER = 8   # activation needs another operator than the creator (actor 7 in `run`), #7105
DIGEST = "adv1:" + "1" * 64


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("binding"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.add_version(1, 70, 7)
    ns.env.digests[70] = DIGEST
    return ns.env


def run(ns, env, param, actor=7, module=None):
    return ns.binding_admin.manage_binding(module or env.module(governed=False), param, actor)


def binding(env, ns):
    [row] = env.query(1, ns.models.AgentIdentityBinding)
    return row


def reasons(env):
    return [event["reason_code"] for _, event in env.recorded if event["event_type"] == "lifecycle"]


def activate(ns, env, module=None):
    for action in ("create", "submit"):
        assert "error" not in run(ns, env, f"{ONE};action={action}", module=module)
    return run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER, module=module)


# --- parser -----------------------------------------------------------------------------------

def parsed(ns, param):
    return ns.binding_admin.parse_binding_param(param)


def test_a_full_parameter_is_parsed(ns):
    values, error = parsed(ns, f" {ONE} ; action=activate ;{APPROVE};DRY_RUN;")
    assert error is None
    assert values == {"dry_run": True, "project_id": 1, "application_id": 7, "version_id": 70,
                      "mode": "elitea_native", "action": "activate", "approval_ref": "CHG-12",
                      "policy_revision": "rev-1"}


def test_dry_run_is_off_by_default(ns):
    assert parsed(ns, f"{ONE};action=create")[0]["dry_run"] is False


@pytest.mark.parametrize("param, fragment", [
    (None, "project_id, application_id, version_id, mode, action required"),
    ("", "required"),
    ("dry_run", "required"),
    (f"{ONE}", "action required"),
    ("project_id=1;application_id=7;version_id=70;action=create", "mode required"),
    ("application_id=7;version_id=70;mode=elitea_native;action=create", "project_id required"),
    (f"{ONE};action=create;color=red", "unknown parameter 'color=red'"),
    (f"{ONE};action=create;project", "unknown parameter 'project'"),
    ("project_id=x;application_id=7;version_id=70;mode=elitea_native;action=create", "invalid project_id: 'x'"),
    ("project_id=0;application_id=7;version_id=70;mode=elitea_native;action=create", "invalid project_id: '0'"),
    ("project_id=1;application_id=-3;version_id=70;mode=elitea_native;action=create", "invalid application_id"),
    ("project_id=1;application_id=7;version_id=1.5;mode=elitea_native;action=create", "invalid version_id"),
    ("project_id=1;application_id=7;version_id=70;mode=root;action=create", "invalid mode: 'root'"),
    (f"{ONE};action=delete", "invalid action: 'delete'"),
])
def test_a_bad_parameter_is_a_readable_error(ns, param, fragment):
    values, error = parsed(ns, param)
    assert values is None and fragment in error


@pytest.mark.parametrize("param", [None, "", "action=create", f"{ONE};action=nope", f"{ONE};version_id=x;action=create"])
def test_a_bad_parameter_changes_nothing(ns, env, param):
    result = run(ns, env, param)
    assert list(result) == ["error"]
    assert env.recorded == [] and env.sessions == []


# --- create -----------------------------------------------------------------------------------

def test_create_inserts_a_draft_with_the_digest_of_the_current_definition(ns, env):
    result = run(ns, env, f"{ONE};action=create")
    row = binding(env, ns)
    assert result == {"action": "create", "binding_ref": str(row.id), "state": "draft",
                      "definition_digest": DIGEST, "dry_run": False}
    assert (row.state, row.definition_digest, row.revocation_epoch, row.created_by) == ("draft", DIGEST, 0, 7)
    assert (row.project_id, row.application_id, row.version_id, row.identity_mode) == (1, 7, 70, "elitea_native")
    assert row.approval_ref is None and row.policy_revision is None


def test_create_writes_one_lifecycle_event_in_the_session_that_inserted(ns, env):
    run(ns, env, f"{ONE};action=create")
    [(event_session, event)] = env.recorded
    [(_, write_session)] = [entry for entry in env.sessions if entry[1] is event_session]
    assert write_session.commits == 1
    assert event == {
        "event_type": "lifecycle", "reason_code": "BINDING_DRAFT", "outcome": "success", "project_id": 1,
        "application_id": 7, "agent_binding_ref": str(binding(env, ns).id), "revocation_epoch": 0,
        "caller_ref": "operator:7",
    }


def test_a_missing_operator_id_is_recorded_as_a_plain_operator(ns, env):
    run(ns, env, f"{ONE};action=create", actor=0)
    assert env.recorded[0][1]["caller_ref"] == "operator" and binding(env, ns).created_by is None


def test_create_twice_is_refused(ns, env):
    run(ns, env, f"{ONE};action=create")
    result = run(ns, env, f"{ONE};action=create")
    assert result == {"error": "binding already exists; use another action"}
    assert len(env.query(1, ns.models.AgentIdentityBinding)) == 1 and len(env.recorded) == 1


def test_one_version_can_have_a_binding_per_mode(ns, env):
    run(ns, env, f"{ONE};action=create")
    assert "error" not in run(ns, env, "project_id=1;application_id=7;version_id=70;mode=dedicated_agent;action=create")
    assert len(env.query(1, ns.models.AgentIdentityBinding)) == 2


def test_create_dry_run_reports_the_digest_and_writes_nothing(ns, env):
    result = run(ns, env, f"{ONE};action=create;dry_run")
    assert result == {"dry_run": True, "action": "create", "state": "draft", "definition_digest": DIGEST}
    assert env.query(1, ns.models.AgentIdentityBinding) == [] and env.recorded == []


def test_the_version_must_exist_in_this_application(ns, env):
    assert run(ns, env, "project_id=1;application_id=8;version_id=70;mode=elitea_native;action=create") == {
        "error": "the version does not exist in this application"}
    assert run(ns, env, "project_id=1;application_id=7;version_id=71;mode=elitea_native;action=create") == {
        "error": "the version does not exist in this application"}
    assert env.query(1, ns.models.AgentIdentityBinding) == []


def test_a_definition_that_cannot_be_digested_is_a_readable_error(ns, env):
    env.digests.pop(70)
    result = run(ns, env, f"{ONE};action=create")
    assert result["error"].startswith("VERSION_NOT_FOUND")
    assert env.query(1, ns.models.AgentIdentityBinding) == []


# --- the lifecycle ----------------------------------------------------------------------------

def test_submit_walks_draft_to_pending_approval_with_an_event_per_change(ns, env):
    run(ns, env, f"{ONE};action=create")
    result = run(ns, env, f"{ONE};action=submit")
    assert result["state"] == "pending_approval" and binding(env, ns).state == "pending_approval"
    assert reasons(env) == ["BINDING_DRAFT", "BINDING_PROVISIONING", "BINDING_PENDING_APPROVAL"]
    # both changes and both events in one transaction
    sessions = {id(session) for session, event in env.recorded[1:]}
    assert len(sessions) == 1
    [write_session] = [s for _, s in env.sessions if id(s) in sessions]
    assert write_session.commits == 1


def test_activate_needs_an_approval_and_a_policy_revision(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit")
    for param in (f"{ONE};action=activate", f"{ONE};action=activate;approval_ref=CHG-12",
                  f"{ONE};action=activate;policy_revision=rev-1"):
        assert run(ns, env, param)["error"].startswith("APPROVAL_REQUIRED")
    assert binding(env, ns).state == "pending_approval"


def test_activate_records_who_approved_and_under_which_policy(ns, env):
    result = activate(ns, env)
    row = binding(env, ns)
    assert result["state"] == "active" and (row.state, row.approval_ref, row.policy_revision) == (
        "active", "CHG-12", "rev-1")
    assert row.state_changed_by == APPROVER and row.created_by == 7 and row.revocation_epoch == 0
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["approver_ref"], event["policy_revision"]) == ("BINDING_ACTIVE", "CHG-12", "rev-1")


def test_activate_refuses_a_definition_that_changed_since_the_binding_was_created(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit")
    env.digests[70] = "adv1:" + "2" * 64
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER)["error"].startswith("BINDING_DIGEST_MISMATCH")
    assert binding(env, ns).state == "pending_approval"


def test_a_runtime_domain_binding_is_activated_only_with_the_operators_allow(ns, env):
    param = "project_id=1;application_id=7;version_id=70;mode=runtime_domain_binding"
    for action in ("create", "submit"):
        run(ns, env, f"{param};action={action}")
    assert run(ns, env, f"{param};action=activate;{APPROVE}", actor=APPROVER)["error"].startswith("DENIED_BINDING")
    allowed = run(ns, env, f"{param};action=activate;{APPROVE}", actor=APPROVER,
                  module=env.module(allow_runtime_domain_binding=True))
    assert allowed["state"] == "active"


def test_suspend_raises_the_epoch_and_revokes_the_live_contexts_of_that_binding_only(ns, env):
    activate(ns, env)
    env.add_version(1, 80, 8)
    other = env.add_binding(1, 8, 80)
    module = env.module()
    mine = ns.adm.admit_execution(module, env.request(), run_id="run-mine").context_ref
    theirs = ns.adm.admit_execution(module, env.request(application_id=8, version_id=80), run_id="run-theirs").context_ref
    done = ns.adm.admit_execution(module, env.request(), run_id="run-done").context_ref
    ns.adm.track_context(module, "t1", 1, done)
    ns.adm.complete_context(module, "t1")
    result = run(ns, env, f"{ONE};action=suspend")
    assert result["state"] == "suspended"
    row = [b for b in env.query(1, ns.models.AgentIdentityBinding) if b.id != other][0]
    assert (row.state, row.revocation_epoch) == ("suspended", 1)
    states = {str(c.id): (c.state, c.revoked_reason) for c in env.query(1, ns.models.ExecutionSecurityContext)}
    assert states[mine] == ("revoked", "EPOCH_MISMATCH")
    assert states[theirs] == ("admitted", None)
    assert states[done] == ("completed", None)
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["revocation_epoch"], event["outcome"]) == ("BINDING_SUSPENDED", 1, "success")
    # the worker of the revoked run is told why
    caller = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": "run-mine"}
    assert ns.adm.validate_context(module, 1, mine, caller)["reason_code"] == "EPOCH_MISMATCH"


def test_resume_reactivates_with_the_stored_approval_and_keeps_the_raised_epoch(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=suspend")
    result = run(ns, env, f"{ONE};action=resume")
    row = binding(env, ns)
    assert result["state"] == "active" and (row.approval_ref, row.policy_revision, row.revocation_epoch) == (
        "CHG-12", "rev-1", 1)
    assert reasons(env)[-2:] == ["BINDING_SUSPENDED", "BINDING_ACTIVE"]


def test_a_resumed_binding_does_not_revive_the_contexts_admitted_before_the_suspension(ns, env):
    activate(ns, env)
    module = env.module()
    ref = ns.adm.admit_execution(module, env.request(), run_id="run-1").context_ref
    run(ns, env, f"{ONE};action=suspend")
    run(ns, env, f"{ONE};action=resume")
    caller = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": "run-1"}
    assert ns.adm.validate_context(module, 1, ref, caller)["reason_code"] == "EPOCH_MISMATCH"
    assert ns.adm.admit_execution(module, env.request(), run_id="run-2").context_ref  # new work is admitted again


def test_retire_from_active_passes_through_retiring_and_revokes(ns, env):
    activate(ns, env)
    ref = ns.adm.admit_execution(env.module(), env.request(), run_id="run-1").context_ref
    result = run(ns, env, f"{ONE};action=retire")
    row = binding(env, ns)
    assert result["state"] == "retired" and (row.state, row.revocation_epoch) == ("retired", 2)
    assert reasons(env)[-2:] == ["BINDING_RETIRING", "BINDING_RETIRED"]
    [context] = env.query(1, ns.models.ExecutionSecurityContext)
    assert str(context.id) == ref and context.state == "revoked"


@pytest.mark.parametrize("steps, expected_events", [
    ([], ["BINDING_RETIRED"]),
    (["submit"], ["BINDING_RETIRED"]),
])
def test_retire_before_activation_goes_straight_to_retired(ns, env, steps, expected_events):
    run(ns, env, f"{ONE};action=create")
    for step in steps:
        run(ns, env, f"{ONE};action={step}")
    assert run(ns, env, f"{ONE};action=retire")["state"] == "retired"
    assert reasons(env)[-1:] == expected_events


@pytest.mark.parametrize("action, state", [
    ("activate", "draft"), ("submit", "pending_approval"), ("resume", "draft"), ("suspend", "draft"),
    ("retire", "retired"),
])
def test_an_action_that_does_not_apply_to_the_state_is_refused_and_changes_nothing(ns, env, action, state):
    run(ns, env, f"{ONE};action=create")
    if state != "draft":
        run(ns, env, f"{ONE};action=submit")
    if state == "retired":
        run(ns, env, f"{ONE};action=retire")
    before = (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded))
    result = run(ns, env, f"{ONE};action={action}" + (f";{APPROVE}" if action == "activate" else ""))
    assert result == {"error": f"cannot {action} a binding in state '{state}'"}
    assert (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded)) == before


def test_an_action_on_a_binding_that_does_not_exist_is_refused(ns, env):
    assert run(ns, env, f"{ONE};action=suspend") == {"error": "no such binding; create it first"}


@pytest.mark.parametrize("action, extra", [("submit", ""), ("activate", f";{APPROVE}"), ("suspend", ""), ("retire", "")])
def test_dry_run_reports_the_plan_and_writes_nothing(ns, env, action, extra):
    run(ns, env, f"{ONE};action=create")
    if action in ("activate", "suspend"):
        run(ns, env, f"{ONE};action=submit")
    if action == "suspend":
        run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER)
    before = (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded))
    result = run(ns, env, f"{ONE};action={action}{extra};dry_run")
    assert result["dry_run"] is True and result["from"] == before[0] and result["to"]
    assert (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded)) == before


def test_dry_run_of_the_whole_retire_path_lists_every_step(ns, env):
    activate(ns, env)
    result = run(ns, env, f"{ONE};action=retire;dry_run")
    assert (result["from"], result["to"]) == ("active", ["retiring", "retired"])


# --- fail closed -------------------------------------------------------------------------------

def test_a_change_whose_audit_event_cannot_be_written_is_rolled_back_with_its_revocations(ns, env):
    activate(ns, env)
    ref = ns.adm.admit_execution(env.module(), env.request(), run_id="run-1").context_ref
    env.record_raises = RuntimeError("outbox full")
    result = run(ns, env, f"{ONE};action=suspend")
    assert result == {"error": "failed; nothing was changed (is the audit trail available?)"}
    row = binding(env, ns)
    assert (row.state, row.revocation_epoch) == ("active", 0)
    [context] = env.query(1, ns.models.ExecutionSecurityContext)
    assert str(context.id) == ref and context.state == "admitted"


def test_create_without_an_audit_trail_writes_nothing(ns, env):
    env.has_audit_method = False
    ns.install_fakes(env)
    assert "error" in run(ns, env, f"{ONE};action=create")
    assert env.query(1, ns.models.AgentIdentityBinding) == []


def test_a_lost_race_is_reported_and_rolled_back(ns, env, monkeypatch):
    run(ns, env, f"{ONE};action=create")
    monkeypatch.setattr(ns.binding_admin, "transition_binding", lambda *args, **kwargs: False)
    result = run(ns, env, f"{ONE};action=submit")
    assert result == {"error": "the binding changed while this ran; read it again and retry"}
    assert binding(env, ns).state == "draft" and len(env.recorded) == 1


def test_the_failure_message_never_echoes_the_cause(ns, env):
    env.record_raises = RuntimeError("secret-looking detail eyJhbGciOi")
    result = run(ns, env, f"{ONE};action=create")
    assert "eyJ" not in str(result) and "eyJ" not in "\n".join(env.log.lines)


def test_the_task_works_with_governance_off_and_on_alike(ns, env):
    off = run(ns, env, f"{ONE};action=create", module=env.module(governed=False))
    assert off["state"] == "draft"
    env.reset()
    env.add_version(1, 70, 7)
    env.digests[70] = DIGEST
    assert run(ns, env, f"{ONE};action=create", module=env.module())["state"] == "draft"


# --- self-approval (#7105) --------------------------------------------------------------------------

def values_of(ns, param, **more):
    values, error = parsed(ns, param)
    assert error is None
    return {**values, **more}


def test_the_creator_cannot_approve_their_own_binding(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit", actor=APPROVER)
    events = len(env.recorded)
    result = run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=7)
    assert result["error"].startswith("SELF_APPROVAL")
    assert (binding(env, ns).state, len(env.recorded)) == ("pending_approval", events)


def test_whoever_submitted_the_binding_cannot_approve_it(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit", actor=9)
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=9)["error"].startswith("SELF_APPROVAL")
    assert binding(env, ns).state == "pending_approval"
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER)["state"] == "active"


def test_the_task_without_an_identified_operator_cannot_approve(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit")
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=0)["error"].startswith("SELF_APPROVAL")


def test_a_resume_is_not_an_approval_and_the_one_who_suspended_may_resume(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=suspend", actor=APPROVER)
    assert run(ns, env, f"{ONE};action=resume", actor=APPROVER)["state"] == "active"


# --- rebind (#7105) ---------------------------------------------------------------------------------

NEW_DIGEST = "adv1:" + "2" * 64


def test_rebind_takes_an_active_binding_back_to_approval_for_the_definition_as_it_is(ns, env):
    activate(ns, env)
    ref = ns.adm.admit_execution(env.module(), env.request(), run_id="run-1").context_ref
    env.digests[70] = NEW_DIGEST   # the agent was edited
    result = run(ns, env, f"{ONE};action=rebind", actor=9)
    row = binding(env, ns)
    assert result == {"action": "rebind", "binding_ref": str(row.id), "state": "pending_approval", "dry_run": False}
    assert (row.state, row.definition_digest, row.approval_ref, row.policy_revision) == (
        "pending_approval", NEW_DIGEST, None, None)
    assert (row.revocation_epoch, row.state_changed_by, row.created_by) == (1, 9, 7)
    [context] = env.query(1, ns.models.ExecutionSecurityContext)
    assert str(context.id) == ref and (context.state, context.revoked_reason) == ("revoked", "EPOCH_MISMATCH")
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["revocation_epoch"], event["outcome"], event["caller_ref"]) == (
        "BINDING_REBIND_PENDING_APPROVAL", 1, "success", "operator:9")
    assert "approver_ref" not in event and "policy_revision" not in event


def test_a_rebound_binding_admits_nothing_until_it_is_approved_again(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=rebind", actor=9)
    with pytest.raises(ns.adm.AdmissionDenied):
        ns.adm.admit_execution(env.module(), env.request(), run_id="run-2")
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=10)["state"] == "active"
    assert ns.adm.admit_execution(env.module(), env.request(), run_id="run-3").context_ref


def test_a_rebound_binding_is_approved_by_neither_the_creator_nor_the_rebinder(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=rebind", actor=9)
    for actor in (7, 9):
        assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=actor)["error"].startswith("SELF_APPROVAL")
    # the first approver (8) may approve the new definition: they did not create or rebind it
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER)["state"] == "active"


def test_a_rebind_is_approved_for_the_new_definition_only(ns, env):
    activate(ns, env)
    env.digests[70] = NEW_DIGEST
    run(ns, env, f"{ONE};action=rebind", actor=9)
    env.digests[70] = "adv1:" + "3" * 64   # edited again before the approval
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=10)["error"].startswith("BINDING_DIGEST_MISMATCH")
    env.digests[70] = NEW_DIGEST
    assert run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=10)["state"] == "active"


def test_rebind_works_from_suspended_and_raises_the_epoch_once_more(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=suspend")
    env.digests[70] = NEW_DIGEST
    assert run(ns, env, f"{ONE};action=rebind", actor=9)["state"] == "pending_approval"
    row = binding(env, ns)
    assert (row.revocation_epoch, row.definition_digest, row.approval_ref) == (2, NEW_DIGEST, None)
    assert reasons(env)[-2:] == ["BINDING_SUSPENDED", "BINDING_REBIND_PENDING_APPROVAL"]


@pytest.mark.parametrize("state", ["draft", "pending_approval", "retired"])
def test_rebind_is_refused_where_there_is_nothing_live_to_rebind(ns, env, state):
    run(ns, env, f"{ONE};action=create")
    if state != "draft":
        run(ns, env, f"{ONE};action=submit")
    if state == "retired":
        run(ns, env, f"{ONE};action=retire")
    before = (binding(env, ns).state, binding(env, ns).definition_digest, len(env.recorded))
    assert run(ns, env, f"{ONE};action=rebind") == {"error": f"cannot rebind a binding in state '{state}'"}
    assert (binding(env, ns).state, binding(env, ns).definition_digest, len(env.recorded)) == before


def test_rebind_dry_run_reports_the_plan_and_writes_nothing(ns, env):
    activate(ns, env)
    before = (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded))
    result = run(ns, env, f"{ONE};action=rebind;dry_run")
    assert (result["from"], result["to"], result["dry_run"]) == ("active", ["pending_approval"], True)
    assert (binding(env, ns).state, binding(env, ns).revocation_epoch, len(env.recorded)) == before


def test_a_rebind_whose_audit_event_cannot_be_written_changes_nothing(ns, env):
    activate(ns, env)
    env.digests[70] = NEW_DIGEST
    env.record_raises = RuntimeError("outbox full")
    assert "error" in run(ns, env, f"{ONE};action=rebind")
    row = binding(env, ns)
    assert (row.state, row.revocation_epoch, row.approval_ref, row.definition_digest) == ("active", 0, "CHG-12", DIGEST)


def test_the_parser_knows_rebind(ns):
    assert parsed(ns, f"{ONE};action=rebind")[0]["action"] == "rebind"


# --- run_binding_action: the one function the task and the API call (#7105) -----------------------------

def raised(ns, env, param, actor=7, **more):
    with pytest.raises(ns.binding_admin.BindingActionError) as err:
        ns.binding_admin.run_binding_action(env.module(governed=False), values_of(ns, param, **more), actor)
    return err.value


@pytest.mark.parametrize("setup, param, more, code", [
    ([], f"{ONE};action=suspend", {}, "NOT_FOUND"),
    (["create"], f"{ONE};action=create", {}, "BINDING_EXISTS"),
    (["create"], f"{ONE};action=resume", {}, "INVALID_STATE"),
    (["create"], f"{ONE};action=submit", {"expected_state": "active"}, "CONCURRENT_CHANGE"),
    (["create", "submit"], f"{ONE};action=activate;{APPROVE}", {}, "SELF_APPROVAL"),
    (["create", "submit"], f"{ONE};action=activate", {}, "APPROVAL_REQUIRED"),
    ([], "project_id=1;application_id=8;version_id=70;mode=elitea_native;action=create", {}, "VERSION_NOT_FOUND"),
])
def test_a_refusal_carries_a_stable_code_and_nothing_is_written(ns, env, setup, param, more, code):
    for action in setup:
        run(ns, env, f"{ONE};action={action}")
    before = (len(env.query(1, ns.models.AgentIdentityBinding)), len(env.recorded))
    assert raised(ns, env, param, **more).code == code
    assert (len(env.query(1, ns.models.AgentIdentityBinding)), len(env.recorded)) == before


def test_the_messages_of_a_refusal_name_no_id(ns, env):
    env.digests.pop(70)
    err = raised(ns, env, f"{ONE};action=create")
    assert err.code == "VERSION_NOT_FOUND" and err.message == "the version does not exist in this application"
    assert err.task_text.startswith("VERSION_NOT_FOUND")   # the task prints what it always printed


def test_an_unexpected_failure_is_the_audit_message_with_its_own_code(ns, env):
    env.record_raises = RuntimeError("outbox full")
    err = raised(ns, env, f"{ONE};action=create")
    assert (err.code, err.message) == ("AUDIT_UNAVAILABLE", "failed; nothing was changed (is the audit trail available?)")


def test_a_row_is_named_by_id_and_only_inside_its_project(ns, env):
    run(ns, env, f"{ONE};action=create")
    other_project = "project_id=2;application_id=7;version_id=70;mode=elitea_native;action=submit"
    env.add_version(2, 70, 7)
    mine = str(binding(env, ns).id)
    assert raised(ns, env, other_project, binding_id=uuid.UUID(mine)).code == "NOT_FOUND"
    assert binding(env, ns).state == "draft" and env.query(2, ns.models.AgentIdentityBinding) == []
    result = ns.binding_admin.run_binding_action(
        env.module(governed=False), values_of(ns, f"{ONE};action=submit", binding_id=uuid.UUID(mine)), 7)
    assert result["state"] == "pending_approval"


def test_the_task_text_of_every_code_is_unchanged(ns, env):
    """The task prints the plain message, or ``CODE: message`` for the codes that always had a prefix."""
    assert run(ns, env, f"{ONE};action=suspend") == {"error": "no such binding; create it first"}
    run(ns, env, f"{ONE};action=create")
    assert run(ns, env, f"{ONE};action=create") == {"error": "binding already exists; use another action"}
    assert run(ns, env, f"{ONE};action=resume") == {"error": "cannot resume a binding in state 'draft'"}
    run(ns, env, f"{ONE};action=submit")
    assert run(ns, env, f"{ONE};action=activate")["error"].startswith(
        "APPROVAL_REQUIRED: activation needs an approval and a policy revision")


# --- the approval is the approver's own (#7105 review) ----------------------------------------------------

@pytest.mark.parametrize("action", ["create", "submit", "suspend", "resume", "retire", "rebind"])
@pytest.mark.parametrize("field", ["approval_ref=CHG-1", "policy_revision=rev-1"])
def test_the_parser_refuses_approval_fields_for_any_action_but_activate(ns, action, field):
    values, error = parsed(ns, f"{ONE};action={action};{field}")
    assert values is None and error == "approval_ref and policy_revision belong to action=activate only"


@pytest.mark.parametrize("value", ["", "has space", "line\nbreak", "x" * 129, "semi;colon", "a\tb", "ü", "a=b"])
def test_the_parser_holds_an_approval_to_printable_reference_text(ns, value):
    for key in ("approval_ref", "policy_revision"):
        other = "policy_revision=rev-1" if key == "approval_ref" else "approval_ref=CHG-1"
        values, error = parsed(ns, f"{ONE};action=activate;{key}={value};{other}")
        assert values is None and error is not None, (key, value)


@pytest.mark.parametrize("value", ["CHG-12", "a.b_c:d/e@f-g", "x" * 128, "2026-10-20.1"])
def test_the_parser_accepts_reference_text(ns, value):
    assert parsed(ns, f"{ONE};action=activate;approval_ref={value};policy_revision={value}")[1] is None


def test_submit_with_an_approval_is_refused_and_writes_nothing(ns, env):
    run(ns, env, f"{ONE};action=create")
    result = run(ns, env, f"{ONE};action=submit;{APPROVE}")
    assert "belong to action=activate only" in result["error"]
    assert binding(env, ns).state == "draft" and binding(env, ns).approval_ref is None and len(env.recorded) == 1


def test_activate_without_the_approval_fields_is_refused_whatever_the_row_holds(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit")
    for param in (f"{ONE};action=activate", f"{ONE};action=activate;approval_ref=CHG-1",
                  f"{ONE};action=activate;policy_revision=rev-1"):
        assert run(ns, env, param, actor=APPROVER)["error"].startswith("APPROVAL_REQUIRED")
    assert binding(env, ns).state == "pending_approval"


def test_the_audit_names_the_approvers_own_reference(ns, env):
    run(ns, env, f"{ONE};action=create")
    run(ns, env, f"{ONE};action=submit")
    run(ns, env, f"{ONE};action=activate;approval_ref=MINE-1;policy_revision=pol-9", actor=APPROVER)
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["approver_ref"], event["policy_revision"]) == ("BINDING_ACTIVE", "MINE-1", "pol-9")
    assert (binding(env, ns).approval_ref, binding(env, ns).policy_revision) == ("MINE-1", "pol-9")
    assert all("approver_ref" not in event for _, event in env.recorded[:-1])


def test_a_resume_keeps_the_stored_approval_and_ignores_whatever_the_values_carry(ns, env):
    activate(ns, env)
    run(ns, env, f"{ONE};action=suspend")
    result = ns.binding_admin.run_binding_action(
        env.module(governed=False),
        values_of(ns, f"{ONE};action=resume", approval_ref="EVIL-1", policy_revision="EVIL-2"), 7)
    row = binding(env, ns)
    assert result["state"] == "active" and (row.approval_ref, row.policy_revision) == ("CHG-12", "rev-1")
    event = env.recorded[-1][1]
    assert (event["approver_ref"], event["policy_revision"]) == ("CHG-12", "rev-1")


@pytest.mark.parametrize("action", ["submit", "suspend", "retire", "rebind"])
def test_no_other_action_stamps_the_approval_even_when_the_values_carry_one(ns, env, action):
    if action == "submit":
        run(ns, env, f"{ONE};action=create")
    else:
        activate(ns, env)
    before = (binding(env, ns).approval_ref, binding(env, ns).policy_revision)
    ns.binding_admin.run_binding_action(
        env.module(governed=False), values_of(ns, f"{ONE};action={action}", approval_ref="EVIL-1", policy_revision="EVIL-2"), 7)
    row = binding(env, ns)
    assert "EVIL-1" not in (row.approval_ref, row.policy_revision) and "EVIL-2" not in (row.approval_ref, row.policy_revision)
    if action in ("suspend", "retire"):
        assert (row.approval_ref, row.policy_revision) == before
    assert all("EVIL" not in json_text(event) for _, event in env.recorded)


def json_text(value):
    import json
    return json.dumps(value, default=str)


# --- a binding whose version is gone (#7105 review) ----------------------------------------------------------

def drop_version(ns, env, version_id=70):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.query(ns.ApplicationVersion).filter_by(id=version_id).delete()
        session.commit()


@pytest.mark.parametrize("action", ["suspend", "retire"])
def test_a_binding_can_be_taken_out_of_use_after_its_version_was_deleted(ns, env, action):
    activate(ns, env)
    drop_version(ns, env)
    result = run(ns, env, f"{ONE};action={action}")
    assert "error" not in result and binding(env, ns).state == {"suspend": "suspended", "retire": "retired"}[action]


def test_submit_of_a_draft_works_on_the_row_alone(ns, env):
    run(ns, env, f"{ONE};action=create")
    drop_version(ns, env)
    assert run(ns, env, f"{ONE};action=submit")["state"] == "pending_approval"


@pytest.mark.parametrize("action", ["activate", "resume", "rebind", "create"])
def test_what_reads_the_definition_still_needs_the_version(ns, env, action):
    if action == "create":
        drop_version(ns, env)
    else:
        activate(ns, env) if action == "rebind" else run(ns, env, f"{ONE};action=create")
        if action == "resume":
            run(ns, env, f"{ONE};action=submit")
            run(ns, env, f"{ONE};action=activate;{APPROVE}", actor=APPROVER)
            run(ns, env, f"{ONE};action=suspend")
        if action == "activate":
            run(ns, env, f"{ONE};action=submit")
        drop_version(ns, env)
    before = (len(env.query(1, ns.models.AgentIdentityBinding)), len(env.recorded))
    err = raised(ns, env, f"{ONE};action={action}" + (f";{APPROVE}" if action == "activate" else ""), actor=APPROVER)
    assert err.code == "VERSION_NOT_FOUND" and err.message == "the version does not exist in this application"
    assert (len(env.query(1, ns.models.AgentIdentityBinding)), len(env.recorded)) == before


# --- two creates that race (#7105 review) -----------------------------------------------------------------------

def test_a_create_that_loses_the_race_to_the_unique_key_is_binding_exists_not_an_audit_failure(ns, env, monkeypatch):
    run(ns, env, f"{ONE};action=create")
    events = len(env.recorded)
    # the pre-check does not see the other writer's row yet: the INSERT meets the unique constraint
    monkeypatch.setattr(ns.binding_admin, "_find", lambda values: ns.sa.select(ns.models.AgentIdentityBinding).where(ns.sa.false()))
    err = raised(ns, env, f"{ONE};action=create")
    assert (err.code, err.message) == ("BINDING_EXISTS", "binding already exists; use another action")
    assert err.task_text == err.message
    assert len(env.query(1, ns.models.AgentIdentityBinding)) == 1 and len(env.recorded) == events


# --- the rebind event carries both digests (#7105 review) -------------------------------------------------------

def test_the_rebind_event_names_the_new_definition_and_the_previous_one(ns, env):
    activate(ns, env)
    env.digests[70] = NEW_DIGEST
    run(ns, env, f"{ONE};action=rebind", actor=9)
    event = env.recorded[-1][1]
    assert (event["target_ref"], event["operation_id"]) == (NEW_DIGEST, f"previous:{DIGEST}")
    assert all("target_ref" not in other and "operation_id" not in other for _, other in env.recorded[:-1])


# --- the admin task method ---------------------------------------------------------------------

@pytest.fixture
def admin_tasks(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models", "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [str(PLUGIN_ROOT)]
        sys.modules.setdefault(package, module)
    fake = types.ModuleType("elitea_core.utils.binding_admin")
    fake.calls = []

    def manage_binding(module, param, actor_user_id=0):
        fake.calls.append((module, param, actor_user_id))
        return {"action": "create", "definition_digest": "adv1:x", "state": "draft"}

    fake.manage_binding = manage_binding
    sys.modules[fake.__name__] = fake
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"))
    loaded = importlib.util.module_from_spec(spec)
    loaded.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    loaded.binding_fake = fake
    return loaded


def test_the_task_hands_the_parameter_and_the_executing_admin_to_the_action(admin_tasks):
    pylon_module = types.SimpleNamespace()
    result = admin_tasks.Method.manage_agent_identity_binding(pylon_module, param=ONE + ";action=create", _executing_user_id=11)
    assert result["state"] == "draft"
    assert admin_tasks.binding_fake.calls == [(pylon_module, ONE + ";action=create", 11)]


def test_the_task_tolerates_a_missing_parameter_and_admin(admin_tasks):
    admin_tasks.Method.manage_agent_identity_binding(types.SimpleNamespace())
    assert admin_tasks.binding_fake.calls[0][1:] == ("", 0)


def test_the_task_is_documented_for_operators():
    doc = (PLUGIN_ROOT / "methods" / "admin_tasks.py").read_text()
    section = doc[doc.index("def manage_agent_identity_binding"):doc.index("def migrate_eval_dataset_columns")]
    for word in ("project_id=N", "application_id=A", "version_id=V", "mode=", "action=", "approval_ref", "policy_revision", "dry_run"):
        assert word in section
