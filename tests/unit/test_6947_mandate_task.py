"""C-1009 (#6947): the ``manage_autonomous_mandate`` admin task - parser, lifecycle actions, audit, refusals.

Real models and statements on SQLite files; see ``fixtures/mandate_env.py``.
"""
import re
import sys
import types
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER, add_mandate, mandate_row  # noqa: E402  pylint: disable=C0413

UNTIL = (NOW + timedelta(days=30)).isoformat()
CREATE = f"project_id=1;action=create;version_id=70;owner_user_id={OWNER};operations=run:schedule;max_runtime_s=600;valid_until={UNTIL}"
CREATOR, APPROVER = 3, 1
APPROVE = "approval_ref=CHG-12;policy_revision=rev-1"
DIGEST = "adv1:" + "1" * 64


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_task"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    mandate_env.prepare(ns, ns.env)
    ns.env.digests[70] = DIGEST
    ns.revoked = []
    ns.mandate_admin.revoke_mandate_contexts = lambda session, mandate_id, reason="MANDATE_INACTIVE": ns.revoked.append(
        (session, mandate_id, reason)) or 0
    return ns.env


@pytest.fixture
def agent(ns, env):
    """An application version with one active binding; returns the binding id."""
    return env.seed_agent(1, 7, 70)


def run(ns, env, param, actor=CREATOR, module=None):
    return ns.mandate_admin.manage_mandate(module or env.module(governed=False), param, actor)


def create(ns, env, **kwargs):
    result = run(ns, env, CREATE, **kwargs)
    assert "error" not in result, result
    return uuid.UUID(result["mandate_ref"])


def to_state(ns, env, state):
    """Walk a fresh mandate to ``state`` through the task; returns its id."""
    mandate_id = create(ns, env)
    if state == "draft":
        return mandate_id
    assert run(ns, env, f"project_id=1;action=submit;mandate={mandate_id}")["state"] == "pending_approval"
    if state != "pending_approval":
        approved = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
        assert approved.get("state") == "active", approved
    if state == "suspended":
        assert run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}")["state"] == "suspended"
    return mandate_id


def events(env, event_type=None):
    return [event for _, event in env.recorded if event_type in (None, event["event_type"])]


# --- parser -----------------------------------------------------------------------------------

def parsed(ns, param):
    return ns.mandate_admin.parse_mandate_param(param)


def test_a_full_create_parameter_is_parsed(ns, env):
    values, error = parsed(ns, f" {CREATE} ;TARGETS=*;DRY_RUN;")
    assert error is None
    assert values == {
        "dry_run": True, "project_id": 1, "action": "create", "version_id": 70, "owner_user_id": OWNER,
        "operations": ["run:schedule"], "max_runtime_s": 600, "valid_until": NOW + timedelta(days=30), "targets": "*"}


def test_operations_are_a_deduplicated_list_in_the_order_given(ns, env):
    values, _ = parsed(ns, CREATE.replace("run:schedule", "run:webhook, run:schedule,run:webhook"))
    assert values["operations"] == ["run:webhook", "run:schedule"]


def test_the_validity_is_normalised_to_utc(ns, env):
    values, _ = parsed(ns, CREATE.replace(UNTIL, "2026-11-20T14:00:00+02:00"))
    assert values["valid_until"] == NOW + timedelta(days=31)
    assert values["valid_until"].utcoffset() == timedelta(0) and values["valid_until"].hour == 12


@pytest.mark.parametrize("action,extra", [
    ("submit", ""), ("reject", ""), ("suspend", ""), ("resume", ""), ("revoke", ""),
    ("approve", ";approval_ref=a;policy_revision=p")])
def test_the_other_actions_take_a_mandate(ns, env, action, extra):
    mandate_id = uuid.uuid4()
    values, error = parsed(ns, f"project_id=1;action={action};mandate={mandate_id}{extra}")
    assert error is None and values["mandate"] == mandate_id


@pytest.mark.parametrize("param, fragment", [
    (None, "project_id, action required"),
    ("", "required"),
    ("dry_run", "required"),
    ("action=create", "project_id required"),
    ("project_id=1", "action required"),
    ("project_id=1;action=delete", "invalid action: 'delete'"),
    ("project_id=x;action=create", "invalid project_id: 'x'"),
    ("project_id=0;action=submit;mandate=" + str(uuid.uuid4()), "invalid project_id: '0'"),
    (CREATE.replace("version_id=70;", ""), "exactly one of version_id and toolkit_id required"),
    (CREATE + ";toolkit_id=4", "exactly one of version_id and toolkit_id required"),   # #7305: not both
    (CREATE.replace("run:schedule", "run:index_schedule"), "invalid operations"),   # a toolkit mandate's only
    (CREATE.replace(f"owner_user_id={OWNER};", ""), "owner_user_id required"),
    (CREATE.replace("operations=run:schedule;", ""), "operations required"),
    (CREATE.replace("max_runtime_s=600;", ""), "max_runtime_s required"),
    (CREATE.replace(f";valid_until={UNTIL}", ""), "valid_until required"),
    (CREATE + ";color=red", "unknown parameter 'color=red'"),
    (CREATE + ";project", "unknown parameter 'project'"),
    (CREATE.replace("version_id=70", "version_id=1.5"), "invalid version_id"),
    (CREATE.replace("owner_user_id=5", "owner_user_id=-5"), "invalid owner_user_id"),
    (CREATE.replace("run:schedule", "run:everything"), "invalid operations"),
    (CREATE.replace("run:schedule", "run:schedule,tool:call"), "invalid operations"),
    (CREATE + ";targets=repo:x", "invalid targets: 'repo:x'"),
    (CREATE.replace("max_runtime_s=600", "max_runtime_s=59"), "invalid max_runtime_s: '59'"),
    (CREATE.replace("max_runtime_s=600", "max_runtime_s=86401"), "invalid max_runtime_s: '86401'"),
    (CREATE.replace(UNTIL, "tomorrow"), "invalid valid_until: 'tomorrow'"),
    (CREATE.replace(UNTIL, "2026-11-20T12:00:00"), "needs an offset"),
    (CREATE.replace(UNTIL, "2026-10-20T12:00:00+00:00"), "must be in the future"),
    (CREATE.replace(UNTIL, "2026-10-19T12:00:00+00:00"), "must be in the future"),
    (CREATE.replace(UNTIL, (NOW + timedelta(days=180, seconds=1)).isoformat()), "at most 180 days ahead"),
    (CREATE.replace(UNTIL, (NOW + timedelta(days=181)).isoformat()), "at most 180 days ahead"),
    ("project_id=1;action=submit", "mandate required"),
    ("project_id=1;action=submit;mandate=nope", "invalid mandate: 'nope'"),
    (f"project_id=1;action=approve;mandate={uuid.uuid4()}", "approval_ref, policy_revision required"),
    (f"project_id=1;action=submit;mandate={uuid.uuid4()};version_id=70", "version_id does not apply to submit"),
    (f"project_id=1;action=submit;mandate={uuid.uuid4()};approval_ref=a", "approval_ref does not apply to submit"),
    (CREATE + f";mandate={uuid.uuid4()}", "mandate does not apply to create"),
    (CREATE + ";approval_ref=a", "approval_ref does not apply to create"),
])
def test_a_bad_parameter_is_a_readable_error(ns, env, param, fragment):
    values, error = parsed(ns, param)
    assert values is None and fragment in error


def test_the_validity_ceiling_is_inclusive_at_exactly_180_days(ns, env):
    values, error = parsed(ns, CREATE.replace(UNTIL, (NOW + timedelta(days=180)).isoformat()))
    assert error is None and values["valid_until"] == NOW + timedelta(days=180)


@pytest.mark.parametrize("runtime", [60, 86400])
def test_the_runtime_range_is_inclusive(ns, env, runtime):
    assert parsed(ns, CREATE.replace("max_runtime_s=600", f"max_runtime_s={runtime}"))[1] is None


@pytest.mark.parametrize("param", [None, "", "action=create", "project_id=1;action=nope", CREATE.replace("version_id=70", "version_id=x")])
def test_a_bad_parameter_changes_nothing(ns, env, agent, param):
    result = run(ns, env, param)
    assert list(result) == ["error"]
    assert env.recorded == [] and env.query(1, ns.Mandate) == []


# --- create -----------------------------------------------------------------------------------

def test_create_inserts_a_draft_bound_to_the_active_binding(ns, env, agent):
    result = run(ns, env, CREATE)
    [row] = env.query(1, ns.Mandate)
    assert result == {"action": "create", "mandate_ref": str(row.id), "revision": 1, "state": "draft", "dry_run": False}
    assert (row.state, row.revision, row.revocation_epoch, row.created_by, row.state_changed_by) == ("draft", 1, 0, CREATOR, CREATOR)
    assert (row.project_id, row.application_id, row.agent_version, row.agent_binding_ref) == (1, 7, 70, agent)
    assert (row.owner_user_id, row.allowed_operations, row.allowed_targets, row.max_runtime_s) == (
        OWNER, ["run:schedule"], ["*"], 600)
    assert row.owner_user_ref == {"user_id": OWNER, "issuer": "https://issuer.example", "tid": "tid-1", "oid": "oid-5"}
    assert row.valid_until.replace(tzinfo=None) == (NOW + timedelta(days=30)).replace(tzinfo=None)
    assert (row.approved_by, row.approval_ref, row.policy_revision) == (None, None, None)


def test_create_writes_one_lifecycle_event_in_the_session_that_inserted(ns, env, agent):
    result = run(ns, env, CREATE)
    [(event_session, event)] = env.recorded
    assert event_session is env.sessions[-1][1]
    assert event["event_type"] == "lifecycle" and event["reason_code"] == "MANDATE_DRAFT" and event["outcome"] == "success"
    assert (event["project_id"], event["application_id"], event["mandate_ref"]) == (1, 7, result["mandate_ref"])
    assert event["agent_binding_ref"] == str(agent) and event["caller_ref"] == f"operator:{CREATOR}"
    assert event["revocation_epoch"] == 0 and "user_ref" not in event


def test_the_operations_of_a_webhook_mandate_are_stored(ns, env, agent):
    run(ns, env, CREATE.replace("run:schedule", "run:schedule,run:webhook"))
    assert env.query(1, ns.Mandate)[0].allowed_operations == ["run:schedule", "run:webhook"]


def test_a_dry_run_creates_nothing(ns, env, agent):
    result = run(ns, env, CREATE + ";dry_run")
    assert result == {"dry_run": True, "action": "create", "state": "draft", "revision": 1}
    assert env.query(1, ns.Mandate) == [] and env.recorded == []


def test_a_dry_run_needs_no_operator(ns, env, agent):
    assert run(ns, env, CREATE + ";dry_run", actor=0)["dry_run"] is True


@pytest.mark.parametrize("actor", [0, None, True, "3", -1])
def test_a_write_needs_a_real_operator(ns, env, agent, actor):
    result = run(ns, env, CREATE, actor=actor)
    assert result["code"] == "ACTOR_REQUIRED"
    assert env.query(1, ns.Mandate) == [] and env.recorded == []


def test_create_is_refused_for_a_version_that_does_not_exist(ns, env, agent):
    assert run(ns, env, CREATE.replace("version_id=70", "version_id=71"))["code"] == "VERSION_NOT_FOUND"


@pytest.mark.parametrize("state", ["draft", "pending_approval", "suspended", "retiring", "retired"])
def test_create_is_refused_without_an_active_binding(ns, env, state):
    env.seed_agent(1, 7, 70, state=state)
    result = run(ns, env, CREATE)
    assert result["code"] == "DENIED_BINDING"
    assert env.query(1, ns.Mandate) == [] and env.recorded == []


def test_create_is_refused_for_a_binding_of_another_version(ns, env):
    env.seed_agent(1, 7, 71)
    env.add_version(1, 70, 7)
    assert run(ns, env, CREATE)["code"] == "DENIED_BINDING"


def test_create_is_refused_when_two_bindings_are_active(ns, env, agent):
    env.add_binding(1, 7, 70, mode="dedicated_agent")
    assert run(ns, env, CREATE)["code"] == "DENIED_BINDING"


def test_a_runtime_domain_binding_counts_only_with_the_operators_allow(ns, env):
    env.seed_agent(1, 7, 70, mode="runtime_domain_binding")
    assert run(ns, env, CREATE)["code"] == "DENIED_BINDING"
    assert "error" not in run(ns, env, CREATE, module=env.module(governed=False, allow_runtime_domain_binding=True))


def test_create_is_refused_when_the_definition_changed_since_the_binding(ns, env, agent):
    env.digests[70] = "adv1:" + "2" * 64
    result = run(ns, env, CREATE)
    assert result["code"] == "BINDING_DIGEST_MISMATCH"
    assert env.query(1, ns.Mandate) == []


def test_a_definition_that_cannot_be_digested_is_a_readable_error(ns, env, agent, monkeypatch):
    def boom(session, version_id):
        raise ns.digest.DigestError("VERSION_NOT_FOUND", "no such version 70")

    monkeypatch.setattr(ns.mandate_admin, "digest_for_version", boom)
    result = run(ns, env, CREATE)
    assert result == {"error": "VERSION_NOT_FOUND: the agent definition could not be read", "code": "VERSION_NOT_FOUND"}


def test_create_is_refused_for_an_owner_that_does_not_exist(ns, env, agent):
    assert run(ns, env, CREATE.replace("owner_user_id=5", "owner_user_id=6"))["code"] == "OWNER_NOT_FOUND"


@pytest.mark.parametrize("missing", ["idp_issuer", "idp_tid", "idp_oid"])
@pytest.mark.parametrize("value", [None, ""])
def test_create_is_refused_for_an_owner_without_a_linked_subject(ns, env, agent, missing, value):
    ns.users[OWNER][missing] = value
    result = run(ns, env, CREATE)
    assert result["code"] == "OWNER_NOT_LINKED"
    assert env.query(1, ns.Mandate) == [] and env.recorded == []


@pytest.mark.parametrize("state", ["draft", "pending_approval", "active", "suspended"])
def test_a_second_live_mandate_for_the_version_is_refused(ns, env, agent, state):
    to_state(ns, env, state)
    env.recorded.clear()
    assert run(ns, env, CREATE)["code"] == "MANDATE_EXISTS"
    assert len(env.query(1, ns.Mandate)) == 1 and env.recorded == []


def test_a_new_revision_is_accepted_after_a_revoke(ns, env, agent):
    first = create(ns, env)
    assert run(ns, env, f"project_id=1;action=revoke;mandate={first}")["state"] == "revoked"
    dry = run(ns, env, CREATE + ";dry_run")
    second = run(ns, env, CREATE)
    assert dry["revision"] == second["revision"] == 2
    assert mandate_row(env, ns, first).state == "revoked"


def test_a_lost_insert_race_is_reported_and_rolled_back(ns, env, agent, monkeypatch):
    monkeypatch.setattr(ns.mandate_admin, "_live_mandate", lambda session, project_id, *args: None)
    create(ns, env)
    env.recorded.clear()
    result = run(ns, env, CREATE)
    assert result["code"] == "CONCURRENT_CHANGE" and env.recorded == []
    assert len(env.query(1, ns.Mandate)) == 1


def test_a_create_whose_audit_event_cannot_be_written_is_rolled_back(ns, env, agent):
    env.record_raises = RuntimeError("outbox down")
    result = run(ns, env, CREATE)
    assert result == {"error": "failed; nothing was changed (is the audit trail available?)", "code": "FAILED"}
    assert env.query(1, ns.Mandate) == []


# --- lifecycle ----------------------------------------------------------------------------------

def test_submit_then_approve_by_someone_else_activates(ns, env, agent):
    mandate_id = to_state(ns, env, "pending_approval")
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
    assert result == {"action": "approve", "mandate_ref": str(mandate_id), "state": "active", "dry_run": False}
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.approved_by, row.approval_ref, row.policy_revision) == ("active", APPROVER, "CHG-12", "rev-1")
    assert [(e["event_type"], e["reason_code"]) for e in events(env)] == [
        ("lifecycle", "MANDATE_DRAFT"), ("lifecycle", "MANDATE_PENDING_APPROVAL"), ("approval", "MANDATE_ACTIVE")]
    approval = events(env, "approval")[0]
    assert approval["approver_ref"] == "CHG-12" and approval["policy_revision"] == "rev-1"
    assert approval["caller_ref"] == f"operator:{APPROVER}" and approval["mandate_ref"] == str(mandate_id)
    assert ns.revoked == []


@pytest.mark.parametrize("actor,name", [(CREATOR, "creator"), (OWNER, "owner")])
def test_the_creator_and_the_owner_cannot_approve(ns, env, agent, actor, name):
    mandate_id = to_state(ns, env, "pending_approval")
    env.recorded.clear()
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", actor)
    assert result["code"] == "SELF_APPROVAL", name
    assert mandate_row(env, ns, mandate_id).state == "pending_approval" and env.recorded == []


def test_a_mandate_past_its_validity_cannot_be_approved(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "pending_approval")
    monkeypatch.setattr(ns.mandate_admin, "_now", lambda: NOW + timedelta(days=30))
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
    assert result["code"] == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, mandate_id).state == "pending_approval"


@pytest.mark.parametrize("binding_state", ["suspended", "retired"])
def test_approval_needs_the_binding_to_be_active_still(ns, env, agent, binding_state):
    mandate_id = to_state(ns, env, "pending_approval")
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).values(state=binding_state))
        session.commit()
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
    assert result["code"] == "BINDING_NOT_ACTIVE"
    assert mandate_row(env, ns, mandate_id).state == "pending_approval"


def test_approval_twice_is_an_invalid_state(ns, env, agent):
    mandate_id = to_state(ns, env, "active")
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", 9)
    assert result["code"] == "INVALID_STATE"
    assert mandate_row(env, ns, mandate_id).approved_by == APPROVER


def test_a_lost_race_on_approval_is_reported_and_rolled_back(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "pending_approval")
    monkeypatch.setattr(ns.mandate_admin, "approve_mandate", lambda *args, **kwargs: False)
    env.recorded.clear()
    result = run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
    assert result["code"] == "CONCURRENT_CHANGE" and env.recorded == []


def test_reject_sends_a_pending_mandate_back_to_draft(ns, env, agent):
    mandate_id = to_state(ns, env, "pending_approval")
    assert run(ns, env, f"project_id=1;action=reject;mandate={mandate_id}", APPROVER)["state"] == "draft"
    assert events(env)[-1]["reason_code"] == "MANDATE_REJECTED"
    assert run(ns, env, f"project_id=1;action=submit;mandate={mandate_id}")["state"] == "pending_approval"


@pytest.mark.parametrize("action,state", [
    ("submit", "active"), ("submit", "pending_approval"), ("reject", "draft"), ("reject", "active"),
    ("suspend", "draft"), ("suspend", "pending_approval"), ("suspend", "suspended"),
    ("resume", "draft"), ("resume", "active"), ("approve", "draft"), ("revoke", "revoked")])
def test_an_action_that_does_not_apply_to_the_state_is_refused_and_changes_nothing(ns, env, agent, action, state):
    if state == "revoked":
        mandate_id = to_state(ns, env, "draft")
        run(ns, env, f"project_id=1;action=revoke;mandate={mandate_id}")
    else:
        mandate_id = to_state(ns, env, state)
    env.recorded.clear()
    extra = f";{APPROVE}" if action == "approve" else ""
    result = run(ns, env, f"project_id=1;action={action};mandate={mandate_id}{extra}", APPROVER)
    assert result["code"] == "INVALID_STATE"
    assert env.recorded == [] and mandate_row(env, ns, mandate_id).state == state


def test_suspend_raises_the_epoch_revokes_the_contexts_and_is_audited_in_one_transaction(ns, env, agent):
    mandate_id = to_state(ns, env, "active")
    env.recorded.clear()
    result = run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}", 9)
    assert result["state"] == "suspended"
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.suspended_reason, row.revocation_epoch, row.state_changed_by) == ("suspended", "MANUAL", 1, 9)
    [(revoke_session, revoked_id, reason)] = ns.revoked
    [(event_session, event)] = env.recorded
    assert revoke_session is event_session and revoked_id == mandate_id and reason == "MANDATE_INACTIVE"
    assert (event["reason_code"], event["revocation_epoch"]) == ("MANDATE_SUSPENDED", 1)


def test_resume_is_by_someone_other_than_the_owner(ns, env, agent):
    mandate_id = to_state(ns, env, "suspended")
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", OWNER)["code"] == "SELF_APPROVAL"
    assert mandate_row(env, ns, mandate_id).state == "suspended"
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["state"] == "active"
    row = mandate_row(env, ns, mandate_id)
    assert (row.suspended_reason, row.revocation_epoch) == (None, 1)
    assert events(env)[-1]["reason_code"] == "MANDATE_ACTIVE" and len(ns.revoked) == 1  # only the suspension revoked


def test_resume_after_the_owner_was_suspended_needs_the_owner_active_again(ns, env, agent):
    mandate_id = add_mandate(env, ns, state="suspended", suspended_reason="OWNER_SUSPENDED",
                             binding_ref=agent, epoch=1)
    param = f"project_id=1;action=resume;mandate={mandate_id}"
    env.epochs[OWNER] = {"active": False, "user": 2, "token": 0}
    assert run(ns, env, param, APPROVER)["code"] == "OWNER_INACTIVE"
    env.epochs[OWNER] = None
    assert run(ns, env, param, APPROVER)["code"] == "OWNER_INACTIVE"
    env.epochs[OWNER] = {"active": True, "user": 3, "token": 0}
    assert run(ns, env, param, APPROVER)["state"] == "active"


def test_a_manual_suspension_does_not_ask_about_the_owner(ns, env, agent):
    mandate_id = to_state(ns, env, "suspended")
    env.epoch_calls.clear()
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["state"] == "active"
    assert env.epoch_calls == []


def test_a_mandate_past_its_validity_cannot_be_resumed(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "suspended")
    monkeypatch.setattr(ns.mandate_admin, "_now", lambda: NOW + timedelta(days=31))
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["code"] == "MANDATE_EXPIRED"


@pytest.mark.parametrize("state", ["draft", "pending_approval", "active", "suspended"])
def test_revoke_works_from_every_live_state_and_revokes_the_contexts(ns, env, agent, state):
    mandate_id = to_state(ns, env, state)
    ns.revoked.clear()
    assert run(ns, env, f"project_id=1;action=revoke;mandate={mandate_id}")["state"] == "revoked"
    row = mandate_row(env, ns, mandate_id)
    assert row.state == "revoked" and row.revocation_epoch == (2 if state == "suspended" else 1)
    assert [call[1] for call in ns.revoked] == [mandate_id]
    assert events(env)[-1]["reason_code"] == "MANDATE_REVOKED"


def test_revoked_is_final(ns, env, agent):
    mandate_id = to_state(ns, env, "active")
    run(ns, env, f"project_id=1;action=revoke;mandate={mandate_id}")
    for action in ("resume", "suspend", "revoke", "submit"):
        assert run(ns, env, f"project_id=1;action={action};mandate={mandate_id}", APPROVER)["code"] == "INVALID_STATE"


def test_a_mandate_of_another_project_is_not_found(ns, env, agent):
    mandate_id = to_state(ns, env, "draft")
    result = run(ns, env, f"project_id=2;action=revoke;mandate={mandate_id}")
    assert result["code"] == "MANDATE_NOT_FOUND"
    assert run(ns, env, f"project_id=1;action=revoke;mandate={uuid.uuid4()}")["code"] == "MANDATE_NOT_FOUND"
    assert mandate_row(env, ns, mandate_id).state == "draft"


@pytest.mark.parametrize("action", ["submit", "reject", "suspend", "resume", "revoke", "approve"])
def test_a_dry_run_changes_nothing(ns, env, agent, action):
    start = {"submit": "draft", "reject": "pending_approval", "suspend": "active", "resume": "suspended",
             "revoke": "active", "approve": "pending_approval"}[action]
    mandate_id = to_state(ns, env, start)
    env.recorded.clear()
    ns.revoked.clear()
    extra = f";{APPROVE}" if action == "approve" else ""
    result = run(ns, env, f"project_id=1;action={action};mandate={mandate_id}{extra};dry_run", APPROVER)
    assert result["dry_run"] is True and result["from"] == start and result["mandate_ref"] == str(mandate_id)
    assert mandate_row(env, ns, mandate_id).state == start
    assert env.recorded == [] and ns.revoked == []


def test_a_change_whose_audit_event_cannot_be_written_is_rolled_back_with_its_revocations(ns, env, agent):
    mandate_id = to_state(ns, env, "active")
    env.record_raises = RuntimeError("outbox down")
    result = run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}")
    assert result["code"] == "FAILED"
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.revocation_epoch) == ("active", 0)


def test_a_lost_race_on_a_move_is_reported_and_rolled_back(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "active")
    monkeypatch.setattr(ns.mandate_admin, "transition_mandate", lambda *args, **kwargs: False)
    ns.revoked.clear()
    env.recorded.clear()
    assert run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}")["code"] == "CONCURRENT_CHANGE"
    assert env.recorded == [] and ns.revoked == []


# --- what the task never says ------------------------------------------------------------------------

def test_no_message_carries_an_id(ns):
    for code, message in ns.mandate_admin.MESSAGES.items():
        assert not re.search(r"\d|[0-9a-f]{8}-", message), code


def test_neither_the_log_nor_the_results_carry_a_digest_a_subject_or_the_approval(ns, env, agent):
    results = [run(ns, env, CREATE)]
    mandate_id = uuid.UUID(results[0]["mandate_ref"])
    results.append(run(ns, env, f"project_id=1;action=submit;mandate={mandate_id}"))
    results.append(run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER))
    results.append(run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", 9))
    results.append(run(ns, env, CREATE.replace("run:schedule", "run:nothing")))
    text = repr(results) + "\n".join(ns.log.lines) + repr([event for _, event in env.recorded])
    for forbidden in (DIGEST, "oid-5", "tid-1", "issuer.example"):
        assert forbidden not in text
    for result in results:
        assert set(result) <= {"action", "mandate_ref", "revision", "state", "dry_run", "error", "code"}


def test_the_runtime_domain_setting_is_read_from_the_module_not_hardcoded(ns, env):
    module = env.module(governed=False)
    del module.admission_settings
    env.seed_agent(1, 7, 70, mode="dedicated_agent")
    assert "error" not in run(ns, env, CREATE, module=module)


# --- revoking the contexts of a mandate's runs -------------------------------------------------------

def test_revoking_a_mandates_contexts_is_one_conditional_statement_that_does_not_commit(ns):
    statements = []

    class Session:
        def execute(self, statement):
            statements.append(statement)
            return types.SimpleNamespace(rowcount=2)

        def commit(self):
            raise AssertionError("must not commit")

    mandate_id = uuid.uuid4()
    assert ns.adm.revoke_mandate_contexts(Session(), mandate_id, "MANDATE_EXPIRED") == 2
    [statement] = statements
    compiled = statement.compile(dialect=ns.postgresql.dialect())
    sql = str(compiled)
    assert sql.startswith("UPDATE tenant.execution_security_context SET state=")
    assert "execution_security_context.subject @>" in sql and "execution_security_context.state IN" in sql
    assert "revoked_reason" in sql and "ended_at=now()" in sql
    assert {"kind": "mandate", "ref": f"mandate:{mandate_id}"} in compiled.params.values()
    assert "revoked" in compiled.params.values() and "MANDATE_EXPIRED" in compiled.params.values()
    assert ["admitted", "running"] in compiled.params.values()


# --- review fixes (#6947): lazy expiry, bounds, binding, dependency failures ---------------------------------

def overdue(ns, env, monkeypatch, days=31):
    monkeypatch.setattr(ns.mandate_admin, "_now", lambda: NOW + timedelta(days=days))


@pytest.mark.parametrize("state", ["active", "suspended"])
def test_an_overdue_mandate_does_not_hold_the_slot_so_it_can_be_renewed(ns, env, agent, monkeypatch, state):
    old = to_state(ns, env, state)
    overdue(ns, env, monkeypatch)
    env.recorded.clear()
    ns.revoked.clear()
    result = run(ns, env, CREATE.replace(UNTIL, (NOW + timedelta(days=60)).isoformat()))
    assert result["state"] == "draft" and result["revision"] == 2, result
    row = mandate_row(env, ns, old)
    assert row.state == "expired" and row.state_changed_by is None
    assert row.revocation_epoch == (2 if state == "suspended" else 1)
    assert [call[1:] for call in ns.revoked] == [(old, "MANDATE_EXPIRED")]
    assert [(e["reason_code"], e["mandate_ref"]) for e in events(env)] == [
        ("MANDATE_EXPIRED", str(old)), ("MANDATE_DRAFT", result["mandate_ref"])]
    assert events(env)[0]["revocation_epoch"] == row.revocation_epoch


def test_expiry_and_the_new_draft_are_one_transaction(ns, env, agent, monkeypatch):
    old = to_state(ns, env, "active")
    overdue(ns, env, monkeypatch)
    env.record_raises = RuntimeError("outbox down")
    assert run(ns, env, CREATE.replace(UNTIL, (NOW + timedelta(days=60)).isoformat()))["code"] == "FAILED"
    assert mandate_row(env, ns, old).state == "active" and len(env.query(1, ns.Mandate)) == 1


def test_a_mandate_within_its_validity_still_holds_the_slot(ns, env, agent, monkeypatch):
    to_state(ns, env, "active")
    overdue(ns, env, monkeypatch, days=29)
    assert run(ns, env, CREATE)["code"] == "MANDATE_EXISTS"


def test_a_dry_run_of_a_renewal_expires_nothing(ns, env, agent, monkeypatch):
    old = to_state(ns, env, "active")
    overdue(ns, env, monkeypatch)
    env.recorded.clear()
    ns.revoked.clear()
    assert run(ns, env, CREATE.replace(UNTIL, (NOW + timedelta(days=60)).isoformat()) + ";dry_run") == {
        "dry_run": True, "action": "create", "state": "draft", "revision": 2}
    assert mandate_row(env, ns, old).state == "active" and env.recorded == [] and ns.revoked == []


@pytest.mark.parametrize("action,state", [("submit", "draft"), ("approve", "pending_approval")])
def test_a_draft_or_pending_mandate_past_its_validity_is_refused_not_acted_on(ns, env, agent, monkeypatch, action, state):
    mandate_id = to_state(ns, env, state)
    overdue(ns, env, monkeypatch)
    env.recorded.clear()
    extra = f";{APPROVE}" if action == "approve" else ""
    assert run(ns, env, f"project_id=1;action={action};mandate={mandate_id}{extra}", APPROVER)["code"] == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, mandate_id).state == state and env.recorded == []


def test_a_suspended_mandate_past_its_validity_is_expired_on_resume_and_stays_expired(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "suspended")
    overdue(ns, env, monkeypatch)
    ns.revoked.clear()
    env.recorded.clear()
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["code"] == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, mandate_id).state == "expired"   # committed, not rolled back with the refusal
    assert [call[1:] for call in ns.revoked] == [(mandate_id, "MANDATE_EXPIRED")]
    assert [e["reason_code"] for e in events(env)] == ["MANDATE_EXPIRED"]
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["code"] == "INVALID_STATE"


def test_an_overdue_resume_dry_run_writes_nothing(ns, env, agent, monkeypatch):
    mandate_id = to_state(ns, env, "suspended")
    overdue(ns, env, monkeypatch)
    env.recorded.clear()
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id};dry_run", APPROVER)["code"] == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, mandate_id).state == "suspended" and env.recorded == []


@pytest.mark.parametrize("binding_state", ["suspended", "retired"])
def test_resume_needs_the_binding_to_be_active_still(ns, env, agent, binding_state):
    mandate_id = to_state(ns, env, "suspended")
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).values(state=binding_state))
        session.commit()
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["code"] == "BINDING_NOT_ACTIVE"
    assert mandate_row(env, ns, mandate_id).state == "suspended"


@pytest.mark.parametrize("name,value", [
    ("approval_ref", "x" * 129), ("approval_ref", "has space"), ("approval_ref", "q'uote"),
    ("policy_revision", "x" * 129), ("policy_revision", "new\nline"), ("policy_revision", "<tag>"),
    ("approval_ref", "\u00e9")])
def test_an_unbounded_or_odd_reference_is_a_readable_refusal(ns, env, name, value):
    other = "policy_revision=rev-1" if name == "approval_ref" else "approval_ref=CHG-1"
    values, error = parsed(ns, f"project_id=1;action=approve;mandate={uuid.uuid4()};{other};{name}={value}")
    assert values is None and error.startswith(f"invalid {name}: 1 to 128 characters")
    assert value not in error


@pytest.mark.parametrize("value", ["x" * 128, "CHG-12", "a.b_c:d/e@f-g", "2026-10-1"])
def test_the_allowed_reference_shapes_are_accepted(ns, env, value):
    assert parsed(ns, f"project_id=1;action=approve;mandate={uuid.uuid4()};approval_ref={value};policy_revision=r")[1] is None


@pytest.mark.parametrize("until", ["9999-12-31T23:59:59-01:00", "0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59+00:00"])
def test_a_validity_at_the_edge_of_the_calendar_is_a_readable_refusal(ns, env, until):
    values, error = parsed(ns, CREATE.replace(UNTIL, until))
    assert values is None and error.startswith("invalid valid_until")


@pytest.mark.parametrize("key", ["project_id", "version_id", "owner_user_id"])
def test_an_integer_too_big_for_the_column_is_refused(ns, env, key):
    param = CREATE.replace("project_id=1", "project_id=1")
    param = f"{param};{key}=99999999999" if key == "project_id" else param.replace(f"{key}=", f"{key}=99999999999", 1)
    values, error = parsed(ns, param)
    assert values is None and f"invalid {key}" in error


def test_the_largest_column_integer_is_accepted(ns, env):
    assert parsed(ns, CREATE.replace("version_id=70", "version_id=2147483647"))[1] is None


def test_the_parser_never_escapes_the_task(ns, env, monkeypatch):
    def boom(param):
        raise OverflowError("date value out of range")

    monkeypatch.setattr(ns.mandate_admin, "parse_mandate_param", boom)
    result = run(ns, env, CREATE)
    assert list(result) == ["error"] and result["error"].startswith("invalid parameter")


@pytest.mark.parametrize("error,code", [
    (RuntimeError("No such user found: id=5"), "OWNER_NOT_FOUND"),
    (RuntimeError("timeout waiting for the rpc"), "DEPENDENCY_UNAVAILABLE"),
    (TimeoutError("auth did not answer"), "DEPENDENCY_UNAVAILABLE"),
    (ConnectionError("redis down"), "DEPENDENCY_UNAVAILABLE")])
def test_only_a_missing_user_is_owner_not_found(ns, env, agent, error, code):
    def get_user(user_id):
        raise error

    ns.tools.auth.get_user = get_user
    result = run(ns, env, CREATE)
    assert result["code"] == code
    assert env.query(1, ns.Mandate) == [] and env.recorded == []
    assert ns.mandate_admin.MESSAGES["DEPENDENCY_UNAVAILABLE"] == "The identity service did not answer; try again."


def test_an_identity_service_that_does_not_answer_on_resume_is_a_dependency_failure(ns, env, agent):
    mandate_id = add_mandate(env, ns, state="suspended", suspended_reason="OWNER_SUSPENDED", binding_ref=agent, epoch=1)

    def boom(kind, ident):
        raise TimeoutError("no answer")

    ns.tools.auth.read_identity_epochs = boom
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["code"] == "DEPENDENCY_UNAVAILABLE"
    assert mandate_row(env, ns, mandate_id).state == "suspended"
