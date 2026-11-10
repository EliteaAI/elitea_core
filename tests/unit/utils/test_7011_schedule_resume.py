"""The schedule owner, the user a run acts as or a holder of the schedules admin permission decides a waiting run, and only
while it waits. Only the user the run acts as edits, answers or sends text.

Contract under test: utils/schedule_runs.py (check_schedule_decision, apply_schedule_decision,
ScheduleDecisionForbidden, ScheduleEditForbidden, ScheduleDecisionInvalid, ScheduleRunNotWaiting) on the real models,
in-memory SQLite.
Cases: TC-US18-05, TC-US18-08, TC-US18-09, TC-US18-10.
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select

# Shared fixtures of the US-09 and US-13 writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

OWNER, ADMIN = 12, 1


@pytest.fixture
def project_admins(runs):
    """The user ids that hold the admin permission for the RPC read of the chat path."""
    return runs.permission_holders


def _waiting(runs, session, **schedule_values):
    schedule = _schedule(runs, session, created_by=OWNER, **schedule_values)
    run = _run(runs, session, schedule, status="waiting_for_approval")
    run.approval_expires_at = datetime.now(UTC) + timedelta(hours=24)
    session.flush()
    return schedule, run


def _decide(runs, run_id, caller_id, actions, fan_out=False):
    decision = runs.check_schedule_decision(1, run_id, caller_id, actions, fan_out)
    if decision:
        runs.apply_schedule_decision(1, run_id, decision)
    return decision


def test_el_7011_r2_1_an_owner_approve_sets_running_and_clears_the_expiry(runs, session, sent):
    _, run = _waiting(runs, session)

    _decide(runs, run.id, OWNER, ["approve"])

    session.refresh(run)
    assert (run.status, run.approval_expires_at, run.finished_at) == ("running", None, None)
    assert run.resumed_at is not None
    assert sent.notices == []


def test_el_7011_r2_1_the_check_alone_keeps_the_run_waiting(runs, session, sent):
    # A guard of continue_predict that refuses after the check leaves the run as it was
    _, run = _waiting(runs, session)

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"]) == "approve"

    session.refresh(run)
    assert (run.status, run.resumed_at) == ("waiting_for_approval", None)
    assert run.approval_expires_at is not None


@pytest.mark.parametrize("actions, decision", [
    (["approve"], "approve"),
    (["edit"], "approve"),
    (["answer"], "approve"),
    (["block_with_comment"], "approve"),
    (["reject"], "reject"),
    (["approve", "reject"], "reject"),
    (["reject", "approve", "edit"], "reject"),
])
def test_el_7011_r2_1_r2_2_any_reject_in_the_list_rejects(runs, session, sent, actions, decision):
    _, run = _waiting(runs, session)

    assert runs.check_schedule_decision(1, run.id, OWNER, actions) == decision


@pytest.mark.parametrize("actions", [[], [None], [""], ["maybe"], ["approve", "maybe"], [["approve"]]])
def test_el_7011_r2_1_an_empty_or_unknown_action_is_invalid(runs, session, sent, actions):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionInvalid):
        runs.check_schedule_decision(1, run.id, OWNER, actions)

    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_el_7011_r2_2_a_reject_fails_the_run_with_its_conversation(runs, session, sent):
    schedule, run = _waiting(runs, session)
    run.conversation_id = 55
    session.flush()

    _decide(runs, run.id, OWNER, ["reject"])

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("failed", "rejected")
    assert (schedule.last_run_status, schedule.last_conversation_id) == ("failed", 55)


@pytest.mark.parametrize("action", ["approve", "reject"])
def test_a_schedules_admin_who_is_not_the_owner_decides(runs, session, sent, action):
    _, run = _waiting(runs, session)

    decision = runs.check_schedule_decision(1, run.id, ADMIN, [action], is_admin=True)
    runs.apply_schedule_decision(1, run.id, decision, ADMIN)

    session.refresh(run)
    assert run.status == ("running" if action == "approve" else "failed")
    assert next(event for event in run.events if event["kind"] == "decision")["by"] == ADMIN


@pytest.mark.parametrize("action", ["approve", "reject"])
def test_the_chat_path_reads_the_admin_permission_of_the_caller(runs, session, sent, project_admins, action):
    project_admins.add(ADMIN)
    _, run = _waiting(runs, session)

    assert runs.check_schedule_decision(1, run.id, ADMIN, [action]) == action


def test_the_chat_path_refuses_a_caller_whose_permission_read_fails(runs, session, sent):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"])


def test_a_caller_who_is_not_a_schedules_admin_gets_forbidden_even_when_the_route_says_so(runs, session, sent,
                                                                                         project_admins):
    project_admins.add(ADMIN)  # the flag of the route wins: the permission read is not asked
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"], is_admin=False)


def test_the_permission_read_is_not_asked_for_the_owner(runs, session, sent, monkeypatch):
    _, run = _waiting(runs, session)

    def refuse(project_id, owner_id, entity_type=None, entity_id=None, verbs=("run",)):
        raise AssertionError("the owner must not trigger the permission read")

    monkeypatch.setattr(sys.modules["plugins.elitea_core.utils.schedule_prerun"], "check_owner_rights", refuse)

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"]) == "approve"


def test_a_run_linked_to_another_conversation_is_not_decided(runs, session, sent):
    _, run = _waiting(runs, session)
    run.conversation_id = 55
    session.flush()

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], conversation_id=56)
    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"], conversation_id=55) == "approve"


@pytest.mark.parametrize("action,text", [("approve", "approved"), ("reject", "rejected")])
def test_the_owner_gets_a_notice_when_an_admin_decides(runs, session, sent, action, text):
    _, run = _waiting(runs, session)

    decision = runs.check_schedule_decision(1, run.id, ADMIN, [action], is_admin=True)
    runs.apply_schedule_decision(1, run.id, decision, ADMIN)

    assert sent.notices == [(OWNER, "schedule_edited_by_admin", run.id)]
    assert text in sent.extras[0]["action"]


def test_the_owner_gets_no_notice_for_their_own_decision(runs, session, sent):
    _, run = _waiting(runs, session)

    runs.apply_schedule_decision(1, run.id, runs.check_schedule_decision(1, run.id, OWNER, ["reject"]), OWNER)

    assert sent.notices == []


def test_the_actor_of_a_manual_run_is_the_one_who_pressed_run_now(runs, session, sent):
    schedule, run = _waiting(runs, session)
    run.trigger, run.requested_by = "manual", ADMIN
    session.flush()

    assert runs.run_actor_id(session, run) == ADMIN
    run.trigger = "cron"
    assert runs.run_actor_id(session, run) == OWNER


def test_a_user_who_is_not_an_admin_or_the_owner_gets_forbidden(runs, session, sent):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"])

    session.refresh(run)
    assert (run.status, run.resumed_at) == ("waiting_for_approval", None)


def test_el_7011_r2_3_a_reject_by_another_user_keeps_the_run_waiting(runs, session, sent):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["reject"])

    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_el_7011_r2_3_the_owner_check_comes_before_the_status_and_action_checks(runs, session, sent):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule, status="failed")

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["maybe"])


def test_el_7011_r2_3_a_fan_out_child_of_another_user_gets_forbidden(runs, session, sent):
    _, run = _waiting(runs, session)
    _decide(runs, run.id, OWNER, ["approve"])

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"], fan_out=True)


def test_el_7011_r2_4_a_second_approve_gets_not_waiting(runs, session, sent):
    _, run = _waiting(runs, session)
    _decide(runs, run.id, OWNER, ["approve"])

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"])

    session.refresh(run)
    assert run.status == "running"


def test_el_7011_r2_4_a_fan_out_child_on_a_resumed_run_changes_nothing(runs, session, sent):
    _, run = _waiting(runs, session)
    _decide(runs, run.id, OWNER, ["approve"])
    session.refresh(run)
    resumed_at = run.resumed_at

    assert runs.check_schedule_decision(1, run.id, OWNER, ["reject"], fan_out=True) is None

    session.refresh(run)
    assert (run.status, run.resumed_at, run.finished_at) == ("running", resumed_at, None)


def test_el_7011_r2_4_an_apply_after_a_parallel_decision_gets_not_waiting(runs, session, sent):
    _, run = _waiting(runs, session)
    decision = runs.check_schedule_decision(1, run.id, OWNER, ["approve"])
    _decide(runs, run.id, OWNER, ["approve"])

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.apply_schedule_decision(1, run.id, decision)


def test_el_7011_r2_4_a_reject_of_a_finished_run_gets_not_waiting(runs, session, sent):
    schedule = _schedule(runs, session, created_by=OWNER, last_run_status="success")
    run = _run(runs, session, schedule, status="success")

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, OWNER, ["reject"])

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, schedule.last_run_status) == ("success", "success")


def test_el_7011_r2_4_an_unknown_run_gets_not_waiting(runs, session, sent):
    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, 999, OWNER, ["approve"])


SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool"}}


def _waiting_run(runs, session, trigger="cron", requested_by=None):
    """A run of OWNER's schedule that waits for approval; a manual run was started by `requested_by`."""
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule, status="waiting_for_approval")
    run.trigger, run.requested_by, run.conversation_id = trigger, requested_by, 55
    run.approval_expires_at = datetime.now(UTC) + timedelta(hours=24)
    session.flush()
    return schedule, run


@pytest.mark.parametrize("action", ["edit", "answer", "block_with_comment"])
def test_an_admin_cannot_edit_answer_or_note_a_run_that_acts_as_the_owner(runs, session, sent, action):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, [action])
    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve", action])


@pytest.mark.parametrize("action", ["approve", "reject"])
def test_the_owner_may_approve_or_reject_a_run_that_acts_as_an_admin(runs, session, sent, action):
    _, run = _waiting_run(runs, session, "manual", ADMIN)

    assert runs.check_schedule_decision(1, run.id, OWNER, [action]) == action


@pytest.mark.parametrize("action", ["edit", "answer", "block_with_comment"])
def test_the_owner_cannot_edit_answer_or_note_a_run_that_acts_as_an_admin(runs, session, sent, action):
    _, run = _waiting_run(runs, session, "manual", ADMIN)

    with pytest.raises(runs.ScheduleEditForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, [action])
    with pytest.raises(runs.ScheduleEditForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve", action])


def test_the_owner_decision_reads_no_admin_permission(runs, session, sent):
    _, run = _waiting_run(runs, session, "manual", ADMIN)

    runs.check_schedule_decision(1, run.id, OWNER, ["approve"])

    assert runs.rights_calls == []


def test_an_admin_edit_on_the_owner_run_raises_the_edit_refusal(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden) as refused:
        runs.check_schedule_decision(1, run.id, ADMIN, ["edit"])

    assert type(refused.value) is runs.ScheduleEditForbidden


@pytest.mark.parametrize("caller, trigger, requested_by", [
    (ADMIN, "cron", None),      # a schedules admin on the owner's cron run
    (OWNER, "manual", ADMIN),   # the owner on a run that acts as an admin
])
def test_a_non_actor_approve_with_text_is_refused(runs, session, sent, caller, trigger, requested_by):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session, trigger, requested_by)

    with pytest.raises(runs.ScheduleEditForbidden):
        runs.check_schedule_decision(1, run.id, caller, ["approve"], has_text=True)

    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_a_user_without_rights_gets_the_rights_refusal_even_with_text(runs, session, sent):
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden) as refused:
        runs.check_schedule_decision(1, run.id, ADMIN, ["edit"], has_text=True)

    assert type(refused.value) is runs.ScheduleDecisionForbidden


@pytest.mark.parametrize("caller, trigger, requested_by", [(OWNER, "cron", None), (ADMIN, "manual", ADMIN)])
def test_the_actor_may_approve_with_text(runs, session, sent, caller, trigger, requested_by):
    _, run = _waiting_run(runs, session, trigger, requested_by)

    assert runs.check_schedule_decision(1, run.id, caller, ["approve"], has_text=True) == "approve"


def test_the_user_a_run_acts_as_may_edit_it(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session, "manual", ADMIN)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["edit"]) == "approve"


def test_an_admin_who_lost_the_admin_permission_still_decides_their_own_run(runs, session, sent):
    _, run = _waiting_run(runs, session, "manual", ADMIN)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["approve"]) == "approve"


def test_a_non_actor_skip_of_a_sign_in_rejects_the_run(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["skip"], sign_in=True) == "reject"


@pytest.mark.parametrize("answers", [["authorize"], ["skip", "authorize"]])
def test_a_non_actor_cannot_authorize_a_sign_in(runs, session, sent, answers):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleEditForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, answers, sign_in=True)

    session.refresh(run)
    assert run.status == "waiting_for_approval"


@pytest.mark.parametrize("caller, trigger, requested_by", [(OWNER, "cron", None), (ADMIN, "manual", ADMIN)])
def test_the_actor_authorizes_their_own_sign_in(runs, session, sent, caller, trigger, requested_by):
    _, run = _waiting_run(runs, session, trigger, requested_by)

    assert runs.check_schedule_decision(1, run.id, caller, ["authorize"], sign_in=True) == "approve"
    assert runs.check_schedule_decision(1, run.id, caller, ["skip"], sign_in=True) == "reject"


def test_a_user_without_rights_gets_the_rights_refusal_for_a_sign_in_authorize(runs, session, sent):
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden) as refused:
        runs.check_schedule_decision(1, run.id, ADMIN, ["authorize"], sign_in=True)

    assert type(refused.value) is runs.ScheduleDecisionForbidden


def test_an_unknown_action_of_an_admin_is_invalid_not_forbidden(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionInvalid):
        runs.check_schedule_decision(1, run.id, ADMIN, ["maybe"])


def test_an_admin_approve_is_refused_when_the_user_the_run_acts_as_lost_access(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    runs.lost_access.add(OWNER)
    _, run = _waiting_run(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"], recheck_actor=True)

    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_an_admin_approve_is_refused_when_the_rights_check_of_the_actor_cannot_answer(runs, session, sent, monkeypatch):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)
    monkeypatch.setattr(sys.modules["plugins.elitea_core.utils.schedule_prerun"], "check_owner_rights",
                        lambda *args, **kwargs: "access check unavailable")

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, ["approve"], recheck_actor=True)


def test_an_admin_reject_needs_no_rights_of_the_user_the_run_acts_as(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    runs.lost_access.add(OWNER)
    _, run = _waiting_run(runs, session)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["reject"], recheck_actor=True) == "reject"


def test_the_recheck_asks_for_the_agent_of_the_schedule(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    _, run = _waiting_run(runs, session)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["approve"], recheck_actor=True) == "approve"
    assert (OWNER, "agent", 1, ("run",)) in runs.rights_calls


def test_the_recheck_asks_for_the_agent_of_a_test_run(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    session.execute(insert(runs.EntityScheduleRun.__table__).values(
        trigger="test", status="waiting_for_approval", requested_by=OWNER, conversation_id=55,
        form={"entity_type": "pipeline"}, application_id=1))
    run_id = session.scalar(select(runs.EntityScheduleRun.id).where(runs.EntityScheduleRun.trigger == "test"))

    assert runs.check_schedule_decision(1, run_id, ADMIN, ["approve"], recheck_actor=True) == "approve"
    assert (OWNER, "pipeline", 1, ("run",)) in runs.rights_calls


def test_the_user_a_run_acts_as_is_not_rechecked_for_their_own_decision(runs, session, sent):
    runs.lost_access.add(OWNER)
    _, run = _waiting_run(runs, session)

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"], recheck_actor=True) == "approve"
    assert runs.rights_calls == []


def test_the_recheck_is_off_unless_asked_for(runs, session, sent):
    runs.permission_holders.add(ADMIN)
    runs.lost_access.add(OWNER)
    _, run = _waiting_run(runs, session)

    assert runs.check_schedule_decision(1, run.id, ADMIN, ["approve"]) == "approve"


@pytest.mark.parametrize("trigger, requested_by, recipient", [
    ("manual", ADMIN, ADMIN),   # Run now by an admin acts as the admin, who answers the card
    ("cron", None, OWNER),
    ("manual", OWNER, OWNER),
])
def test_the_approval_notice_goes_to_the_user_the_run_acts_as(runs, session, sent, trigger, requested_by, recipient):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule)
    run.trigger, run.requested_by, run.conversation_id = trigger, requested_by, 55
    session.flush()

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    assert sent.notices == [(recipient, "schedule_approval_needed", run.id)]


@pytest.mark.parametrize("trigger, requested_by, recipient", [("manual", ADMIN, ADMIN), ("cron", None, OWNER)])
def test_the_end_notice_and_the_chat_event_go_to_the_user_the_run_acts_as(
        runs, session, sent, trigger, requested_by, recipient):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule)
    run.trigger, run.requested_by = trigger, requested_by
    session.flush()

    runs.finish_schedule_run(1, run.id, "failed", "Tool failed", conversation_id=55)

    assert sent.notices == [(recipient, "schedule_run_failed", run.id)]
    assert [(user, run_id) for user, run_id, *_ in sent.emitted] == [(recipient, run.id)]
