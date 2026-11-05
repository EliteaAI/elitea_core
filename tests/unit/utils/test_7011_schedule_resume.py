"""Only the schedule owner or a holder of the schedules admin permission decides a waiting run, and only while it waits.

Contract under test: utils/schedule_runs.py (check_schedule_decision, apply_schedule_decision,
ScheduleDecisionForbidden, ScheduleDecisionInvalid, ScheduleRunNotWaiting) on the real models,
in-memory SQLite.
Cases: TC-US18-05, TC-US18-08, TC-US18-09, TC-US18-10.
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest

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
