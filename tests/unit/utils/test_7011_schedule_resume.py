"""Only the user a run acts as decides a waiting run, and only while it waits.

That is the owner for a cron or webhook run, and the person who pressed Run now or Test run for a manual run.

Contract under test: utils/schedule_runs.py (check_schedule_decision, apply_schedule_decision,
ScheduleDecisionForbidden, ScheduleDecisionInvalid, ScheduleRunNotWaiting) on the real models,
in-memory SQLite.
Cases: TC-US18-05, TC-US18-08, TC-US18-09, TC-US18-10.
"""
from datetime import UTC, datetime, timedelta

import pytest

# Shared fixtures of the US-09 and US-13 writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

OWNER, ADMIN = 12, 1


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


def test_an_admin_who_is_not_the_owner_gets_forbidden(runs, session, sent):
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


@pytest.fixture
def an_admin_run(runs, session):
    """A waiting run of OWNER's schedule that ADMIN started with Run now, so it acts as ADMIN."""
    schedule, run = _waiting(runs, session)
    run.trigger, run.requested_by = "manual", ADMIN
    session.flush()
    return schedule, run


def test_the_actor_of_a_manual_or_test_run_is_the_one_who_pressed_the_button(runs, session, sent):
    _, run = _waiting(runs, session)

    assert runs.run_actor_id(session, run) == OWNER
    for trigger in ("manual", "test"):
        run.trigger, run.requested_by = trigger, ADMIN
        assert runs.run_actor_id(session, run) == ADMIN
    run.trigger, run.requested_by = "manual", None
    assert runs.run_actor_id(session, run) == OWNER


@pytest.mark.parametrize("trigger", ["cron", "webhook"])
@pytest.mark.parametrize("action", ["approve", "reject"])
def test_an_admin_cannot_decide_a_run_that_acts_as_the_owner(runs, session, sent, trigger, action):
    _, run = _waiting(runs, session)
    run.trigger = trigger
    session.flush()

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, [action])

    session.refresh(run)
    assert run.status == "waiting_for_approval"


@pytest.mark.parametrize("action", ["approve", "reject"])
def test_the_owner_cannot_decide_a_run_that_acts_as_an_admin(runs, session, sent, an_admin_run, action):
    _, run = an_admin_run

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, [action])

    session.refresh(run)
    assert run.status == "waiting_for_approval"


@pytest.mark.parametrize("action", ["approve", "reject"])
def test_an_admin_decides_the_run_they_started(runs, session, sent, an_admin_run, action):
    _, run = an_admin_run

    _decide(runs, run.id, ADMIN, [action])

    session.refresh(run)
    assert run.status == ("running" if action == "approve" else "failed")


def test_the_run_log_names_an_admin_when_the_run_acts_as_an_admin(runs, session, sent, an_admin_run):
    _, run = an_admin_run

    _decide(runs, run.id, ADMIN, ["approve"])

    session.refresh(run)
    decision = next(event for event in run.events if event["kind"] == "decision")
    assert (decision["text"], decision["by"]) == ("Approved by an admin", ADMIN)


def test_the_run_log_names_the_owner_when_the_run_acts_as_the_owner(runs, session, sent):
    _, run = _waiting(runs, session)

    _decide(runs, run.id, OWNER, ["reject"])

    session.refresh(run)
    decision = next(event for event in run.events if event["kind"] == "decision")
    assert (decision["text"], decision["by"]) == ("Rejected by the schedule owner", OWNER)


def test_after_a_change_of_owner_the_new_owner_decides_a_waiting_cron_run(runs, session, sent):
    new_owner = 99
    schedule, run = _waiting(runs, session)
    schedule.created_by = new_owner
    session.flush()

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"])
    assert runs.check_schedule_decision(1, run.id, new_owner, ["approve"]) == "approve"


@pytest.mark.parametrize("answer, decision", [("authorize", "approve"), ("skip", "reject")])
def test_only_the_actor_answers_a_sign_in(runs, session, sent, answer, decision):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, ADMIN, [answer], sign_in=True)
    assert runs.check_schedule_decision(1, run.id, OWNER, [answer], sign_in=True) == decision


def test_a_run_linked_to_another_conversation_is_not_decided(runs, session, sent):
    _, run = _waiting(runs, session)
    run.conversation_id = 55
    session.flush()

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], conversation_id=56)
    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"], conversation_id=55) == "approve"


SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool"}}


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
