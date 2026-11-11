"""EL-7011 US-18: only the schedule owner decides a waiting run, and only while it waits.

Contract under test: utils/schedule_runs.py (check_schedule_decision, apply_schedule_decision,
ScheduleDecisionForbidden, ScheduleDecisionInvalid, ScheduleRunNotWaiting) on the real models,
in-memory SQLite.
Cases: TC-US18-05, TC-US18-08, TC-US18-09, TC-US18-10.
"""
import sys
import types
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
    assert (run.status, run.reason) == ("failed", "rejected by the owner")
    assert (schedule.last_run_status, schedule.last_conversation_id) == ("failed", 55)


def test_el_7011_r2_3_an_admin_who_is_not_the_owner_gets_forbidden(runs, session, sent):
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


def _decision_events(session, run):
    session.refresh(run)
    return [event for event in run.events or [] if event["kind"] == "decision"]


def test_an_approve_all_writes_the_choice_into_the_decision_event(runs, session, sent):
    _, run = _waiting(runs, session)

    runs.apply_schedule_decision(1, run.id, "approve", approve_all=True)

    (event,) = _decision_events(session, run)
    assert event["text"] == "Approved; this run asks no more approvals"
    assert (event["action"], event["by"], event["approve_all"]) == ("approve", OWNER, True)
    assert run.status == "running"


def test_a_plain_approve_keeps_the_owner_text_and_no_choice(runs, session, sent):
    _, run = _waiting(runs, session)

    runs.apply_schedule_decision(1, run.id, "approve")

    (event,) = _decision_events(session, run)
    assert event["text"] == "Approved by the schedule owner"
    assert "approve_all" not in event


def test_a_reject_with_approve_all_fails_the_run_and_records_no_choice(runs, session, sent):
    _, run = _waiting(runs, session)

    runs.apply_schedule_decision(1, run.id, "reject", approve_all=True)

    (event,) = _decision_events(session, run)
    assert event["text"] == "Rejected by the schedule owner" and "approve_all" not in event
    assert (run.status, run.reason) == ("failed", "rejected by the owner")
    assert runs.approves_all(session, run.id) is False


def test_approves_all_is_false_for_a_plain_approve(runs, session, sent):
    _, run = _waiting(runs, session)
    runs.apply_schedule_decision(1, run.id, "approve")

    assert runs.approves_all(session, run.id) is False


def test_approves_all_is_true_after_an_approve_all(runs, session, sent):
    _, run = _waiting(runs, session)
    runs.apply_schedule_decision(1, run.id, "approve", approve_all=True)

    assert runs.approves_all(session, run.id) is True


@pytest.mark.parametrize('status, expected', [
    ('running', True), ('waiting_for_approval', True),
    ('completed', False), ('failed', False), ('blocked', False), ('cancelled', False)])
def test_approves_all_holds_only_while_the_run_is_active(runs, session, sent, status, expected):
    _, run = _waiting(runs, session)
    runs.apply_schedule_decision(1, run.id, "approve", approve_all=True)
    run.status = status
    session.flush()

    assert runs.approves_all(session, run.id) is expected


def test_approves_all_is_false_for_an_unknown_run_and_a_run_without_events(runs, session, sent):
    _, run = _waiting(runs, session)

    assert runs.approves_all(session, run.id) is False
    assert runs.approves_all(session, 999) is False


def test_a_paused_event_that_carries_the_flag_is_no_choice(runs, session, sent):
    _, run = _waiting(runs, session)
    run.events = [{"kind": "paused", "text": "x", "approve_all": True}]
    session.flush()

    assert runs.approves_all(session, run.id) is False


STRANGER = 77


@pytest.fixture
def allowed(monkeypatch):
    """The "Run without asking" setting of the project, switched by `allowed.value`."""
    state = types.SimpleNamespace(value=True, asked=[])

    def is_run_without_asking_allowed(project_id):
        state.asked.append(project_id)
        return state.value

    module = types.ModuleType("plugins.elitea_core.utils.schedule_settings")
    module.is_run_without_asking_allowed = is_run_without_asking_allowed
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.schedule_settings", module)
    return state


def _started_by(runs, session, run, user_id):
    run.requested_by = user_id
    session.flush()


def test_an_approve_all_of_the_owner_is_accepted_while_the_setting_is_on(runs, session, sent, allowed):
    _, run = _waiting(runs, session)

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True) == "approve"
    assert allowed.asked == [1]


def test_an_approve_all_is_refused_while_the_setting_is_off_and_the_run_keeps_waiting(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    allowed.value = False

    with pytest.raises(runs.ScheduleApproveAllOff):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True)

    session.refresh(run)
    assert (run.status, run.resumed_at, _decision_events(session, run)) == ("waiting_for_approval", None, [])


def test_a_plain_approve_still_works_while_the_setting_is_off(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    allowed.value = False

    assert _decide(runs, run.id, OWNER, ["approve"]) == "approve"
    assert allowed.asked == []
    assert run.status == "running"


def test_an_approve_all_of_the_owner_is_refused_when_someone_else_started_the_run(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    _started_by(runs, session, run, ADMIN)

    # The owner still decides the run; the admin who started it is no decider on this base
    with pytest.raises(runs.ScheduleApproveAllForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True)

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"]) == "approve"
    session.refresh(run)
    assert (run.status, _decision_events(session, run)) == ("waiting_for_approval", [])


def test_a_non_actor_refusal_comes_before_the_setting_is_read(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    _started_by(runs, session, run, ADMIN)
    allowed.value = False

    with pytest.raises(runs.ScheduleApproveAllForbidden):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True)

    assert allowed.asked == []


def test_a_user_who_is_not_the_decider_gets_the_decision_refusal_first(runs, session, sent, allowed):
    _, run = _waiting(runs, session)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, STRANGER, ["approve"], approve_all=True)


def test_the_requester_of_a_test_run_may_approve_all(runs, session, sent, allowed):
    run = runs.EntityScheduleRun(schedule_id=None, trigger="test", status="waiting_for_approval", requested_by=OWNER)
    session.add(run)
    session.flush()

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True) == "approve"
    assert runs.is_approve_all_actor(session, run, STRANGER) is False


def test_a_reject_ignores_approve_all_and_the_setting(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    _started_by(runs, session, run, ADMIN)
    allowed.value = False

    assert runs.check_schedule_decision(1, run.id, OWNER, ["reject"], approve_all=True) == "reject"
    assert allowed.asked == []


def test_a_run_that_does_not_wait_gets_the_not_waiting_refusal_before_the_approve_all_one(runs, session, sent, allowed):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule, status="running")
    allowed.value = False

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, OWNER, ["approve"], approve_all=True)


def test_can_approve_all_is_true_for_the_actor_of_a_waiting_run_while_the_setting_is_on(runs, session, sent, allowed):
    _, run = _waiting(runs, session)

    assert runs.can_approve_all(session, 1, run, OWNER) is True


def test_can_approve_all_is_false_while_the_setting_is_off(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    allowed.value = False

    assert runs.can_approve_all(session, 1, run, OWNER) is False


def test_can_approve_all_is_false_for_a_caller_who_did_not_start_the_run(runs, session, sent, allowed):
    _, run = _waiting(runs, session)
    _started_by(runs, session, run, ADMIN)

    assert runs.can_approve_all(session, 1, run, OWNER) is False


def test_can_approve_all_is_false_for_a_run_that_does_not_wait_and_reads_no_setting(runs, session, sent, allowed):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule, status="running")

    assert runs.can_approve_all(session, 1, run, OWNER) is False
    assert allowed.asked == []
