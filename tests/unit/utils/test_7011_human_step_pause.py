"""EL-7011 US-19 (R2.1 to R2.4): a schedule run that pauses for a human answer fails.

Contract under test: utils/schedule_runs.py pause_schedule_run on the real models, in-memory SQLite.
Harnesses come from the sibling 7011 test files.
"""
from datetime import UTC, datetime

import pytest
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

PLAIN = "the run needs a human answer"


def _pause(runs, session, interrupt, status="running"):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule, status=status)
    runs.pause_schedule_run(1, run.id, interrupt)
    session.refresh(run)
    session.refresh(schedule)
    return run, schedule


@pytest.mark.parametrize("interrupt, reason", [
    ({"hitl_interrupt": {"guardrail_type": "clarifying_question", "node_name": "Review"}},
     PLAIN + " (step 'Review')"),  # TC-US19-06
    ({"hitl_interrupt": {"guardrail_type": "hitl", "node_name": "Approve"}},
     PLAIN + " (step 'Approve')"),  # TC-US19-07
    ({"hitl_interrupt": {"guardrail_type": "sensitive_tool_off", "toolkit_name": "SharePoint",
                         "node_name": "mcp_auth_guard"}},
     PLAIN + " (toolkit 'SharePoint')"),  # TC-US19-08: the toolkit wins over the node
    ({"toolkit_name": "SharePoint"}, PLAIN + " (toolkit 'SharePoint')"),  # TC-US19-08: top level
    ({"hitl_interrupt": {"node_name": "x" * 300}}, PLAIN + " (step '" + "x" * 100 + "')"),  # the name is cut
    ({"hitl_interrupts": [{"guardrail_type": "hitl"}, {"node_name": "Second"}]},
     PLAIN + " (step 'Second')"),  # R2.4 over a parallel pause
    ({"type": "interrupt"}, PLAIN),  # TC-US19-11: no name
])
def test_el_7011_r2_1_to_r2_4_a_human_step_pause_fails_the_run_and_names_the_step(
        runs, session, sent, interrupt, reason):
    run, schedule = _pause(runs, session, interrupt)

    assert (run.status, run.reason) == ("failed", reason)
    assert run.finished_at is not None
    assert schedule.last_run_status == "failed"


def test_el_7011_r2_3_a_human_step_failure_sends_one_failed_notice(runs, session, sent):
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "clarifying_question"}})

    assert sent.notices == [(12, "schedule_run_failed", run.id)]


def test_el_7011_r2_1_a_sensitive_tool_pause_still_waits_for_approval(runs, session, sent):
    """TC-US19-09."""
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "sensitive_tool", "node_name": "N"}})

    assert (run.status, run.reason) == ("waiting_for_approval", None)
    assert sent.notices == [(12, "schedule_approval_needed", run.id)]


def test_el_7011_r2_1_a_pause_of_a_run_that_is_not_running_changes_nothing(runs, session, sent):
    """TC-US19-10."""
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "clarifying_question"}}, status="success")

    assert (run.status, run.reason) == ("success", None)
    assert sent.notices == []


SIGN_IN = {"type": "mcp_authorization_required", "guardrail_type": "mcp_auth", "toolkit_name": "SharePoint"}


def test_a_sign_in_pause_waits_for_approval_and_names_the_toolkit(runs, session, sent):
    run, schedule = _pause(runs, session, SIGN_IN)

    assert (run.status, run.reason) == ("waiting_for_approval", None)
    assert run.approval_expires_at is not None
    assert schedule.last_run_status == "waiting_for_approval"
    assert [event["text"] for event in run.events if event["kind"] == "paused"] == [
        "Paused for approval: a sign-in is needed for SharePoint"]
    assert sent.notices == [(12, "schedule_approval_needed", run.id)]


def test_a_sign_in_event_has_the_sign_in_need(runs, session, sent):
    run, _ = _pause(runs, session, SIGN_IN)

    event = next(event for event in run.events if event["kind"] == "paused")
    assert (event["need"], event["toolkit"]) == ("sign_in", "SharePoint")


def test_a_sign_in_pause_under_run_without_asking_fails_the_run_with_the_toolkit(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12, approval_rule="run_without_asking")
    run = _run(runs, session, schedule)

    runs.pause_schedule_run(1, run.id, SIGN_IN)
    session.refresh(run)

    assert run.status == "failed"
    assert "SharePoint" in run.reason and "sign-in" in run.reason
    assert sent.notices == [(12, "schedule_run_failed", run.id)]


def test_a_sign_in_inside_a_parallel_pause_waits_too(runs, session, sent):
    run, _ = _pause(runs, session, {"hitl_interrupts": [{"guardrail_type": "hitl"}, SIGN_IN]})

    assert run.status == "waiting_for_approval"


def test_the_owner_authorizes_a_sign_in_and_the_run_goes_on(runs, session, sent):
    run, _ = _pause(runs, session, SIGN_IN)

    decision = runs.check_schedule_decision(1, run.id, 12, ["authorize"], sign_in=True)
    runs.apply_schedule_decision(1, run.id, decision)
    session.refresh(run)

    assert (decision, run.status) == ("approve", "running")


def test_skipping_a_sign_in_ends_the_run_as_rejected_by_the_owner(runs, session, sent):
    run, _ = _pause(runs, session, SIGN_IN)

    decision = runs.check_schedule_decision(1, run.id, 12, ["skip"], sign_in=True)
    runs.apply_schedule_decision(1, run.id, decision)
    session.refresh(run)

    assert (decision, run.status, run.reason) == ("reject", "failed", "rejected by the owner")


def test_only_the_owner_answers_a_sign_in(runs, session, sent):
    run, _ = _pause(runs, session, SIGN_IN)

    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, 99, ["authorize"], sign_in=True)


def test_an_unknown_sign_in_answer_is_refused(runs, session, sent):
    run, _ = _pause(runs, session, SIGN_IN)

    with pytest.raises(runs.ScheduleDecisionInvalid):
        runs.check_schedule_decision(1, run.id, 12, ["maybe"], sign_in=True)


def test_a_sign_in_answer_on_a_run_that_is_not_waiting_is_refused(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule, status="running")

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.check_schedule_decision(1, run.id, 12, ["authorize"], sign_in=True)


CREDENTIAL_REASON = "credential qa_missing not found"


def _park(runs, session, rule="pause_and_ask", resumed=False):
    schedule = _schedule(runs, session, created_by=12, approval_rule=rule)
    run = _run(runs, session, schedule)
    if resumed:
        run.resumed_at = datetime.now(UTC)
        session.commit()
    parked = runs.park_for_credentials(1, run.id, CREDENTIAL_REASON)
    session.refresh(run)
    session.refresh(schedule)
    return parked, run, schedule


def test_a_missing_credential_waits_for_the_owner_without_a_chat(runs, session, sent):
    parked, run, schedule = _park(runs, session)

    assert parked and (run.status, run.reason) == ("waiting_for_approval", None)
    assert run.conversation_id is None and run.approval_expires_at is not None
    assert schedule.last_run_status == "waiting_for_approval"
    assert [event["text"] for event in run.events if event["kind"] == "paused"] == [
        f"Paused for approval: {CREDENTIAL_REASON}. Add the credentials, then approve"]
    assert sent.notices == [(12, "schedule_approval_needed", run.id)]


def test_a_credential_wait_event_names_the_need_the_toolkit_and_the_credential(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12, approval_rule="pause_and_ask")
    run = _run(runs, session, schedule)
    runs.park_for_credentials(1, run.id, CREDENTIAL_REASON, toolkit="Jira" + "x" * 200, credential="qa_missing")
    session.refresh(run)

    event = next(event for event in run.events if event["kind"] == "paused")
    assert (event["need"], event["credential"], event["toolkit"]) == ("credential", "qa_missing", ("Jira" + "x" * 200)[:100])


def test_a_credential_wait_event_leaves_out_an_unknown_toolkit_and_credential(runs, session, sent):
    _, run, _ = _park(runs, session)

    event = next(event for event in run.events if event["kind"] == "paused")
    assert event["need"] == "credential" and "toolkit" not in event and "credential" not in event


def test_parking_for_a_missing_credential_opens_one_database_session(runs, session, sent, monkeypatch):
    schedule = _schedule(runs, session, created_by=12, approval_rule="pause_and_ask")
    run = _run(runs, session, schedule)
    opened = []
    get_session = runs.db.get_session

    def counting(project_id):
        opened.append(project_id)
        return get_session(project_id)

    monkeypatch.setattr(runs.db, "get_session", counting)

    assert runs.park_for_credentials(1, run.id, CREDENTIAL_REASON)
    assert len(opened) == 1


def test_a_missing_credential_under_run_without_asking_does_not_wait(runs, session, sent):
    parked, run, _ = _park(runs, session, rule="run_without_asking")

    assert not parked and run.status == "running" and sent.notices == []


def test_a_missing_credential_after_the_owner_approved_does_not_wait_again(runs, session, sent):
    parked, run, _ = _park(runs, session, resumed=True)

    assert not parked and run.status == "running" and sent.notices == []


def test_a_run_that_is_not_running_is_not_parked(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule, status="success")

    assert not runs.park_for_credentials(1, run.id, CREDENTIAL_REASON) and sent.notices == []


def test_the_owner_approves_a_credential_wait_and_the_run_runs_again(runs, session, sent):
    _, run, _ = _park(runs, session)

    assert runs.check_schedule_decision(1, run.id, 12, ["approve"]) == "approve"
    runs.apply_schedule_decision(1, run.id, "approve")
    session.refresh(run)

    assert run.status == "running" and run.resumed_at is not None and run.approval_expires_at is None


def test_the_owner_rejects_a_credential_wait_and_the_run_fails(runs, session, sent):
    _, run, _ = _park(runs, session)

    runs.apply_schedule_decision(1, run.id, "reject")
    session.refresh(run)

    assert (run.status, run.reason) == ("failed", "rejected by the owner")
