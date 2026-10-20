"""EL-7011 US-18: a sensitive-tool pause makes a schedule run wait for the owner; a reject ends it.

Contract under test: utils/schedule_runs.py (pause_schedule_run, resume_schedule_run with "reject",
finish_schedule_run with expected_status and notice) on the real models, in-memory SQLite.
Cases: TC-US18-02, TC-US18-03, TC-US18-04, TC-US18-07.
"""
from datetime import UTC, datetime, timedelta

# Shared fixtures of the US-09 and US-13 writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool"}}


def _chat_run(runs, session, schedule):
    run = _run(runs, session, schedule)
    run.conversation_id = 55
    session.flush()
    return run


def test_el_7011_r1_2_a_sensitive_pause_sets_waiting_for_approval_for_24_hours(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _chat_run(runs, session, schedule)
    before = datetime.now(UTC)

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    session.refresh(run)
    assert (run.status, run.finished_at) == ("waiting_for_approval", None)
    expires = run.approval_expires_at.replace(tzinfo=UTC)
    assert before + timedelta(hours=24) <= expires <= datetime.now(UTC) + timedelta(hours=24)


def test_el_7011_r1_2_a_sensitive_pause_in_the_parallel_list_also_waits(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _chat_run(runs, session, schedule)

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [{"type": "hitl"}, SENSITIVE["hitl_interrupt"]]})

    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_el_7011_r1_3_a_waiting_run_sends_the_owner_one_approval_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _chat_run(runs, session, schedule)

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    # The notice helper builds the schedule name and the conversation link from these rows
    assert sent.notices == [(12, "schedule_approval_needed", run.id)]
    assert sent.emitted == []


def test_el_7011_r1_4_a_pause_for_a_run_that_is_not_running_changes_nothing(runs, session, sent):
    schedule = _schedule(runs, session, last_run_status="failed")
    run = _run(runs, session, schedule, status="failed")

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    session.refresh(run)
    assert (run.status, run.approval_expires_at) == ("failed", None)
    assert (sent.notices, sent.emitted) == ([], [])


def test_el_7011_r1_4_a_second_pause_for_a_waiting_run_sends_no_second_notice(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _chat_run(runs, session, schedule)
    runs.pause_schedule_run(1, run.id, SENSITIVE)

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    session.refresh(run)
    assert run.status == "waiting_for_approval"
    assert len(sent.notices) == 1


def test_el_7011_r2_2_a_reject_fails_the_run_and_copies_the_status_without_a_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _chat_run(runs, session, schedule)
    runs.pause_schedule_run(1, run.id, SENSITIVE)
    sent.notices.clear()

    runs.resume_schedule_run(1, run.id, 12, "reject")

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("failed", "rejected by the owner")
    assert run.finished_at is not None
    assert schedule.last_run_status == "failed"
    assert (sent.notices, sent.emitted) == ([], [])


def test_el_7011_r2_2_finish_schedule_run_by_default_leaves_a_waiting_run(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _chat_run(runs, session, schedule)
    runs.pause_schedule_run(1, run.id, SENSITIVE)

    # The stream end of a rejected or waiting run must not close it as running
    assert runs.finish_schedule_run(1, run.id, "success") is False

    session.refresh(run)
    assert run.status == "waiting_for_approval"
