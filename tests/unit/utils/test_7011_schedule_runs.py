"""EL-7011 BUG-35: the schedule `last_run_status` follows a run through the approval wait.

Contract under test: utils/schedule_runs.py (pause_schedule_run, apply_schedule_decision) copy the new
run status to the schedule, so the Schedules list shows Waiting for approval, then Running (US-15 R1.6).
Real models, in-memory SQLite.
"""
from datetime import UTC, datetime, timedelta

# Shared fixtures of the US-09 and US-13 writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

OWNER = 12
SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool"}}


def _status(runs, session, schedule_id):
    session.expire_all()
    return session.get(runs.EntitySchedule, schedule_id).last_run_status


def test_el_7011_bug35_r1_6_a_sensitive_pause_copies_waiting_for_approval(runs, session, sent):
    schedule = _schedule(runs, session, created_by=OWNER, last_run_status="running")
    run = _run(runs, session, schedule)

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    assert _status(runs, session, schedule.id) == "waiting_for_approval"


def test_el_7011_bug35_r1_6_an_approve_copies_running(runs, session, sent):
    schedule = _schedule(runs, session, created_by=OWNER, last_run_status="waiting_for_approval")
    run = _run(runs, session, schedule, status="waiting_for_approval")
    run.approval_expires_at = datetime.now(UTC) + timedelta(hours=24)
    session.flush()

    runs.apply_schedule_decision(1, run.id, "approve")

    assert _status(runs, session, schedule.id) == "running"


def test_el_7011_bug35_r1_6_a_pause_of_a_run_that_is_not_running_keeps_the_status(runs, session, sent):
    schedule = _schedule(runs, session, created_by=OWNER, last_run_status="success")
    run = _run(runs, session, schedule, status="success")

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    assert _status(runs, session, schedule.id) == "success"
