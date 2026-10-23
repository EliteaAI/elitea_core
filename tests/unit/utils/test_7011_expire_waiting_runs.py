"""EL-7011 US-19: the expiry of runs that wait too long for the owner (R1.1 to R1.3).

Contract under test: utils/schedule_runs.py `expire_waiting_runs` on the real models, in-memory SQLite.
Harnesses come from the fail and outcome tests; nothing is copied.
"""
from datetime import UTC, datetime, timedelta

from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def _waiting(runs, session, expires_at, **schedule_values):
    schedule = _schedule(runs, session, **schedule_values)
    run = _run(runs, session, schedule, status="waiting_for_approval")
    run.approval_expires_at = expires_at
    session.flush()
    return schedule, run


def test_el_7011_r1_1_r1_2_expire_waiting_runs_fails_an_expired_run_and_copies_the_status(runs, session, sent):
    schedule, run = _waiting(runs, session, NOW - timedelta(minutes=1))

    assert runs.expire_waiting_runs(1, NOW) == 1  # TC-US19-01

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("failed", "approval expired")
    assert run.finished_at is not None
    assert schedule.last_run_status == "failed"  # TC-US19-02


def test_el_7011_r1_1_expire_waiting_runs_leaves_other_runs_alone(runs, session, sent):
    _, fresh = _waiting(runs, session, NOW + timedelta(hours=1), name="S2")
    _, running = _waiting(runs, session, NOW - timedelta(hours=1), name="S3")
    running.status = "running"
    session.flush()

    assert runs.expire_waiting_runs(1, NOW) == 0  # TC-US19-03

    session.refresh(fresh)
    session.refresh(running)
    assert (fresh.status, running.status) == ("waiting_for_approval", "running")
    assert sent.notices == []


def test_el_7011_r1_1_expire_waiting_runs_takes_at_most_100_runs_per_call(runs, session, sent):
    for index in range(101):
        _waiting(runs, session, NOW - timedelta(minutes=1), name=f"S{index}")

    assert runs.expire_waiting_runs(1, NOW) == 100
    assert runs.expire_waiting_runs(1, NOW) == 1


def test_el_7011_r1_3_expire_waiting_runs_sends_only_the_expired_notice(runs, session, sent):
    _, run = _waiting(runs, session, NOW - timedelta(minutes=1))

    runs.expire_waiting_runs(1, NOW)

    assert sent.notices == [(1, "schedule_approval_expired", run.id)]  # TC-US19-04, TC-US19-05
