"""EL-7011 US-13 R1.2, R1.3, R1.5, R2.1, R2.2, R3.1, R4.1: a schedule run ends with a recorded outcome.

Real: the run writers of `utils/schedule_runs.py`, the claim helpers, `run_owner_schedule` and the schedule
models, over SQLite tables. The harness is the `world` fixture of the US-11 integration test.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_run_outcome.py
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_agent_schedule_run import PKG, PROJECT_ID, world  # noqa: F401 -- fixture reuse

STALE_MINUTES = 120


@pytest.fixture
def ends(world, monkeypatch):  # noqa: F811 -- the fixture extends the imported one
    """The real writers, the schedule row reader and the chat notice recorder on top of `world`."""
    runs = sys.modules[f"{PKG}.utils.schedule_runs"]
    claim = sys.modules[f"{PKG}.utils.schedule_claim"]
    schedules = sys.modules[f"{PKG}.models.entity_schedule"]
    starter = sys.modules[f"{PKG}.utils.schedule_run_starter"]
    emitted = []
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished",
                        lambda *args: emitted.append(args), raising=False)

    def session():
        return sys.modules["tools"].db.get_session(PROJECT_ID)

    def schedule():
        with session() as s:
            return s.get(schedules.EntitySchedule, 1)

    def runs_of_schedule():
        with session() as s:
            return s.query(schedules.EntityScheduleRun).order_by(schedules.EntityScheduleRun.id).all()

    def skip(reason):
        with session() as s:
            run_id = runs.record_skipped_run(s, 1, reason)
            s.commit()
            return run_id

    def insert_cron_run():
        with session() as s:
            return claim.insert_cron_run(s, 1)

    def resume_at(moment):
        with session() as s:
            s.get(schedules.EntityScheduleRun, 1).resumed_at = moment
            s.commit()

    return type("Ends", (), {
        "world": world, "runs": runs, "starter": starter, "emitted": emitted, "schedule": staticmethod(schedule),
        "runs_of_schedule": staticmethod(runs_of_schedule), "skip": staticmethod(skip),
        "insert_cron_run": staticmethod(insert_cron_run), "resume_at": staticmethod(resume_at)})


# TC-US13-01 / R1.2, R1.5
def test_7011_r1_2_r1_5_a_normal_end_sets_success_and_copies_it_to_the_schedule(ends):
    assert ends.runs.finish_schedule_run(PROJECT_ID, 1, "success", None, 42) is True

    run = ends.world.run_row()
    assert (run.status, run.reason, run.conversation_id) == ("success", None, 42)
    assert run.finished_at is not None
    assert (ends.schedule().last_run_status, ends.schedule().last_conversation_id) == ("success", 42)


# TC-US13-03 / R1.3, R1.5: the run fails with the stream error as reason
def test_7011_r1_3_r1_5_an_error_end_sets_failed_with_the_reason(ends):
    ends.runs.finish_schedule_run(PROJECT_ID, 1, "failed", "model quota exceeded", 42)

    run = ends.world.run_row()
    assert (run.status, run.reason, run.finished_at is not None) == ("failed", "model quota exceeded", True)
    assert (ends.schedule().last_run_status, ends.schedule().last_conversation_id) == ("failed", 42)


# R1.2: a second end changes nothing, so a late event cannot overwrite the outcome
def test_7011_r1_2_a_finished_run_is_not_changed_by_a_second_end(ends):
    ends.runs.finish_schedule_run(PROJECT_ID, 1, "success", None, 42)

    assert ends.runs.finish_schedule_run(PROJECT_ID, 1, "failed", "late", 43) is False
    assert (ends.world.run_row().status, ends.schedule().last_conversation_id) == ("success", 42)


# TC-US13-07 / R2.1: the overlap is recorded as skipped and the active run stays running
def test_7011_r2_1_an_overlap_records_a_skipped_run_and_keeps_the_active_run(ends):
    assert ends.insert_cron_run() is None  # the active-run index holds run 1

    skipped_id = ends.skip("the previous run is still running")

    active, skipped = ends.runs_of_schedule()
    assert (active.status, active.finished_at) == ("running", None)
    assert (skipped.id, skipped.status, skipped.reason) == (skipped_id, "skipped", "the previous run is still running")
    assert skipped.conversation_id is None
    assert ends.schedule().last_run_status == "skipped"


# R2.2: a closed budget scope blocks the run before any predict
def test_7011_r2_2_a_closed_budget_blocks_the_run_and_starts_no_predict(ends, monkeypatch):
    monkeypatch.setattr(ends.starter, "closed_budget_scope", lambda _project, _owner: "monthly")

    ends.world.run()

    run = ends.world.run_row()
    assert (run.status, run.reason) == ("blocked", "the monthly budget is used up")
    assert ends.world.predictions == [] and ends.world.conversations() == []
    assert ends.schedule().last_run_status == "blocked"
    assert ends.world.notices == []  # a blocked run sends no notice


# R3.1: only a run quiet for longer than the timeout fails; a resume restarts the clock
def test_7011_r3_1_the_sweep_fails_a_quiet_run_and_leaves_a_fresh_one(ends):
    now = datetime.now(UTC)

    assert ends.runs.fail_stale_runs(PROJECT_ID, now, STALE_MINUTES) == 0
    assert ends.world.run_row().status == "running"

    assert ends.runs.fail_stale_runs(PROJECT_ID, now + timedelta(minutes=STALE_MINUTES + 5), STALE_MINUTES) == 1
    run = ends.world.run_row()
    assert (run.status, run.reason) == ("failed", "the run did not finish in time")
    assert ends.schedule().last_run_status == "failed"


def test_7011_r3_1_a_resumed_run_counts_from_resumed_at(ends):
    later = datetime.now(UTC) + timedelta(minutes=STALE_MINUTES + 5)
    ends.resume_at(later - timedelta(minutes=10))

    assert ends.runs.fail_stale_runs(PROJECT_ID, later, STALE_MINUTES) == 0
    assert ends.world.run_row().status == "running"


# TC-US13-15 / R4.1: a failed run notifies the owner; the stale sweep does too
def test_7011_r4_1_a_failed_run_sends_schedule_run_failed_to_the_owner(ends):
    ends.runs.finish_schedule_run(PROJECT_ID, 1, "failed", "boom", 42)

    assert ends.world.notices == [(12, "schedule_run_failed", "boom")]
    assert [(args[0], args[3], args[5]) for args in ends.emitted] == [(12, "failed", 42)]


def test_7011_r4_1_a_stale_failure_sends_schedule_run_failed_to_the_owner(ends):
    ends.runs.fail_stale_runs(PROJECT_ID, datetime.now(UTC) + timedelta(hours=3), STALE_MINUTES)

    assert ends.world.notices == [(12, "schedule_run_failed", "the run did not finish in time")]
