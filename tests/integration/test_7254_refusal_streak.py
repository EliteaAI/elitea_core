"""#7254 (C-1009): a schedule pauses itself after three governance refusals in a row and tells its owner once.

Real: `run_owner_schedule`, the run writers of `utils/schedule_runs.py` (the refusal count included) and the schedule
models, over SQLite tables: the `world` fixture of the US-11 integration test. Faked: the pre-run check (it passes), the
governed preparation (`prepare_schedule_run`: under governance it answers the `unattended` flag, off it answers
nothing) and the predict answer (a refusal is `{code, error}`, as `predict_sio` answers it).

Run via:
    python tests/run_tests.py -m integration -v integration/test_7254_refusal_streak.py
"""
import sys
import types

import pytest
from integration.test_7011_agent_schedule_run import OWNER, PKG, PROJECT_ID, world  # noqa: F401 -- fixture reuse
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY

_sa = REAL_SQLALCHEMY["sqlalchemy"]

from unit._tool_spend_loader import load

REFUSED = {"code": "MANDATE_REQUIRED", "error": "This scheduled run needs an approved mandate"}
ADMITTED = {"task_id": "task-1"}


@pytest.fixture(autouse=True)
def _timing():
    """The run writers import the timing helpers; the US-11 harness finds them only when an earlier test loaded them."""
    if f"{PKG}.utils.schedule_timing" not in sys.modules:
        load("utils/schedule_timing.py")


@pytest.fixture
def firing(world, monkeypatch):  # noqa: F811 -- the fixture extends the imported one
    """`fire(answer, governed=True)`: claim a Run now of schedule 1 and start it with `answer` as the predict answer."""
    starter = sys.modules[f"{PKG}.utils.schedule_run_starter"]
    schedules = sys.modules[f"{PKG}.models.entity_schedule"]
    tools = sys.modules["tools"]
    state = types.SimpleNamespace(governed=True, answers=[])
    mandate = types.ModuleType(f"{PKG}.utils.mandate_admission")
    mandate.prepare_schedule_run = lambda **_kwargs: {"unattended": True} if state.governed else {}
    monkeypatch.setitem(sys.modules, mandate.__name__, mandate)
    monkeypatch.setattr(starter, "execute_pipeline_via_predict_sio", lambda **_kwargs: state.answers.pop(0))
    monkeypatch.setattr(starter, "finish_trigger_run_refused", lambda *_args: None)  # the chat end has its own tests
    monkeypatch.setattr(starter, "run_prerun_check", lambda _project, schedule, _run: types.SimpleNamespace(
        ok=True, owner_id=schedule.created_by))   # the pre-run check has its own tests
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished",
                        lambda *_args: None, raising=False)
    world.set_run_status("success")   # the fixture's run is over; each firing claims its own

    def fire(answer, governed=True, trigger="manual"):
        state.governed = governed
        state.answers.append(answer)
        run_id = world.claim(trigger)
        starter.run_owner_schedule(PROJECT_ID, 1, run_id)
        if answer is ADMITTED:   # the admitted run ends later, from its chat; end it so the next firing can claim
            with tools.db.get_session(PROJECT_ID) as session:
                session.get(schedules.EntityScheduleRun, run_id).status = "success"
                session.commit()
        return run_id

    def schedule():
        """(refusal_streak, enabled, disabled_reason) of schedule 1; the streak is deferred, so read as columns."""
        S = schedules.EntitySchedule
        with tools.db.get_session(PROJECT_ID) as session:
            return tuple(session.execute(
                _sa.select(S.refusal_streak, S.enabled, S.disabled_reason).where(S.id == 1)).one())

    def set_streak(value):
        with tools.db.get_session(PROJECT_ID) as session:
            session.execute(_sa.update(schedules.EntitySchedule).values(refusal_streak=value))
            session.commit()

    def run_reason(run_id):
        with tools.db.get_session(PROJECT_ID) as session:
            run = session.get(schedules.EntityScheduleRun, run_id)
            return run.status, run.reason

    return types.SimpleNamespace(fire=fire, schedule=schedule, set_streak=set_streak, run_reason=run_reason,
                                 notices=world.notices)


def test_three_refusals_in_a_row_pause_the_schedule_and_tell_the_owner_once(firing):
    runs = [firing.fire(REFUSED, trigger=trigger) for trigger in ("cron", "manual", "test")]   # Run now counts too
    assert firing.schedule() == (3, False, "governance_refused")
    for run_id in runs:   # each run keeps its own refusal text
        assert firing.run_reason(run_id) == ("failed", f"The run failed: {REFUSED['error']}")
    paused = [notice for notice in firing.notices if notice[1] == "schedule_disabled"]
    assert paused == [(OWNER, "schedule_disabled",
                       f"3 runs in a row were refused (The run failed: {REFUSED['error']}). "
                       "Fix the mandate and resume it")]
    assert [notice[1] for notice in firing.notices].count("schedule_run_failed") == 3


def test_two_refusals_do_not_pause_the_schedule(firing):
    firing.fire(REFUSED)
    firing.fire(REFUSED)
    assert firing.schedule() == (2, True, None)
    assert all(notice[1] != "schedule_disabled" for notice in firing.notices)


def test_an_admitted_start_ends_the_streak(firing):
    firing.fire(REFUSED)
    firing.fire(REFUSED)
    firing.fire(ADMITTED)
    assert firing.schedule() == (0, True, None)
    firing.fire(REFUSED)
    assert firing.schedule() == (1, True, None)


def test_a_refusal_after_the_pause_does_not_tell_the_owner_again(firing):
    for _ in range(4):   # the fourth is a Run now of the paused schedule
        firing.fire(REFUSED)
    assert firing.schedule() == (4, False, "governance_refused")
    assert [notice[1] for notice in firing.notices].count("schedule_disabled") == 1


def test_off_an_admitted_start_writes_no_streak(firing):
    firing.set_streak(2)   # left over from an earlier enforce period
    firing.fire(ADMITTED, governed=False)
    assert firing.schedule() == (2, True, None)


