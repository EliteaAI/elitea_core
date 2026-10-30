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

REFUSED = {"code": "MANDATE_REQUIRED", "error": "This scheduled run needs an approved mandate."}
OUTAGE = {"code": "DEPENDENCY_UNAVAILABLE", "error": "A required service is unavailable. Try again later."}
ADMITTED = {"task_id": "task-1"}


@pytest.fixture(autouse=True)
def _timing():
    """The run writers import the timing helpers; the US-11 harness finds them only when an earlier test loaded them."""
    if f"{PKG}.utils.schedule_timing" not in sys.modules:
        load("utils/schedule_timing.py")


@pytest.fixture
def firing(world, monkeypatch):  # noqa: F811 -- the fixture extends the imported one
    """`fire(answer, ...)`: claim a run of schedule 1 and start it with `answer` as the predict answer."""
    starter = sys.modules[f"{PKG}.utils.schedule_run_starter"]
    schedules = sys.modules[f"{PKG}.models.entity_schedule"]
    tools = sys.modules["tools"]
    state = types.SimpleNamespace(governed=True, answers=[])
    mandate = types.ModuleType(f"{PKG}.utils.mandate_admission")
    mandate.prepare_schedule_run = lambda **_kwargs: {"unattended": True} if state.governed else {}
    mandate.mint_owner_run_ticket = lambda request: "owner-ticket"   # the owner's run (#7254): minted at the send
    monkeypatch.setitem(sys.modules, mandate.__name__, mandate)
    monkeypatch.setattr(starter, "execute_pipeline_via_predict_sio", lambda **_kwargs: state.answers.pop(0))
    monkeypatch.setattr(starter, "finish_trigger_run_refused", lambda *_args: None)  # the chat end has its own tests
    monkeypatch.setattr(starter, "run_prerun_check", lambda _project, schedule, _run: types.SimpleNamespace(
        ok=True, owner_id=schedule.created_by))   # the pre-run check has its own tests
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished",
                        lambda *_args: None, raising=False)
    world.set_run_status("success")   # the fixture's run is over; each firing claims its own

    def fire(answer, governed=True, trigger="cron", owner_request=None):
        """A cron firing by default; ``manual``/``test`` with ``owner_request`` is the owner's Run now / Test run."""
        state.governed = governed
        state.answers.append(answer)
        run_id = world.claim(trigger)
        starter.run_owner_schedule(PROJECT_ID, 1, run_id, *(() if owner_request is None else (owner_request,)))
        if answer is ADMITTED:   # the admitted run ends later, from its chat; end it so the next firing can claim
            with tools.db.get_session(PROJECT_ID) as session:
                session.get(schedules.EntityScheduleRun, run_id).status = "success"
                session.commit()
        return run_id

    def schedule():
        """(refusal_streak, enabled, disabled_reason) of schedule 1; the streak is a table column only."""
        S = schedules.EntitySchedule
        with tools.db.get_session(PROJECT_ID) as session:
            return tuple(session.execute(
                _sa.select(schedules.REFUSAL_STREAK, S.enabled, S.disabled_reason).where(S.id == 1)).one())

    def set_streak(value):
        with tools.db.get_session(PROJECT_ID) as session:
            session.execute(_sa.update(schedules.EntitySchedule).values({schedules.REFUSAL_STREAK: value}))
            session.commit()

    def run_reason(run_id):
        with tools.db.get_session(PROJECT_ID) as session:
            run = session.get(schedules.EntityScheduleRun, run_id)
            return run.status, run.reason

    def set_off(reason):
        """The schedule switched off by something else first (by hand: reason None; or `expired`)."""
        with tools.db.get_session(PROJECT_ID) as session:
            session.execute(_sa.update(schedules.EntitySchedule).values(enabled=False, disabled_reason=reason))
            session.commit()

    def resume():
        """Resume through the schedule service, as the resume action does (owner scope)."""
        service = _service(monkeypatch)
        with tools.db.get_session(PROJECT_ID) as session:
            row = session.get(schedules.EntitySchedule, 1)
            service.update_schedule(session, PROJECT_ID, row, _update_model().ScheduleUpdate(enabled=True),
                                    service.ScheduleScope(user_id=OWNER, is_admin=False, permissions=frozenset()))

    return types.SimpleNamespace(fire=fire, schedule=schedule, set_streak=set_streak, run_reason=run_reason,
                                 notices=world.notices, set_off=set_off, resume=resume)


def _update_model():
    if f"{PKG}.models.pd.entity_schedule" not in sys.modules:
        load("models/pd/index.py")
        load("models/pd/entity_schedule.py")
    return sys.modules[f"{PKG}.models.pd.entity_schedule"]


def _service(monkeypatch):
    """The real schedule service on the US-11 world; its permission and folder neighbours are inert here."""
    tools = sys.modules["tools"]
    monkeypatch.setattr(tools, "auth", types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(tools, "config", types.SimpleNamespace(ADMINISTRATION_MODE="administration",
                                                               DEFAULT_MODE="default"), raising=False)
    folder = types.ModuleType(f"{PKG}.utils.folder_access")
    folder.entity_access_error = lambda *_a, **_k: None
    monkeypatch.setitem(sys.modules, folder.__name__, folder)
    monkeypatch.setattr(sys.modules[f"{PKG}.models.enums.all"], "AgentTypes", types.SimpleNamespace(), raising=False)
    _update_model()
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_timing"], "this", types.SimpleNamespace(
        module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))))   # default limits
    load("utils/schedule_access.py")
    return load("utils/schedule_service.py", fresh=True)


def test_three_refusals_in_a_row_pause_the_schedule_and_tell_the_owner_once(firing):
    runs = [firing.fire(REFUSED) for _ in range(3)]   # cron firings
    assert firing.schedule() == (3, False, "governance_refused")
    for run_id in runs:   # each run keeps its own refusal text
        assert firing.run_reason(run_id) == ("failed", f"The run failed: {REFUSED['error']}")
    paused = [notice for notice in firing.notices if notice[1] == "schedule_disabled"]
    assert paused == [(OWNER, "schedule_disabled",
                       "3 runs in a row were refused (The run failed: This scheduled run needs an approved mandate). "
                       "Fix the cause and resume it.")]
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


def test_an_outage_refusal_neither_counts_nor_ends_the_streak(firing):
    """#7254 review M1: a platform outage (``*_UNAVAILABLE``) is not a mandate problem."""
    firing.fire(REFUSED)
    firing.fire(OUTAGE)
    firing.fire(REFUSED)
    assert firing.schedule() == (2, True, None)
    for _ in range(3):
        firing.fire(OUTAGE)
    assert firing.schedule() == (2, True, None)
    assert all(notice[1] != "schedule_disabled" for notice in firing.notices)


@pytest.mark.parametrize("reason", [None, "expired"])
def test_refusals_of_a_schedule_already_off_keep_its_reason_and_send_no_pause_notice(firing, reason):
    """#7254 review S1: Run now of a paused (or expired) schedule is refused three times."""
    firing.set_off(reason)
    for _ in range(3):
        firing.fire(REFUSED)
    assert firing.schedule() == (3, False, reason)
    assert all(notice[1] != "schedule_disabled" for notice in firing.notices)


def test_the_whole_sequence_pause_resume_one_more_refusal(firing):
    """#7254 review S3: on the same rows, in order, with an outage inside the streak."""
    firing.fire(REFUSED)
    firing.fire(OUTAGE)   # not counted
    firing.fire(REFUSED)
    firing.fire(REFUSED)
    assert firing.schedule() == (3, False, "governance_refused")
    assert [notice[1] for notice in firing.notices].count("schedule_disabled") == 1
    firing.resume()
    assert firing.schedule() == (0, True, None)
    firing.fire(REFUSED)
    assert firing.schedule() == (1, True, None)
    assert [notice[1] for notice in firing.notices].count("schedule_disabled") == 1


@pytest.mark.parametrize("code", ["DENIED_ASSERTION_UNAVAILABLE", "DENIED_OAUTH_APP_UNAVAILABLE"])
def test_a_missing_sign_in_or_app_counts_like_any_refusal(firing, code):
    """A lasting condition a person fixes is not an outage: three in a row pause the schedule."""
    for _ in range(3):
        firing.fire({"code": code, "error": "Not available to this run."})
    assert firing.schedule() == (3, False, "governance_refused")


@pytest.mark.parametrize("trigger", ["manual", "test"])
@pytest.mark.parametrize("answer", [{"code": "DENIED_GRANT", "error": "No grant."}, ADMITTED])
def test_run_now_and_test_run_neither_count_nor_end_the_streak(firing, trigger, answer):
    """Roman 2026-10-30: the owner's own runs are not cron firings; only cron firings count toward the pause."""
    firing.set_streak(2)
    for _ in range(3):
        firing.fire(answer, trigger=trigger, owner_request=object())
    assert firing.schedule() == (2, True, None)
    assert all(notice[1] != "schedule_disabled" for notice in firing.notices)
