"""EL-7011 US-19 R1.1, R1.4: the tick fails the waiting runs whose approval time ran out.

Real: the tick RPC, `expire_waiting_runs`, `apply_schedule_decision` and the schedule models, over SQLite.
Faked: the claim, the starter and the notice helper (all through the `world` fixture of the agent run test).

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_approval_expiry_sweep.py
"""
import sys
import types
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_agent_schedule_run import PKG, world  # noqa: F401 -- pytest fixture
from unit.rpc.test_7011_entity_schedules_check import load_tick, tick_caller

EXPIRED, EXPECTED_REASON = "schedule_approval_expired", "approval expired"


@pytest.fixture
def sweep(world, monkeypatch):  # noqa: F811 -- the fixture, extended
    """`world` plus a tick over the real run writers; the one seeded run waits, expired a minute ago."""
    runs, tools = sys.modules[f"{PKG}.utils.schedule_runs"], sys.modules["tools"]
    models = sys.modules[f"{PKG}.models.entity_schedule"]
    state = types.SimpleNamespace(maintenance=False, logged=[])
    quiet = lambda *a, **k: None
    # The agent schedule also tells the owner's open chats; the world stub has only the notice helper
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished", quiet,
                        raising=False)
    stubs = {
        "utils.maintenance_gate": {"is_maintenance_active": lambda: state.maintenance},
        "utils.schedule_claim": {
            "select_due_schedules": lambda *a: [], "claim_schedule": quiet, "insert_cron_run": quiet,
            "legacy_lock_key": quiet, "fail_run": quiet},
        "utils.schedule_dispatch": {"RUN_STARTERS": {"pipeline": print}, "start_schedule_run": quiet, "START_FAILED": ""},
        "utils.schedule_timing": {"compute_next_run": quiet},
    }
    tick = load_tick(
        monkeypatch, stubs, db=tools.db, project_list=lambda **_: [{"id": 34}], schedules={},
        log=types.SimpleNamespace(info=quiet, warning=quiet, exception=lambda msg, *a: state.logged.append(msg % a)))
    monkeypatch.setattr(tick, "_run_legacy_step", quiet)

    def expire_at(offset_minutes, status="waiting_for_approval"):
        with tools.db.get_session(34) as session:
            run = session.get(models.EntityScheduleRun, 1)
            run.status = status
            run.approval_expires_at = None if offset_minutes is None else datetime.now(UTC) + timedelta(
                minutes=offset_minutes)
            session.commit()

    expire_at(-1)
    state.tick, state.expire_at, state.runs = tick_caller(tick), expire_at, runs
    state.world = world
    return state


def _expiry_notices(sweep):
    return [notice for notice in sweep.world.notices if notice[1] == EXPIRED]


def test_el_7011_r1_1_tick_fails_the_expired_waiting_run(sweep):
    """TC-US19-01: the run fails with the reason, the schedule copies the status, one expiry notice only."""
    sweep.tick()
    run = sweep.world.run_row()
    assert (run.status, run.reason) == ("failed", EXPECTED_REASON)
    assert run.finished_at is not None and run.approval_expires_at is not None
    assert sweep.world.schedule().last_run_status == "failed"
    assert sweep.world.notices == [(12, EXPIRED, EXPECTED_REASON)]


@pytest.mark.parametrize("status, offset", [("waiting_for_approval", 5), ("waiting_for_approval", None),
                                            ("running", -1)])
def test_el_7011_r1_1_tick_leaves_runs_that_are_not_expired_waiting(sweep, status, offset):
    """TC-US19-02: a future limit, no limit, or another status stays as it is."""
    sweep.expire_at(offset, status)
    sweep.tick()
    assert sweep.world.run_row().status == status
    assert not sweep.world.notices


def test_el_7011_r1_1_late_approve_after_the_sweep_is_refused(sweep):
    """TC-US19-03: the expiry won, so the owner decision finds a run that no longer waits."""
    sweep.tick()
    with pytest.raises(sweep.runs.ScheduleRunNotWaiting):
        sweep.runs.apply_schedule_decision(34, 1, "approve")
    assert sweep.world.run_row().status == "failed"


def test_el_7011_r1_4_second_replica_changes_nothing_and_sends_no_notice(sweep):
    """TC-US19-14: the conditional update has one winner, so a second sweep of the run is silent."""
    sweep.tick()
    sweep.tick()
    assert sweep.world.run_row().status == "failed"
    assert len(_expiry_notices(sweep)) == 1


def test_el_7011_r1_4_maintenance_tick_does_not_sweep(sweep):
    """TC-US19-15: the expired run waits for the first tick outside maintenance."""
    sweep.maintenance = True
    sweep.tick()
    assert sweep.world.run_row().status == "waiting_for_approval"
    sweep.maintenance = False
    sweep.tick()
    assert sweep.world.run_row().status == "failed"


def test_el_7011_r1_4_failing_expiry_is_logged_and_the_tick_goes_on(sweep, monkeypatch):
    """The expiry has its own try: a failure is logged and the stale sweep and dispatch still run."""
    monkeypatch.setattr(sweep.runs, "expire_waiting_runs", lambda *a: 1 / 0)
    monkeypatch.setattr(sweep.runs, "fail_stale_runs", lambda *a: sweep.logged.append("stale ran") or 0)
    sweep.tick()
    assert any("expiry sweep failed" in message for message in sweep.logged) and "stale ran" in sweep.logged
