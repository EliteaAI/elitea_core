"""EL-7011 US-13 R2.1, R3.1 to R3.3: the tick records Skipped at the claim and sweeps stale runs.

Contract under test: rpc/entity_schedules_check.py. The claim, the writers of utils/schedule_runs.py
and the starter are faked, so the test pins what the tick calls and with which values.
"""
import contextlib
import types
from datetime import UTC, datetime

import pytest
from unit.rpc.test_7011_entity_schedules_check import load_tick, tick_caller

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
SKIP_REASON = "the previous run is still running"


class FakeSession:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


class World:
    def __init__(self):
        self.projects, self.config = [1], {"dispatch_cap": 100}
        self.rows, self.conflict, self.fail_sweep = {}, set(), set()
        self.skipped, self.swept, self.started, self.sessions, self.logged = [], [], [], [], []
        self.fail_skip = False

    def insert_run(self, session, schedule_id):
        return None if schedule_id in self.conflict else schedule_id * 10

    def record_skipped(self, session, schedule_id, reason):
        if self.fail_skip:
            raise RuntimeError("db down")
        self.skipped.append((schedule_id, reason, session.commits))
        return 1

    def fail_stale(self, project_id, now, stale_minutes):
        if project_id in self.fail_sweep:
            raise RuntimeError("db down")
        self.swept.append((project_id, stale_minutes))
        return 0

    @contextlib.contextmanager
    def get_session(self, project_id):
        session = FakeSession()
        self.sessions.append(session)
        yield session


@pytest.fixture
def world(monkeypatch):
    world = World()
    quiet = lambda *a, **k: None
    stubs = {
        "utils.schedule_claim": {
            "select_due_schedules": lambda session, types_, limit: world.rows.get(1, []),
            "claim_schedule": lambda *a: True, "insert_cron_run": world.insert_run,
            "legacy_lock_key": lambda project_id: project_id, "fail_run": quiet,
        },
        "utils.schedule_dispatch": {
            "RUN_STARTERS": {"pipeline": print},
            "start_schedule_run": lambda *a: world.started.append(a) or True, "START_FAILED": "",
        },
        "utils.schedule_timing": {"compute_next_run": lambda cron, zone, after: NOW},
        "utils.schedule_runs": {"record_skipped_run": world.record_skipped, "fail_stale_runs": world.fail_stale},
    }
    tick = load_tick(
        monkeypatch, stubs, db=types.SimpleNamespace(get_session=world.get_session),
        project_list=lambda **_: [{"id": p} for p in world.projects], schedules=world.config,
        log=types.SimpleNamespace(info=quiet, warning=quiet, exception=lambda msg, *a: world.logged.append(msg)),
    )
    monkeypatch.setattr(tick, "_run_legacy_step", lambda *a: None)
    world.tick = tick_caller(tick)
    return world


def _row(row_id):
    return types.SimpleNamespace(id=row_id, entity_type="pipeline", cron="* * * * *", timezone="UTC", next_run=NOW,
                                 expires_at=None, name="schedule", created_by=1)


def test_7011_r2_1_active_run_at_the_claim_records_a_skipped_run(world):
    """TC-US13-07: the claim stays committed, then the skipped run is written and committed."""
    world.rows, world.conflict = {1: [_row(1), _row(2)]}, {1}
    world.tick()
    assert world.skipped == [(1, SKIP_REASON, 1)]
    assert world.sessions[0].commits == 3  # claim of 1, skipped run of 1, claim of 2
    assert [started[2] for started in world.started] == [2]


def test_7011_r2_1_a_started_run_records_no_skipped_run(world):
    """TC-US13-08"""
    world.rows = {1: [_row(1)]}
    world.tick()
    assert world.skipped == []


def test_7011_r3_1_every_project_is_swept_even_when_the_cap_is_spent(world):
    """TC-US13-12"""
    world.projects, world.config["dispatch_cap"] = [1, 2], 0
    world.tick()
    assert [project for project, _ in world.swept] == [1, 2]


def test_7011_r3_3_stale_minutes_default_to_120(world):
    """TC-US13-13"""
    world.tick()
    assert world.swept == [(1, 120)]


def test_7011_r3_1_stale_minutes_come_from_the_config(world):
    world.config["stale_run_minutes"] = 45
    world.tick()
    assert world.swept == [(1, 45)]


def test_7011_r3_1_a_failing_sweep_does_not_stop_the_next_project(world):
    """TC-US13-14"""
    world.projects, world.fail_sweep = [1, 2], {1}
    world.tick()
    assert world.swept == [(2, 120)]


@pytest.mark.parametrize(("configured", "used"), [(0, 120), (None, 120), (-5, 1), ("30", 30)])
def test_7011_r3_1_stale_minutes_are_bounded_to_at_least_one(world, configured, used):
    world.config["stale_run_minutes"] = configured
    world.tick()
    assert world.swept == [(1, used)]


def test_7011_r2_1_a_failed_skip_record_is_logged_and_the_next_row_still_starts(world):
    world.rows, world.conflict, world.fail_skip = {1: [_row(1), _row(2)]}, {1}, True
    world.tick()
    assert any("skipped run not recorded" in message for message in world.logged)
    assert [started[2] for started in world.started] == [2]
