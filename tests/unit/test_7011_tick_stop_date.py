"""EL-7011 US-14 R3.1 to R3.4: the tick disables a schedule past its stop date and notifies its owner.

Contract under test: rpc/entity_schedules_check.py. The claim, the starter, the model and the notice
helper are faked, so the test pins what the tick writes and sends, not how Postgres answers.
"""
import contextlib
import types
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfoNotFoundError

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from unit.rpc.test_7011_entity_schedules_check import load_tick, tick_caller

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
PAST, FUTURE = datetime.now(UTC) - timedelta(days=1), datetime.now(UTC) + timedelta(days=1)  # the tick reads the clock


class FakeEntitySchedule(declarative_base()):
    __tablename__ = "entity_schedules"
    id = sa.Column(sa.Integer, primary_key=True)
    enabled = sa.Column(sa.Boolean)
    disabled_reason = sa.Column(sa.String)


class FakeSession:
    def __init__(self, world):
        self.world, self.commits = world, 0

    def execute(self, statement):
        if isinstance(statement, sa.sql.Select):  # the database clock
            return types.SimpleNamespace(scalar=lambda: self.world.db_now or datetime.now(UTC))
        params = statement.compile().params
        self.world.updates.append(params)
        return types.SimpleNamespace(rowcount=0 if params["id_1"] in self.world.taken else 1)

    def commit(self):
        self.commits += 1
        self.world.events.append("commit")

    def rollback(self):
        self.world.events.append("rollback")


class World:
    def __init__(self):
        self.rows, self.taken, self.fail_update = [], set(), False
        self.updates, self.claims, self.notices, self.started, self.events = [], [], [], [], []
        self.db_now, self.next_run_after = None, []

    def notify(self, project_id, owner_id, event_type, schedule, extra=None):
        self.events.append("notice")
        self.notices.append((project_id, owner_id, event_type, schedule.id, schedule.name))
        return True

    @contextlib.contextmanager
    def get_session(self, project_id):
        yield FakeSession(self)


@pytest.fixture
def world(monkeypatch):
    world = World()
    quiet = lambda *a, **k: None

    def claim(session, schedule_id, seen, new):
        world.claims.append(schedule_id)
        return True

    def next_run(cron, zone, after):
        world.next_run_after.append(after)
        if cron == "bad cron":
            raise ValueError(cron)
        if zone == "Mars/Base":
            raise ZoneInfoNotFoundError(zone)
        return NOW

    stubs = {
        "utils.schedule_claim": {
            "select_due_schedules": lambda session, types_, limit: world.rows,
            "claim_schedule": claim, "insert_cron_run": lambda session, schedule_id: schedule_id * 10,
            "legacy_lock_key": lambda project_id: project_id, "fail_run": quiet,
        },
        "utils.schedule_dispatch": {
            "RUN_STARTERS": {"pipeline": print},
            "start_schedule_run": lambda *a: world.started.append(a[2]) or True, "START_FAILED": "",
        },
        "utils.schedule_timing": {"compute_next_run": next_run},
        "utils.schedule_runs": {"record_skipped_run": quiet, "fail_stale_runs": lambda *a: 0,
                                "expire_waiting_runs": lambda *a: 0},
        "models.entity_schedule": {"EntitySchedule": FakeEntitySchedule},
        "models.enums.all": {"NotificationEventTypes": types.SimpleNamespace(
            schedule_expired="schedule_expired", schedule_disabled="schedule_disabled")},
        "utils.schedule_notifications": {"notify_schedule_event": world.notify},
    }
    tick = load_tick(
        monkeypatch, stubs, db=types.SimpleNamespace(get_session=world.get_session),
        project_list=lambda **_: [{"id": 1}], schedules={"dispatch_cap": 100},
        log=types.SimpleNamespace(info=quiet, warning=quiet, exception=quiet),
    )
    monkeypatch.setattr(tick, "_run_legacy_step", lambda *a: None)
    world.module = tick
    world.tick = tick_caller(tick)
    return world


def _row(row_id, expires_at, owner=7, cron="* * * * *", timezone="UTC"):
    return types.SimpleNamespace(
        id=row_id, entity_type="pipeline", cron=cron, timezone=timezone, next_run=NOW,
        expires_at=expires_at, name=f"nightly {row_id}", created_by=owner,
    )


def test_7011_r3_1_expired_row_is_disabled_with_reason_expired(world):
    """TC-US14-08"""
    world.rows = [_row(1, PAST)]
    world.tick()
    assert [(u["enabled"], u["disabled_reason"], u["id_1"]) for u in world.updates] == [(False, "expired", 1)]


def test_7011_r3_2_expired_row_starts_no_run_and_sends_one_notice_to_the_owner(world):
    """TC-US14-09: no claim, no run; one notice after the commit"""
    world.rows = [_row(1, PAST, owner=7)]
    world.tick()
    assert not world.claims and not world.started
    assert world.notices == [(1, 7, "schedule_expired", 1, "nightly 1")]
    assert world.events == ["commit", "notice"]


def test_7011_r3_4_row_taken_by_another_replica_sends_no_notice(world):
    """TC-US14-10"""
    world.rows, world.taken = [_row(1, PAST)], {1}
    world.tick()
    assert not world.notices and not world.started


def test_7011_r3_1_row_without_a_stop_date_or_before_it_runs_as_before(world):
    """TC-US14-11"""
    world.rows = [_row(1, None), _row(2, FUTURE)]
    world.tick()
    assert not world.updates and not world.notices
    assert world.started == [1, 2]


def test_7011_r3_3_expired_and_live_rows_in_one_tick_are_handled_each_its_own_way(world):
    """TC-US14-12"""
    world.rows = [_row(1, PAST), _row(2, FUTURE)]
    world.tick()
    assert [n[3] for n in world.notices] == [1]
    assert world.started == [2]


def test_7011_r3_1_a_failing_update_does_not_stop_the_next_row(world, monkeypatch):
    world.rows = [_row(1, PAST), _row(2, FUTURE)]
    def broken(self, statement):
        if isinstance(statement, sa.sql.Select):
            return types.SimpleNamespace(scalar=lambda: datetime.now(UTC))
        raise RuntimeError("db")

    monkeypatch.setattr(FakeSession, "execute", broken)
    world.tick()
    assert world.events[0] == "rollback" and not world.notices
    assert world.started == [2]


def test_a_bad_cron_and_a_bad_time_zone_are_disabled_once_and_the_next_row_runs(world):
    world.rows = [_row(1, None, cron="bad cron"), _row(2, None, timezone="Mars/Base"), _row(3, None)]
    world.tick()
    assert [(u["enabled"], u["disabled_reason"], u["id_1"]) for u in world.updates] == [
        (False, "invalid_cron", 1), (False, "invalid_cron", 2)]
    assert [n[2:4] for n in world.notices] == [("schedule_disabled", 1), ("schedule_disabled", 2)]
    assert world.claims == [3] and world.started == [3]


def test_expiry_and_the_next_run_follow_the_database_clock(world):
    world.db_now = datetime(2020, 1, 1, tzinfo=UTC)
    world.rows = [_row(1, datetime(2020, 6, 1, tzinfo=UTC)), _row(2, datetime(2019, 6, 1, tzinfo=UTC))]
    world.tick()
    assert world.next_run_after == [world.db_now]
    assert [n[3] for n in world.notices] == [2] and world.started == [1]


def test_a_tick_that_finds_the_previous_one_running_ends_at_once(world):
    world.rows = [_row(1, None)]
    assert world.module._tick_lock.acquire(blocking=False)
    try:
        world.tick()
    finally:
        world.module._tick_lock.release()
    assert not world.started and not world.claims
    world.tick()
    assert world.started == [1]
