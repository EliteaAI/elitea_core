"""EL-7011 US-08: due query, claim and cron run insert of the schedule tick.

Contract under test: utils/schedule_claim.py (select_due_schedules, claim_schedule, insert_cron_run)
on the real models, in-memory SQLite; the partial unique index has a SQLite form for this.
"""
import sys
import types
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Column, Integer, Table, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Session
from unit._tool_spend_loader import load

# Naive UTC, as SQLite returns it; SEEN is in the past, so a row with it is due
SEEN = datetime(2026, 1, 1, 10, 0)
NEXT = datetime(2026, 1, 1, 11, 0)


@pytest.fixture
def claim(monkeypatch):
    """schedule_claim loaded on the real schedule models with a fresh declarative base."""
    class Base(DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):
        Table(parent, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
    stubs = {
        "db": types.SimpleNamespace(Base=Base),
        "db_tools": types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {})),
        "config": types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"),
    }
    package = types.ModuleType("plugins.elitea_core.models")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models", package)
    for name, stub in stubs.items():
        setattr(package, name, stub)
        monkeypatch.setitem(sys.modules, "plugins.elitea_core.models." + name, stub)
    for dotted in ("models.entity_schedule", "utils.schedule_claim"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    load("models/entity_schedule.py", fresh=True)
    return load("utils/schedule_claim.py", fresh=True)


@pytest.fixture
def session(claim):
    engine = create_engine("sqlite://", execution_options={"schema_translate_map": {"tenant": None}})
    metadata = claim.EntitySchedule.metadata
    claim.EntitySchedule.__table__.c.variables.server_default = None  # '[]'::jsonb is Postgres only
    metadata.create_all(engine)
    with Session(engine) as db_session:
        db_session.execute(metadata.tables["tenant.applications"].insert().values(id=1))
        yield db_session


def _schedule(claim, session, **values):
    row = claim.EntitySchedule(**{
        "entity_type": "pipeline", "application_id": 1, "name": "S1", "cron": "0 * * * *",
        "timezone": "UTC", "created_by": 1, "principal": {"kind": "mandate"}, "next_run": SEEN, **values,
    })
    session.add(row)
    session.flush()
    return row


def _runs(claim, session, schedule_id):
    return session.scalars(
        select(claim.EntityScheduleRun).where(claim.EntityScheduleRun.schedule_id == schedule_id)
    ).all()


def test_el_7011_r1_1_due_query_returns_enabled_due_rows_of_the_types_oldest_first(claim, session):
    later = _schedule(claim, session, next_run=SEEN)
    earlier = _schedule(claim, session, next_run=SEEN - timedelta(hours=1))
    _schedule(claim, session, enabled=False)
    _schedule(claim, session, entity_type="agent")
    _schedule(claim, session, next_run=datetime(2999, 1, 1))
    _schedule(claim, session, next_run=None)

    due = claim.select_due_schedules(session, ["pipeline"], 10)

    assert [row.id for row in due] == [earlier.id, later.id]


def test_el_7011_r1_1_due_query_stops_at_the_limit(claim, session):
    first = _schedule(claim, session, next_run=SEEN - timedelta(hours=1))
    _schedule(claim, session)

    assert [row.id for row in claim.select_due_schedules(session, ["pipeline"], 1)] == [first.id]


# TC-US08-02
def test_el_7011_r1_1_r1_2_claim_moves_next_run_and_sets_last_run(claim, session):
    row = _schedule(claim, session)

    assert claim.claim_schedule(session, row.id, SEEN, NEXT) is True

    session.expire_all()
    stored = session.get(claim.EntitySchedule, row.id)
    assert stored.next_run.replace(tzinfo=None) == NEXT
    now = datetime.now(UTC).replace(tzinfo=None)
    assert abs(stored.last_run.replace(tzinfo=None) - now) < timedelta(minutes=1)


# TC-US08-03
def test_el_7011_r1_1_lost_claim_changes_nothing(claim, session):
    row = _schedule(claim, session, next_run=NEXT)

    assert claim.claim_schedule(session, row.id, SEEN, NEXT + timedelta(hours=1)) is False

    session.expire_all()
    stored = session.get(claim.EntitySchedule, row.id)
    assert stored.next_run.replace(tzinfo=None) == NEXT
    assert stored.last_run is None
    assert _runs(claim, session, row.id) == []


def test_el_7011_r1_1_second_claim_of_the_same_due_time_loses(claim, session):
    row = _schedule(claim, session)

    assert claim.claim_schedule(session, row.id, SEEN, NEXT) is True
    assert claim.claim_schedule(session, row.id, SEEN, NEXT) is False


# TC-US08-06
@pytest.mark.parametrize("finished", ["success", "failed", "skipped"])
def test_el_7011_r2_2_cron_run_is_inserted_as_running_beside_a_finished_run(claim, session, finished):
    row = _schedule(claim, session)
    session.add(claim.EntityScheduleRun(schedule_id=row.id, trigger="cron", status=finished))
    session.flush()

    run_id = claim.insert_cron_run(session, row.id)

    run = session.get(claim.EntityScheduleRun, run_id)
    assert (run.schedule_id, run.trigger, run.status) == (row.id, "cron", "running")


# TC-US08-07, TC-US08-08
@pytest.mark.parametrize("active", ["running", "waiting_for_approval"])
def test_el_7011_r2_1_active_run_blocks_a_second_run_and_keeps_the_claim(claim, session, active):
    row = _schedule(claim, session)
    session.add(claim.EntityScheduleRun(schedule_id=row.id, trigger="cron", status=active))
    session.flush()
    assert claim.claim_schedule(session, row.id, SEEN, NEXT) is True

    assert claim.insert_cron_run(session, row.id) is None

    assert [run.status for run in _runs(claim, session, row.id)] == [active]
    session.expire_all()
    assert session.get(claim.EntitySchedule, row.id).next_run.replace(tzinfo=None) == NEXT


def test_el_7011_r4_2_legacy_lock_key_is_stable_per_project(claim):
    """R4.2: the legacy step and the migration (US-24) take the same key; it never depends on the process."""
    assert [claim.legacy_lock_key(project_id) for project_id in (1, 5)] == [(7011 << 32) + 1, (7011 << 32) + 5]
