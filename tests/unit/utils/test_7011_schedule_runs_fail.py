"""EL-7011 US-09: run failure and schedule disable writers of the pre-run check.

Contract under test: utils/schedule_runs.py (fail_schedule_run, disable_schedule) on the real models,
in-memory SQLite; `tools.db.get_session` hands out the test session.
"""
import sys
import types
from contextlib import contextmanager

import pytest
from sqlalchemy import Column, Integer, Table, create_engine
from sqlalchemy.orm import DeclarativeBase, Session
from unit._tool_spend_loader import load


@pytest.fixture
def runs(monkeypatch):
    """schedule_runs loaded on the real schedule models with a fresh declarative base."""
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
    for dotted in ("models.entity_schedule", "utils.schedule_claim", "utils.schedule_runs"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    load("models/entity_schedule.py", fresh=True)
    load("utils/schedule_claim.py", fresh=True)  # fail_run, the one writer of a failed run
    # The `tools` stub: schedule_runs imports `db` from it at load time
    module = types.ModuleType("tools")
    module.db = types.SimpleNamespace(get_session=None)
    monkeypatch.setitem(sys.modules, "tools", module)
    runs = load("utils/schedule_runs.py", fresh=True)
    runs.EntityScheduleRun = sys.modules["plugins.elitea_core.models.entity_schedule"].EntityScheduleRun
    return runs


@pytest.fixture
def session(runs, monkeypatch):
    engine = create_engine("sqlite://", execution_options={"schema_translate_map": {"tenant": None}})
    metadata = runs.EntitySchedule.metadata
    runs.EntitySchedule.__table__.c.variables.server_default = None  # '[]'::jsonb is Postgres only
    metadata.create_all(engine)
    with Session(engine) as db_session:
        db_session.execute(metadata.tables["tenant.applications"].insert().values(id=1))

        @contextmanager
        def get_session(project_id):
            yield db_session

        monkeypatch.setattr(sys.modules["tools"].db, "get_session", get_session)
        yield db_session


def _schedule(runs, session, **values):
    row = runs.EntitySchedule(**{
        "entity_type": "agent", "application_id": 1, "name": "S1", "cron": "0 * * * *",
        "timezone": "UTC", "created_by": 1, "principal": {"kind": "delegation", "ref": 1}, **values,
    })
    session.add(row)
    session.flush()
    return row


def _run(runs, session, schedule, status="running"):
    row = runs.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status=status)
    session.add(row)
    session.flush()
    return row


def test_el_7011_r1_4_fail_schedule_run_records_the_reason_and_the_end_of_a_running_run(runs, session):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.fail_schedule_run(1, run.id, "owner lost access")

    session.refresh(run)
    assert (run.status, run.reason) == ("failed", "owner lost access")
    assert run.finished_at is not None


def test_el_7011_r1_4_fail_schedule_run_sets_last_run_status_of_the_schedule(runs, session):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.fail_schedule_run(1, run.id, "owner lost access")

    session.refresh(schedule)
    assert schedule.last_run_status == "failed"


def test_el_7011_r1_4_fail_schedule_run_leaves_a_finished_run_as_it_is(runs, session):
    schedule = _schedule(runs, session, last_run_status="succeeded")
    run = _run(runs, session, schedule, status="succeeded")

    runs.fail_schedule_run(1, run.id, "owner lost access")

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("succeeded", None)
    assert schedule.last_run_status == "succeeded"


def test_el_7011_r2_5_fail_schedule_run_keeps_the_schedule_enabled(runs, session):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.fail_schedule_run(1, run.id, "no token for the owner")

    session.refresh(schedule)
    assert schedule.enabled is True
    assert schedule.disabled_reason is None


def test_el_7011_r1_5_disable_schedule_switches_off_with_the_reason(runs, session):
    schedule = _schedule(runs, session)

    runs.disable_schedule(1, schedule.id, "owner_lost_access")

    session.refresh(schedule)
    assert (schedule.enabled, schedule.disabled_reason) == (False, "owner_lost_access")


def test_el_7011_r1_5_disable_schedule_changes_only_the_named_schedule(runs, session):
    target = _schedule(runs, session)
    other = _schedule(runs, session, name="S2", created_by=2)

    runs.disable_schedule(1, target.id, "owner_lost_access")

    session.refresh(other)
    assert (other.enabled, other.disabled_reason) == (True, None)
