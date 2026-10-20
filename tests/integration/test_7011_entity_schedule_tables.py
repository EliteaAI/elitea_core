"""Schedule tables on SQLite (EL-7011 US-01): rows, cascade and the active run rule.

Covers R2.2, R2.4, R2.5, R3.2, R3.3, R3.4. The model file is real; only the plugin
package around it is stubbed. PostgreSQL-only DDL (JSONB, the `::jsonb` default, the
tenant schema) is adapted for SQLite here; the manual run on the local stack covers it.

Run via:
    python tests/run_tests.py integration/test_7011_entity_schedule_tables.py -v
"""
import pathlib
import sys
import types

import pytest
from sqlalchemy import Column, Integer, Table, create_engine, event, null, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Session

from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PRINCIPAL = {"kind": "mandate"}


@compiles(JSONB, "sqlite")
def _jsonb_as_json(type_, compiler, **kw):
    return "JSON"


@pytest.fixture
def tables(monkeypatch):
    """(models module, SQLite engine, parent tables by name) with the tables created."""
    class Base(DeclarativeBase):
        pass

    parents = {
        name: Table(name, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
        for name in ("applications", "application_versions")
    }

    package = types.ModuleType("elitea_core_7011_tables")
    package.__path__ = []
    package.db = types.SimpleNamespace(Base=Base)
    package.db_tools = types.SimpleNamespace(AbstractBaseMixin=type("Mixin", (), {}))
    package.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant")
    monkeypatch.setitem(sys.modules, package.__name__, package)
    name = f"{package.__name__}.entity_schedule"
    models = load_module_with_stubs(PLUGIN_ROOT / "models" / "entity_schedule.py", name)
    monkeypatch.setitem(sys.modules, name, models)

    models.EntitySchedule.__table__.c.variables.server_default = None  # `::jsonb` is not SQLite
    engine = create_engine("sqlite://").execution_options(schema_translate_map={"tenant": None})

    @event.listens_for(engine, "connect")
    def _foreign_keys_on(conn, _record):
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    yield models, engine, parents
    engine.dispose()


@pytest.fixture
def session(tables):
    with Session(tables[1]) as s:
        yield s


def _schedule(models, application_id=1, version_id=None, **overrides):
    values = dict(
        entity_type="agent", application_id=application_id, version_id=version_id, name="nightly",
        cron="0 3 * * *", timezone="UTC", created_by=7, principal=PRINCIPAL,
    )
    return models.EntitySchedule(**{**values, **overrides})


def _add_run(session, models, schedule_id, status):
    session.add(models.EntityScheduleRun(schedule_id=schedule_id, trigger="cron", status=status))
    session.flush()


@pytest.fixture
def parents(tables, session):
    """Application 1 with versions 1 and 2; returns the parent tables."""
    apps, versions = tables[2]["applications"], tables[2]["application_versions"]
    session.execute(apps.insert().values(id=1))
    session.execute(versions.insert(), [{"id": 1}, {"id": 2}])
    session.commit()
    return tables[2]


# TC-US01-10 / TC-US01-11 / R2.2: several schedules of one version and of several owners are kept
def test_7011_r2_2_many_schedules_on_one_version(tables, session, parents):
    models = tables[0]
    session.add_all([
        _schedule(models, version_id=1, name="a", created_by=7),
        _schedule(models, version_id=1, name="a", created_by=7),
        _schedule(models, version_id=1, name="b", created_by=8),
    ])
    session.commit()
    rows = session.scalars(select(models.EntitySchedule).where(models.EntitySchedule.version_id == 1)).all()
    assert sorted(r.created_by for r in rows) == [7, 7, 8]


# TC-US01-12 / R2.2: a schedule row needs an owner and a principal
@pytest.mark.parametrize("missing", ["created_by", "principal"])
def test_7011_r2_2_schedule_requires_owner_and_principal(tables, session, parents, missing):
    session.add(_schedule(tables[0], **{missing: null()}))  # JSON None would store 'null', not NULL
    with pytest.raises(IntegrityError):
        session.flush()


# TC-US01-06 / TC-US01-07 / R3.4: run_mode accepts entry_point, prompt and NULL only
@pytest.mark.parametrize("run_mode", ["entry_point", "prompt", None])
def test_7011_r3_4_run_mode_accepts_known_values(tables, session, parents, run_mode):
    session.add(_schedule(tables[0], run_mode=run_mode))
    session.flush()


@pytest.mark.parametrize("run_mode", ["cron", ""])
def test_7011_r3_4_run_mode_rejects_other_values(tables, session, parents, run_mode):
    session.add(_schedule(tables[0], run_mode=run_mode))
    with pytest.raises(IntegrityError, match="ck_entity_schedules_run_mode"):
        session.flush()


# TC-US01-08 / TC-US01-21 / R2.4: deleting an application deletes its schedules and their runs
def test_7011_r2_4_application_delete_cascades(tables, session, parents):
    models = tables[0]
    schedule = _schedule(models, version_id=1)
    session.add(schedule)
    session.flush()
    _add_run(session, models, schedule.id, "succeeded")
    session.execute(parents["applications"].delete())
    session.expire_all()
    assert session.scalars(select(models.EntitySchedule)).all() == []
    assert session.scalars(select(models.EntityScheduleRun)).all() == []


# TC-US01-09 / TC-US01-22 / R2.5: deleting a version nulls version_id and keeps the schedule
def test_7011_r2_5_version_delete_nulls_version_id(tables, session, parents):
    models = tables[0]
    session.add_all([_schedule(models, version_id=1), _schedule(models, version_id=2)])
    session.commit()
    versions = parents["application_versions"]
    session.execute(versions.delete().where(versions.c.id == 1))
    session.expire_all()
    assert sorted(s.version_id is None for s in session.scalars(select(models.EntitySchedule))) == [False, True]


# TC-US01-16 / TC-US01-19 / R3.3: a final run lets the next active run in, other schedules are free
@pytest.mark.parametrize("final", ["succeeded", "failed", "cancelled"])
def test_7011_r3_3_new_active_run_after_final_run(tables, session, parents, final):
    models = tables[0]
    first, other = _schedule(models), _schedule(models, name="other")
    session.add_all([first, other])
    session.flush()
    _add_run(session, models, first.id, final)
    _add_run(session, models, first.id, "running")
    _add_run(session, models, other.id, "running")


# TC-US01-17 / TC-US01-18 / R3.2: a second active run of one schedule is rejected
@pytest.mark.parametrize("first,second", [
    ("running", "running"), ("running", "waiting_for_approval"), ("waiting_for_approval", "running"),
])
def test_7011_r3_2_second_active_run_rejected(tables, session, parents, first, second):
    models = tables[0]
    schedule = _schedule(models)
    session.add(schedule)
    session.flush()
    _add_run(session, models, schedule.id, first)
    with pytest.raises(IntegrityError, match="entity_schedule_runs.schedule_id"):
        _add_run(session, models, schedule.id, second)
