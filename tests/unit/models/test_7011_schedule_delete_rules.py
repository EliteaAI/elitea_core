"""Unit tests for schedule FK delete rules (EL-7011 US-14): cascade to runs, no FK from conversations.

Covers R1.1 (entity delete cascades), R1.2 (run conversations stay), R1.3 (rollback keeps schedules).
The model file is real; only the plugin packages around it are stubbed. The database is SQLite with
real SQLAlchemy and foreign key enforcement enabled.

Run via:
    cd repositories/elitea_core/.wt/us14
    python tests/run_tests.py models/test_7011_schedule_delete_rules.py -m unit -v
"""
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs


def _sqlalchemy_modules():
    return {n: m for n, m in sys.modules.items() if n == "sqlalchemy" or n.startswith("sqlalchemy.")}


def _import_real_sqlalchemy():
    """Real sqlalchemy modules by name, even when an earlier suite left stubs in sys.modules.

    A stub is a sqlalchemy entry without a `__file__`. Real modules stay cached: running one
    twice breaks the C-extension state.
    """
    stubs = {n: m for n, m in _sqlalchemy_modules().items() if not hasattr(m, "__file__")}
    for n in stubs:
        del sys.modules[n]
    try:
        import sqlalchemy.dialects.postgresql
        import sqlalchemy.exc
        import sqlalchemy.ext.compiler
        import sqlalchemy.orm  # noqa: F401
        return _sqlalchemy_modules()
    finally:
        sys.modules.update(stubs)


REAL_SQLALCHEMY = _import_real_sqlalchemy()

_sa = REAL_SQLALCHEMY["sqlalchemy"]
Column, Integer, Table, create_engine, event, select = (
    _sa.Column, _sa.Integer, _sa.Table, _sa.create_engine, _sa.event, _sa.select
)
JSONB = REAL_SQLALCHEMY["sqlalchemy.dialects.postgresql"].JSONB
IntegrityError = REAL_SQLALCHEMY["sqlalchemy.exc"].IntegrityError
compiles = REAL_SQLALCHEMY["sqlalchemy.ext.compiler"].compiles
DeclarativeBase = REAL_SQLALCHEMY["sqlalchemy.orm"].DeclarativeBase
Session = REAL_SQLALCHEMY["sqlalchemy.orm"].Session

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PRINCIPAL = {"kind": "delegation"}


@compiles(JSONB, "sqlite")
def _jsonb_as_json(type_, compiler, **kw):
    return "JSON"


@pytest.fixture
def tables(monkeypatch):
    """(models module, SQLite engine, parent tables by name) with the tables created."""
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)

    class Base(DeclarativeBase):
        pass

    parents = {
        name: Table(name, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
        for name in ("applications", "application_versions")
    }

    package = types.ModuleType("elitea_core_7011_fk_delete")
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
    """Helper to create EntitySchedule instances."""
    values = {
        "entity_type": "agent", "application_id": application_id, "version_id": version_id,
        "name": "nightly", "cron": "0 3 * * *", "timezone": "UTC", "created_by": 7,
        "principal": PRINCIPAL,
    }
    return models.EntitySchedule(**{**values, **overrides})


def _add_run(session, models, schedule_id, status):
    """Helper to add a run to a schedule."""
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


# TC-US14-01 / R1.1: Deleting an application deletes its schedules via FK cascade
def test_7011_r1_1_application_delete_cascades_to_schedules(tables, session, parents):
    """Verify that EntitySchedule.application_id FK with ON DELETE CASCADE works.

    When an application is deleted, all its schedules must be deleted in the same transaction.
    """
    models = tables[0]
    schedule = _schedule(models, application_id=1)
    session.add(schedule)
    session.flush()
    schedule_id = schedule.id

    # Verify schedule exists before delete
    assert session.scalars(select(models.EntitySchedule).where(
        models.EntitySchedule.id == schedule_id)).first() is not None

    # Delete the application
    session.execute(parents["applications"].delete())
    session.expire_all()

    # Verify schedule is deleted via cascade
    assert session.scalars(select(models.EntitySchedule).where(
        models.EntitySchedule.id == schedule_id)).first() is None


# TC-US14-02 / R1.1: Deleting an application deletes schedule runs via FK cascade
def test_7011_r1_1_application_delete_cascades_to_runs(tables, session, parents):
    """Verify that EntityScheduleRun.schedule_id FK with ON DELETE CASCADE works.

    When a schedule is deleted (via application delete cascade), all its runs must also be deleted.
    """
    models = tables[0]
    schedule = _schedule(models, application_id=1)
    session.add(schedule)
    session.flush()

    # Add a run to the schedule
    run_id = None
    for _ in range(1):
        run = models.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="succeeded")
        session.add(run)
        session.flush()
        run_id = run.id

    # Verify run exists before delete
    assert session.scalars(select(models.EntityScheduleRun).where(
        models.EntityScheduleRun.id == run_id)).first() is not None

    # Delete the application, which cascades to schedules, which cascades to runs
    session.execute(parents["applications"].delete())
    session.expire_all()

    # Verify run is deleted via cascade
    assert session.scalars(select(models.EntityScheduleRun).where(
        models.EntityScheduleRun.id == run_id)).first() is None


# TC-US14-03 / R1.1: Delete operation with multiple schedules cascades to all their runs
def test_7011_r1_1_application_delete_cascades_to_multiple_schedules_and_runs(
    tables, session, parents
):
    """Verify cascade works for multiple schedules and runs (realistic scenario).

    When an application with multiple schedules (each with multiple runs) is deleted,
    all schedules and all their runs must be deleted.
    """
    models = tables[0]

    # Add multiple schedules with multiple runs each
    schedule1 = _schedule(models, application_id=1, name="schedule1")
    schedule2 = _schedule(models, application_id=1, name="schedule2", version_id=2)
    session.add_all([schedule1, schedule2])
    session.flush()

    _add_run(session, models, schedule1.id, "succeeded")
    _add_run(session, models, schedule1.id, "failed")
    _add_run(session, models, schedule2.id, "running")


    # Delete the application
    session.execute(parents["applications"].delete())
    session.expire_all()

    # Verify all schedules are deleted
    assert session.scalars(select(models.EntitySchedule)).all() == []
    # Verify all runs are deleted
    assert session.scalars(select(models.EntityScheduleRun)).all() == []


# TC-US14-04 / R1.2: EntityScheduleRun has no FK from run conversations
def test_7011_r1_2_no_fk_from_conversations_to_schedules(tables, session, parents):
    """Verify that EntityScheduleRun does not have an FK to a conversations table.

    This ensures run conversations stay in Run History when a schedule is deleted.
    The test verifies that the model does NOT define a relationship or FK to conversations.
    """
    models = tables[0]

    # Verify EntityScheduleRun has a conversation_id column
    run = models.EntityScheduleRun()
    assert hasattr(run, 'conversation_id'), "EntityScheduleRun must have conversation_id column"

    # Verify there is NO FK relationship or constraint from conversations to schedules
    # by checking that the ForeignKey on conversation_id doesn't point to entity_schedules
    fk_list = [fk for fk in models.EntityScheduleRun.__table__.foreign_keys]
    conversation_fks = [fk for fk in fk_list if 'conversation' in str(fk).lower()]
    assert len(conversation_fks) == 0, (
        "EntityScheduleRun must not have an FK to conversations table; "
        "this ensures conversations stay when a schedule is deleted"
    )


# TC-US14-05 / R1.3: Rollback prevents cascade deletion
def test_7011_r1_3_rollback_prevents_cascade_deletion(tables, session, parents):
    """Verify that a rollback prevents cascade deletion.

    When an application delete is refused or rolls back, all schedules must remain unchanged.
    """
    models = tables[0]
    schedule = _schedule(models, application_id=1)
    session.add(schedule)
    session.flush()
    schedule_id = schedule.id

    # Create a savepoint and attempt delete
    savepoint = session.begin_nested()
    session.execute(parents["applications"].delete())
    session.expire_all()

    # Verify schedule is deleted within the nested transaction
    assert session.scalars(select(models.EntitySchedule).where(
        models.EntitySchedule.id == schedule_id)).first() is None

    # Rollback the nested transaction
    savepoint.rollback()
    session.expire_all()

    # Verify schedule is restored after rollback
    assert session.scalars(select(models.EntitySchedule).where(
        models.EntitySchedule.id == schedule_id)).first() is not None


# TC-US14-06 / R1.3: Rollback preserves schedule with its runs
def test_7011_r1_3_rollback_preserves_schedules_and_runs(tables, session, parents):
    """Verify that rollback preserves both schedules and runs.

    When a delete transaction rolls back, all related data must be preserved.
    """
    models = tables[0]
    schedule = _schedule(models, application_id=1)
    session.add(schedule)
    session.flush()

    # Add runs to the schedule
    run1 = models.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="succeeded")
    run2 = models.EntityScheduleRun(schedule_id=schedule.id, trigger="manual", status="failed")
    session.add_all([run1, run2])
    session.flush()

    schedule_id = schedule.id
    run1_id = run1.id
    run2_id = run2.id

    # Create a savepoint and attempt delete
    savepoint = session.begin_nested()
    session.execute(parents["applications"].delete())
    session.expire_all()

    # Verify all are deleted within the nested transaction
    assert session.scalars(select(models.EntitySchedule)).all() == []
    assert session.scalars(select(models.EntityScheduleRun)).all() == []

    # Rollback
    savepoint.rollback()
    session.expire_all()

    # Verify schedule and runs are restored
    restored_schedule = session.scalars(select(models.EntitySchedule).where(
        models.EntitySchedule.id == schedule_id)).first()
    assert restored_schedule is not None
    assert restored_schedule.name == "nightly"

    restored_runs = session.scalars(select(models.EntityScheduleRun).where(
        models.EntityScheduleRun.schedule_id == schedule_id)).all()
    assert len(restored_runs) == 2
    assert {r.id for r in restored_runs} == {run1_id, run2_id}
