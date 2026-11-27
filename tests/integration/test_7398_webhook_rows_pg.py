"""Webhook rows of entity_schedules on a real PostgreSQL schema (#7398).

Covers: the schema admin task on a schema with the R-2.0.8 shape and existing schedules (run twice),
the due query never picking a webhook row, and the active-run index (several active webhook runs,
still one active cron run). Set ELITEA_7011_PG_DSN as for test_7011_tick_claim.py:

    ELITEA_7011_PG_DSN=postgresql://centry:changeme@localhost:5432/db python3 run_tests.py integration/test_7398_webhook_rows_pg.py

Each test creates its own schema `p_<big id>` and drops it; it touches nothing else.
"""
import contextlib
import importlib.util
import os
import pathlib
import sys
import types
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY

DSN = os.environ.get("ELITEA_7011_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="ELITEA_7011_PG_DSN is not set")
PROJECT_ID = 900000 + os.getpid() % 90000  # a p_N name the task's regex accepts, far from real projects
SCHEMA = f"p_{PROJECT_ID}"
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = "plugins.elitea_core"

# The R-2.0.8 shape of the two tables, as db_migrations.txt created it before #7398
OLD_DDL = f"""
CREATE TABLE {SCHEMA}.entity_schedules (
    id SERIAL PRIMARY KEY, uuid UUID NOT NULL DEFAULT gen_random_uuid(), entity_type VARCHAR(16) NOT NULL,
    application_id INTEGER NOT NULL, version_id INTEGER, name VARCHAR(256) NOT NULL, description VARCHAR(2304),
    cron VARCHAR(128) NOT NULL, timezone VARCHAR(64) NOT NULL, run_mode VARCHAR(16), prompt TEXT,
    variables JSONB NOT NULL DEFAULT '[]'::jsonb, enabled BOOLEAN NOT NULL DEFAULT true,
    disabled_reason VARCHAR(32), approval_rule VARCHAR(32) NOT NULL DEFAULT 'pause_and_ask',
    created_by INTEGER NOT NULL, updated_by INTEGER, principal JSONB NOT NULL,
    last_run TIMESTAMPTZ, next_run TIMESTAMPTZ, last_run_status VARCHAR(32), last_conversation_id INTEGER,
    expires_at TIMESTAMPTZ, approval_confirmed_by INTEGER, approval_confirmed_at TIMESTAMPTZ,
    approval_fingerprint TEXT, original_trigger JSONB, refusal_streak SMALLINT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE {SCHEMA}.entity_schedule_runs (
    id SERIAL PRIMARY KEY, schedule_id INTEGER NOT NULL REFERENCES {SCHEMA}.entity_schedules(id) ON DELETE CASCADE,
    trigger VARCHAR(16) NOT NULL, status VARCHAR(32) NOT NULL, reason TEXT, conversation_id INTEGER,
    requested_by INTEGER, approval_expires_at TIMESTAMPTZ, started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resumed_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, events JSONB, version_id INTEGER,
    principal_label VARCHAR(256), risk_outcome VARCHAR(32)
);
CREATE UNIQUE INDEX uq_entity_schedule_runs_active ON {SCHEMA}.entity_schedule_runs (schedule_id)
    WHERE status IN ('running', 'waiting_for_approval');
INSERT INTO {SCHEMA}.entity_schedules (entity_type, application_id, name, cron, timezone, created_by, principal, next_run)
    VALUES ('agent', 1, 'existing', '0 9 * * *', 'UTC', 3, '{{"kind": "delegation", "ref": 3}}', now() - interval '1 minute');
"""


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


@pytest.fixture
def world(monkeypatch, request):
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    sa, orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]
    engine = sa.create_engine(DSN)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {SCHEMA}"))
        conn.execute(sa.text(OLD_DDL))

    def drop_schema():
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        engine.dispose()

    request.addfinalizer(drop_schema)

    class Base(orm.DeclarativeBase):
        pass

    for parent in ("applications", "application_versions", "elitea_tools"):
        sa.Table(parent, Base.metadata, sa.Column("id", sa.Integer, primary_key=True), schema="tenant")

    @contextlib.contextmanager
    def get_session(_project_id):
        with orm.Session(engine) as session:
            yield session

    quiet = lambda *a, **k: None
    stubs = {
        "pylon.core.tools": _module("pylon.core.tools", log=types.SimpleNamespace(
            info=quiet, warning=quiet, exception=quiet, error=quiet)),
        "tools": _module("tools", db=types.SimpleNamespace(get_session=get_session), this=types.SimpleNamespace(
            module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))),
        PACKAGE: _module(PACKAGE),
        f"{PACKAGE}.utils": _module(f"{PACKAGE}.utils"),
        f"{PACKAGE}.models": _module(
            f"{PACKAGE}.models", db=types.SimpleNamespace(Base=Base),
            db_tools=types.SimpleNamespace(AbstractBaseMixin=type("Mixin", (), {})),
            config=types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant")),
    }
    for name, module in stubs.items():
        monkeypatch.setitem(sys.modules, name, module)

    def load(relative):
        name = f"{PACKAGE}.{relative[:-3].replace('/', '.')}"
        spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    w = types.SimpleNamespace(sa=sa, orm=orm, engine=engine)
    w.schema_task = load("utils/webhook_trigger_schema.py")
    w.models = load("models/entity_schedule.py")
    load("utils/schedule_timing.py")
    w.claim = load("utils/schedule_claim.py")
    w.session = lambda: orm.Session(engine.execution_options(schema_translate_map={"tenant": SCHEMA}))

    def sql(statement, **params):
        with engine.begin() as conn:
            result = conn.execute(sa.text(statement), params)
            return result.fetchall() if result.returns_rows else None

    w.sql = sql
    return w


def _pending(world):
    return PROJECT_ID in world.schema_task.project_ids_pending()


def _migrate(world):
    migrated, failed = world.schema_task.apply_webhook_trigger_columns([PROJECT_ID])
    assert (migrated, failed) == ([PROJECT_ID], [])


# AC11: the task reports the old schema, migrates it, and a second run changes nothing
def test_7398_schema_task_is_idempotent_and_reports_pending(world):
    assert _pending(world)
    assert world.schema_task.apply_webhook_trigger_columns([PROJECT_ID], dry_run=True) == ([PROJECT_ID], [])
    assert _pending(world)  # a dry run writes nothing
    _migrate(world)
    assert not _pending(world)
    _migrate(world)  # again: every statement is guarded
    indexdef = world.sql(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = :s AND indexname = 'uq_entity_schedule_runs_active'",
        s=SCHEMA)
    assert len(indexdef) == 1 and "webhook" in indexdef[0][0]
    nullable = dict(world.sql(
        "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = :s "
        "AND table_name = 'entity_schedules' AND column_name IN ('cron', 'timezone', 'next_run', 'trigger_kind')",
        s=SCHEMA))
    assert nullable == {"cron": "YES", "timezone": "YES", "next_run": "YES", "trigger_kind": "NO"}
    assert world.sql(f"SELECT trigger_kind FROM {SCHEMA}.entity_schedules") == [("cron",)]  # existing row kept
    comment = world.sql(f"SELECT obj_description('{SCHEMA}.entity_schedules'::regclass)")[0][0]
    assert "webhook" in comment


def test_7398_schema_constraints_refuse_bad_rows(world):
    _migrate(world)
    insert = (f"INSERT INTO {SCHEMA}.entity_schedules (entity_type, application_id, name, created_by, principal, "
              "trigger_kind, cron, timezone, next_run) VALUES ('agent', 1, 'x', 3, '{}'::jsonb, :kind, :cron, 'UTC', :next)")
    IntegrityError = REAL_SQLALCHEMY["sqlalchemy.exc"].IntegrityError
    for kind, cron, next_run in (("cron", None, None), ("webhook", None, datetime.now(UTC)), ("timer", "* * * * *", None)):
        with pytest.raises(IntegrityError):
            world.sql(insert, kind=kind, cron=cron, next=next_run)
    world.sql(insert, kind="webhook", cron=None, next=None)


def _add(world, **fields):
    values = {"entity_type": "agent", "application_id": 1, "name": "row", "created_by": 3,
              "principal": {"kind": "delegation", "ref": 3}}
    with world.session() as session:
        row = world.models.EntitySchedule(**{**values, **fields})
        session.add(row)
        session.commit()
        return row.id


# AC10: the tick never selects a webhook row, even an enabled one past its stop date
def test_7398_due_query_skips_webhook_rows(world):
    _migrate(world)
    past = datetime.now(UTC) - timedelta(minutes=5)
    webhook = _add(world, trigger_kind="webhook", webhook_type="github", expires_at=past)
    cron = _add(world, cron="*/5 * * * *", timezone="UTC", next_run=past)
    with world.session() as session:
        due = {row.id for row in world.claim.select_due_schedules(session, ["agent"], 100)}
    assert cron in due and webhook not in due


# The active-run index: two active webhook runs are fine, a second active cron run is still refused
def test_7398_active_run_index_exempts_webhook_runs(world):
    _migrate(world)
    webhook = _add(world, trigger_kind="webhook", webhook_type="custom")
    cron = _add(world, cron="0 9 * * *", timezone="UTC")
    with world.session() as session:
        assert world.claim.insert_run(session, webhook, "webhook") is not None
        assert world.claim.insert_run(session, webhook, "webhook") is not None
        assert world.claim.insert_run(session, cron, "cron") is not None
        assert world.claim.insert_run(session, cron, "cron") is None
        session.commit()
