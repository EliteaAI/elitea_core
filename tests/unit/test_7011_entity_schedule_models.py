"""Unit tests for the schedule models (EL-7011 US-01).

Tests the ORM models for EntitySchedule, EntityScheduleRun, and EntitySchedulePreference.
Covers R1.1, R1.2 (SQL block), R1.3 (models created from code), R2.1, R2.3, R2.6, R3.1, R3.4.

Run via:
    python tests/run_tests.py unit/test_7011_entity_schedule_models.py -v
"""
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent


def _pkg(name):
    """Create a minimal module/package stub."""
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


@pytest.fixture(scope="function")
def entity_schedule_models(isolated_sys_modules):
    """Load entity_schedule.py on real SQLAlchemy with a fresh declarative base."""
    from sqlalchemy import Column, Integer, Table
    from sqlalchemy.orm import DeclarativeBase

    class _Base(DeclarativeBase):
        pass

    # Parent tables the foreign keys point at, so the DDL compiles.
    for parent in ("applications", "application_versions"):
        Table(parent, _Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")

    # Stub tools package
    tools = _pkg("tools")

    class _FakeDB:
        Base = _Base

    class _FakeDBTools:
        class AbstractBaseMixin:
            pass

    class _FakeConfig:
        POSTGRES_TENANT_SCHEMA = "tenant"

    tools.db = _FakeDB()
    tools.db_tools = _FakeDBTools()
    tools.config = _FakeConfig()
    sys.modules["tools"] = tools
    sys.modules["tools.db"] = tools.db
    sys.modules["tools.db_tools"] = tools.db_tools
    sys.modules["tools.config"] = tools.config

    # Set up package structure for relative imports
    elitea_core_7011_pkg = _pkg("elitea_core_7011")
    models_pkg = _pkg("elitea_core_7011.models")

    # Add the stubs to the models package so relative imports work
    models_pkg.db = tools.db
    models_pkg.db_tools = tools.db_tools
    models_pkg.config = tools.config

    sys.modules["elitea_core_7011"] = elitea_core_7011_pkg
    sys.modules["elitea_core_7011.models"] = models_pkg

    # Now import the actual module
    from fixtures.helpers import load_module_with_stubs
    return load_module_with_stubs(
        PLUGIN_ROOT / "models" / "entity_schedule.py",
        "elitea_core_7011.models.entity_schedule",
    )


def _ddl(model):
    """PostgreSQL CREATE TABLE text of a model, whitespace collapsed."""
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    return " ".join(str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())).split())


def _index(model, name):
    return next(ix for ix in model.__table__.indexes if ix.name == name)


# R2.1: created_by and principal are NOT NULL, in the tenant schema
def test_7011_r2_1_schedule_owner_columns_not_null(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "CREATE TABLE tenant.entity_schedules" in ddl
    assert "created_by INTEGER NOT NULL" in ddl
    assert "principal JSONB NOT NULL" in ddl


# R2.6: description VARCHAR(2304) and run_mode nullable
def test_7011_r2_6_description_and_run_mode_nullable(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "description VARCHAR(2304)," in ddl
    assert "run_mode VARCHAR(16)," in ddl


# Owner CR 2026-10-21 (risk review): who confirmed "Run without asking", when, for which tools
def test_el7011_cr_schedule_has_nullable_confirmation_columns(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "approval_confirmed_by INTEGER," in ddl
    assert "approval_confirmed_at TIMESTAMP WITH TIME ZONE," in ddl
    assert "approval_fingerprint TEXT," in ddl


def test_el7011_cr_block_adds_the_confirmation_columns(sql_block):
    for column in ("approval_confirmed_by INTEGER NULL", "approval_confirmed_at TIMESTAMPTZ NULL",
                   "approval_fingerprint TEXT NULL"):
        assert f"ALTER TABLE {{pid}}.entity_schedules ADD COLUMN IF NOT EXISTS {column}" in sql_block
        assert column in next(s for s in sql_block if s.startswith("CREATE TABLE IF NOT EXISTS {pid}.entity_schedules "))


# R2.3: no tag column in any of the three tables
def test_7011_r2_3_no_tag_columns(entity_schedule_models):
    m = entity_schedule_models
    for model in (m.EntitySchedule, m.EntityScheduleRun, m.EntitySchedulePreference):
        assert not [col for col in model.__table__.columns if "tag" in col.name]


# R1.3: the same timestamp type as the R-2.0.8 SQL block (TIMESTAMPTZ)
def test_7011_r1_3_timestamps_are_timezone_aware(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "next_run TIMESTAMP WITH TIME ZONE," in ddl
    assert "created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL" in ddl


# R3.1: named principal kind check
def test_7011_r3_1_principal_kind_check(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert (
        "CONSTRAINT ck_entity_schedules_principal_kind "
        "CHECK (principal->>'kind' IS NOT NULL AND principal->>'kind' IN ('mandate', 'delegation', 'legacy_system'))"
    ) in ddl


# R3.4: named run mode check
def test_7011_r3_4_run_mode_check(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert (
        "CONSTRAINT ck_entity_schedules_run_mode "
        "CHECK (run_mode IS NULL OR run_mode IN ('entry_point', 'prompt'))"
    ) in ddl


# R3.2: one active run per schedule, partial unique index
def test_7011_r3_2_active_run_unique_index(entity_schedule_models):
    index = _index(entity_schedule_models.EntityScheduleRun, "uq_entity_schedule_runs_active")
    assert index.unique
    assert [col.name for col in index.columns] == ["schedule_id"]
    where = "status IN ('running', 'waiting_for_approval')"
    assert str(index.dialect_options["postgresql"]["where"]) == where
    assert str(index.dialect_options["sqlite"]["where"]) == where


# R1.3: run rows cascade with their schedule
def test_7011_r1_3_run_schedule_fk_cascades(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntityScheduleRun)
    assert "FOREIGN KEY(schedule_id) REFERENCES tenant.entity_schedules (id) ON DELETE CASCADE" in ddl


SQL_TABLES = ("entity_schedules", "entity_schedule_runs", "entity_schedule_preferences")
SQL_CHECKS = (
    "ck_entity_schedules_entity_type",
    "ck_entity_schedules_run_mode",
    "ck_entity_schedules_principal_kind",
)


@pytest.fixture(scope="module")
def sql_block():
    """Statements of the R-2.0.8 section of db_migrations.txt, whitespace collapsed."""
    text = (PLUGIN_ROOT / "db_migrations.txt").read_text()
    section = text.split("# R-2.0.8", 1)[1].split("\n# R-2.0.7", 1)[0]
    body = "\n".join(line for line in section.splitlines()[1:] if not line.startswith("#"))
    return [" ".join(stmt.split()) for stmt in body.split(";") if stmt.strip()]


# R1.1: the block creates the three tables
def test_7011_r1_1_block_creates_three_tables(sql_block):
    created = [
        m.group(1)
        for stmt in sql_block
        if (m := re.match(r"CREATE TABLE IF NOT EXISTS \{pid\}\.(\w+) ", stmt))
    ]
    assert created == list(SQL_TABLES)


# R1.1: the block carries the named checks and the active-run index
def test_7011_r1_1_block_has_checks_and_active_run_index(sql_block):
    tables = "\n".join(s for s in sql_block if s.startswith("CREATE TABLE"))
    for name in SQL_CHECKS:
        assert f"CONSTRAINT {name} CHECK" in tables
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_schedule_runs_active "
        "ON {pid}.entity_schedule_runs (schedule_id) "
        "WHERE status IN ('running', 'waiting_for_approval')"
    ) in sql_block


# R1.1: the block has the three named indexes of entity_schedules
def test_7011_r1_1_block_has_schedule_indexes(sql_block):
    names = {
        m.group(1)
        for stmt in sql_block
        if (m := re.match(r"CREATE INDEX IF NOT EXISTS (\w+) ON \{pid\}\.entity_schedules ", stmt))
    }
    assert names == {
        "ix_entity_schedules_next_run_enabled",
        "ix_entity_schedules_created_by",
        "ix_entity_schedules_application_version",
    }


# R1.2: every statement of the block is idempotent
def test_7011_r1_2_every_statement_is_if_not_exists(sql_block):
    # 3 tables, 7 indexes (the runs list and the conversation filter among them), 6 column statements
    assert len(sql_block) == 16
    for stmt in sql_block:
        assert re.match(
            r"CREATE (UNIQUE )?(TABLE|INDEX) IF NOT EXISTS "
            r"|ALTER TABLE \{pid\}\.entity_schedule(s|_runs) ADD COLUMN IF NOT EXISTS ",
            stmt,
        ), stmt


# Owner CR 2026-10-21 (test panel log): a fresh table and an older one get the same run log columns as the model
RUN_LOG_COLUMNS = {"events": "JSONB", "version_id": "INTEGER", "principal_label": "VARCHAR(256)",
                   "risk_outcome": "VARCHAR(32)"}


def test_cr_el7011_the_run_log_columns_are_in_the_table_the_alter_and_the_model(sql_block, entity_schedule_models):
    create = next(s for s in sql_block if s.startswith("CREATE TABLE IF NOT EXISTS {pid}.entity_schedule_runs "))
    alter = next(s for s in sql_block if s.startswith("ALTER TABLE {pid}.entity_schedule_runs "))
    columns = entity_schedule_models.EntityScheduleRun.__table__.c
    for name, sql_type in RUN_LOG_COLUMNS.items():
        assert f"{name} {sql_type} NULL" in create
        assert f"ADD COLUMN IF NOT EXISTS {name} {sql_type} NULL" in alter
        assert columns[name].nullable is True
    assert not columns["version_id"].foreign_keys
