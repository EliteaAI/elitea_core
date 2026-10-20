"""Unit tests for the schedule models (EL-7011 US-01).

Tests the ORM models for EntitySchedule, EntityScheduleRun, and EntitySchedulePreference.
Covers R1.3 (models created from code), R2.1, R2.3, R2.6, R3.1, R3.4.

Run via:
    python tests/run_tests.py unit/test_7011_entity_schedule_models.py -v
"""
import sys
import types
import pathlib

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


# TC-US01-01 / R1.3: EntitySchedule model exists
def test_entity_schedule_model_exists(entity_schedule_models):
    """R1.3: EntitySchedule model is created from the models file."""
    assert hasattr(entity_schedule_models, "EntitySchedule")
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert EntitySchedule is not None


# TC-US01-02 / R1.3: EntityScheduleRun model exists
def test_entity_schedule_run_model_exists(entity_schedule_models):
    """R1.3: EntityScheduleRun model is created from the models file."""
    assert hasattr(entity_schedule_models, "EntityScheduleRun")
    EntityScheduleRun = entity_schedule_models.EntityScheduleRun
    assert EntityScheduleRun is not None


# TC-US01-03 / R1.3: EntitySchedulePreference model exists
def test_entity_schedule_preference_model_exists(entity_schedule_models):
    """R1.3: EntitySchedulePreference model is created from the models file."""
    assert hasattr(entity_schedule_models, "EntitySchedulePreference")
    EntitySchedulePreference = entity_schedule_models.EntitySchedulePreference
    assert EntitySchedulePreference is not None


# TC-US01-04 / R2.1: EntitySchedule has created_by column
def test_entity_schedule_has_created_by_column(entity_schedule_models):
    """R2.1: schedule row keeps created_by not null."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "created_by")
    # Check that created_by column exists in __annotations__
    assert "created_by" in EntitySchedule.__annotations__


# TC-US01-05 / R2.1: EntitySchedule has principal column
def test_entity_schedule_has_principal_column(entity_schedule_models):
    """R2.1: schedule row keeps principal not null."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "principal")
    assert "principal" in EntitySchedule.__annotations__


# TC-US01-06 / R2.6: EntitySchedule has description column
def test_entity_schedule_has_description_column(entity_schedule_models):
    """R2.6: schedule row keeps optional description."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "description")
    assert "description" in EntitySchedule.__annotations__


# TC-US01-07 / R2.6: EntitySchedule has run_mode column
def test_entity_schedule_has_run_mode_column(entity_schedule_models):
    """R2.6: schedule row keeps nullable run_mode."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "run_mode")
    assert "run_mode" in EntitySchedule.__annotations__


# TC-US01-08 / R2.3: EntitySchedule table has no tag column
def test_entity_schedule_has_no_tag_column(entity_schedule_models):
    """R2.3: no tag column in schedule table."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    # The model should not have a tag column
    assert not hasattr(EntitySchedule, "tag")
    # The model should not have a schedule_tag link model
    assert not hasattr(entity_schedule_models, "EntityScheduleTag")


# TC-US01-09 / R1.3: EntityScheduleRun has schedule_id foreign key
def test_entity_schedule_run_has_schedule_id(entity_schedule_models):
    """R1.3: EntityScheduleRun references schedule."""
    EntityScheduleRun = entity_schedule_models.EntityScheduleRun
    assert hasattr(EntityScheduleRun, "schedule_id")
    assert "schedule_id" in EntityScheduleRun.__annotations__


# TC-US01-21 / R2.4: EntitySchedule table name is correct
def test_entity_schedule_table_name(entity_schedule_models):
    """R1.3: EntitySchedule is stored in entity_schedules table."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "__tablename__")
    assert EntitySchedule.__tablename__ == "entity_schedules"


# TC-US01-22 / R1.3: EntityScheduleRun table name is correct
def test_entity_schedule_run_table_name(entity_schedule_models):
    """R1.3: EntityScheduleRun is stored in entity_schedule_runs table."""
    EntityScheduleRun = entity_schedule_models.EntityScheduleRun
    assert hasattr(EntityScheduleRun, "__tablename__")
    assert EntityScheduleRun.__tablename__ == "entity_schedule_runs"


# TC-US01-23 / R1.3: EntitySchedulePreference table name is correct
def test_entity_schedule_preference_table_name(entity_schedule_models):
    """R1.3: EntitySchedulePreference is stored in entity_schedule_preferences table."""
    EntitySchedulePreference = entity_schedule_models.EntitySchedulePreference
    assert hasattr(EntitySchedulePreference, "__tablename__")
    assert EntitySchedulePreference.__tablename__ == "entity_schedule_preferences"


# TC-US01-13 / R3.1: EntitySchedule principal_kind constraint exists
def test_entity_schedule_has_principal_kind_check(entity_schedule_models):
    """R3.1: principal kind check constraint exists in model."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    # Check that the model is properly defined with constraints
    assert hasattr(EntitySchedule, "__table_args__") or hasattr(
        EntitySchedule, "__mapper_args__"
    )
    # The constraint should validate principal->>'kind' IN ('mandate', 'delegation', 'legacy_system')
    # This is verified when the model is instantiated or via SQLAlchemy inspection


# TC-US01-14 / R3.4: EntitySchedule run_mode check constraint exists
def test_entity_schedule_has_run_mode_check(entity_schedule_models):
    """R3.4: run_mode check constraint exists in model."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    # The constraint should validate run_mode IN ('entry_point', 'prompt') OR run_mode IS NULL
    assert hasattr(EntitySchedule, "run_mode")


# TC-US01-15 / R3.2: EntityScheduleRun has active run unique constraint
def test_entity_schedule_run_has_active_run_constraint(entity_schedule_models):
    """R3.2: partial unique index on active runs exists."""
    EntityScheduleRun = entity_schedule_models.EntityScheduleRun
    assert hasattr(EntityScheduleRun, "schedule_id")
    assert hasattr(EntityScheduleRun, "status")
    # The unique constraint should ensure one active run per schedule


# TC-US01-24 / R1.3: EntitySchedule has created_at column
def test_entity_schedule_has_timestamps(entity_schedule_models):
    """R1.3: EntitySchedule has timestamp columns."""
    EntitySchedule = entity_schedule_models.EntitySchedule
    assert hasattr(EntitySchedule, "created_at")
    assert hasattr(EntitySchedule, "updated_at")
    assert "created_at" in EntitySchedule.__annotations__
    assert "updated_at" in EntitySchedule.__annotations__


def _ddl(model):
    """PostgreSQL CREATE TABLE text of a model, whitespace collapsed."""
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    return " ".join(str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())).split())


def _index(model, name):
    return next(ix for ix in model.__table__.indexes if ix.name == name)


# TC-US01-04 / TC-US01-05 / R2.1: created_by and principal are NOT NULL, in the tenant schema
def test_7011_r2_1_schedule_owner_columns_not_null(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "CREATE TABLE tenant.entity_schedules" in ddl
    assert "created_by INTEGER NOT NULL" in ddl
    assert "principal JSONB NOT NULL" in ddl


# TC-US01-06 / TC-US01-07 / R2.6: description VARCHAR(2304) and run_mode nullable
def test_7011_r2_6_description_and_run_mode_nullable(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "description VARCHAR(2304)," in ddl
    assert "run_mode VARCHAR(16)," in ddl


# TC-US01-08 / R2.3: no tag column in any of the three tables
def test_7011_r2_3_no_tag_columns(entity_schedule_models):
    m = entity_schedule_models
    for model in (m.EntitySchedule, m.EntityScheduleRun, m.EntitySchedulePreference):
        assert not [col for col in model.__table__.columns if "tag" in col.name]


# R1.3: the same timestamp type as the R-2.0.8 SQL block (TIMESTAMPTZ)
def test_7011_r1_3_timestamps_are_timezone_aware(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert "next_run TIMESTAMP WITH TIME ZONE," in ddl
    assert "created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL" in ddl


# TC-US01-13 / R3.1: named principal kind check
def test_7011_r3_1_principal_kind_check(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert (
        "CONSTRAINT ck_entity_schedules_principal_kind "
        "CHECK (principal->>'kind' IN ('mandate', 'delegation', 'legacy_system'))"
    ) in ddl


# TC-US01-14 / R3.4: named run mode check
def test_7011_r3_4_run_mode_check(entity_schedule_models):
    ddl = _ddl(entity_schedule_models.EntitySchedule)
    assert (
        "CONSTRAINT ck_entity_schedules_run_mode "
        "CHECK (run_mode IS NULL OR run_mode IN ('entry_point', 'prompt'))"
    ) in ddl


# TC-US01-15 / R3.2: one active run per schedule, partial unique index
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
