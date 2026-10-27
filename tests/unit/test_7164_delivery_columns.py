"""Unit tests for the delivery columns of the schedule table.

Covers R1.1 (a schedule saved without delivery stores mode run_history and no target chat)
and R1.2 (a schedule that existed before the columns reads as run_history with null targets).

Run via:
    python tests/run_tests.py unit/test_7164_delivery_columns.py -v
"""
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent


def _pkg(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


@pytest.fixture
def schedule_model(isolated_sys_modules):
    """EntitySchedule loaded on real SQLAlchemy with a fresh declarative base."""
    from sqlalchemy import Column, Integer, Table
    from sqlalchemy.orm import DeclarativeBase

    class _Base(DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):
        Table(parent, _Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")

    tools = _pkg("tools")
    tools.db = types.SimpleNamespace(Base=_Base)
    tools.db_tools = types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {}))
    tools.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant")
    models_pkg = _pkg("elitea_core_delivery.models")
    models_pkg.db, models_pkg.db_tools, models_pkg.config = tools.db, tools.db_tools, tools.config
    sys.modules.update({
        "tools": tools,
        "elitea_core_delivery": _pkg("elitea_core_delivery"),
        "elitea_core_delivery.models": models_pkg,
    })

    from fixtures.helpers import load_module_with_stubs
    module = load_module_with_stubs(
        PLUGIN_ROOT / "models" / "entity_schedule.py", "elitea_core_delivery.models.entity_schedule"
    )
    return module.EntitySchedule


@pytest.fixture(scope="module")
def delivery_block():
    """Statements of the newest section of db_migrations.txt, whitespace collapsed."""
    text = (PLUGIN_ROOT / "db_migrations.txt").read_text()
    section = text.split("# R-2.0.9", 1)[1].split("\n# R-2.0.8", 1)[0]
    body = "\n".join(line for line in section.splitlines()[1:] if not line.startswith("#"))
    return [" ".join(stmt.split()) for stmt in body.split(";") if stmt.strip()]


def _ddl(model):
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    return " ".join(str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())).split())


def test_schedule_without_delivery_defaults_to_run_history_mode(schedule_model):
    column = schedule_model.__table__.c.delivery_mode
    assert column.default.arg == "run_history"
    assert str(column.server_default.arg) == "run_history"
    assert column.nullable is False
    assert column.type.length == 16


def test_schedule_without_delivery_has_no_target_chat(schedule_model):
    columns = schedule_model.__table__.c
    for name in ("delivery_conversation_uuid", "delivery_participant_id"):
        assert columns[name].nullable is True
        assert columns[name].default is None
        assert columns[name].server_default is None
    ddl = _ddl(schedule_model)
    assert "delivery_mode VARCHAR(16) DEFAULT 'run_history' NOT NULL" in ddl
    assert "delivery_conversation_uuid UUID," in ddl
    assert "delivery_participant_id INTEGER," in ddl


def test_migration_adds_the_delivery_columns_to_an_existing_schedule_table(delivery_block):
    assert delivery_block == [
        (
            "ALTER TABLE {pid}.entity_schedules "
            "ADD COLUMN IF NOT EXISTS delivery_mode VARCHAR(16) NOT NULL DEFAULT 'run_history'"
        ),
        "ALTER TABLE {pid}.entity_schedules ADD COLUMN IF NOT EXISTS delivery_conversation_uuid UUID NULL",
        "ALTER TABLE {pid}.entity_schedules ADD COLUMN IF NOT EXISTS delivery_participant_id INTEGER NULL",
    ]


def test_migration_statements_are_idempotent(delivery_block):
    for stmt in delivery_block:
        assert re.match(r"ALTER TABLE \{pid\}\.entity_schedules ADD COLUMN IF NOT EXISTS ", stmt)
