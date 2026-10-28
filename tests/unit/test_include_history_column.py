"""Unit tests for the chat history column of the schedule table.

A schedule that delivers into a chat stores how many earlier chat messages a run sends to the model.
A schedule row that existed before the column reads 0, which sends no history.

Run via:
    python tests/run_tests.py unit/test_7164_include_history_column.py -v
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
    models_pkg = _pkg("elitea_core_history_col.models")
    models_pkg.db, models_pkg.db_tools, models_pkg.config = tools.db, tools.db_tools, tools.config
    sys.modules.update({
        "tools": tools,
        "elitea_core_history_col": _pkg("elitea_core_history_col"),
        "elitea_core_history_col.models": models_pkg,
    })

    from fixtures.helpers import load_module_with_stubs
    module = load_module_with_stubs(
        PLUGIN_ROOT / "models" / "entity_schedule.py", "elitea_core_history_col.models.entity_schedule"
    )
    return module.EntitySchedule


@pytest.fixture(scope="module")
def history_block():
    """Statements of the section of db_migrations.txt that adds the column, whitespace collapsed."""
    text = (PLUGIN_ROOT / "db_migrations.txt").read_text()
    section = text.split("# R-2.0.11", 1)[1].split("\n# R-2.0.10", 1)[0]
    body = "\n".join(line for line in section.splitlines()[1:] if not line.startswith("#"))
    return [" ".join(stmt.split()) for stmt in body.split(";") if stmt.strip()]


def test_schedule_without_history_setting_defaults_to_zero(schedule_model):
    column = schedule_model.__table__.c.delivery_include_history
    assert column.nullable is False
    assert column.default.arg == 0
    assert column.server_default.arg.text == "0"


def test_history_column_is_an_integer(schedule_model):
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    ddl = " ".join(str(CreateTable(schedule_model.__table__).compile(dialect=postgresql.dialect())).split())
    assert "delivery_include_history INTEGER DEFAULT 0 NOT NULL" in ddl


def test_migration_adds_the_history_column_to_an_existing_schedule_table(history_block):
    assert history_block == [
        ("ALTER TABLE {pid}.entity_schedules "
        "ADD COLUMN IF NOT EXISTS delivery_include_history INTEGER NOT NULL DEFAULT 0")
    ]


def test_migration_statement_is_idempotent(history_block):
    for stmt in history_block:
        assert re.match(r"ALTER TABLE \{pid\}\.entity_schedules ", stmt)
        assert stmt.count("ADD COLUMN IF NOT EXISTS") == stmt.count("ADD COLUMN")
