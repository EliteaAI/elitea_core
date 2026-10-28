"""Unit tests for the chat link columns of the schedule run table.

A run delivered into a chat keeps the chat message it produced in the run row.
A run row that existed before the columns, or a run_history run, reads with empty links.

Run via:
    python tests/run_tests.py unit/test_schedule_run_link_columns.py -v
"""
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
LINK_COLUMNS = ("conversation_uuid", "message_group_id")


def _pkg(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


@pytest.fixture
def run_model(isolated_sys_modules):
    """EntityScheduleRun loaded on real SQLAlchemy with a fresh declarative base."""
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
    models_pkg = _pkg("elitea_core_run_link.models")
    models_pkg.db, models_pkg.db_tools, models_pkg.config = tools.db, tools.db_tools, tools.config
    sys.modules.update({
        "tools": tools,
        "elitea_core_run_link": _pkg("elitea_core_run_link"),
        "elitea_core_run_link.models": models_pkg,
    })

    from fixtures.helpers import load_module_with_stubs
    module = load_module_with_stubs(
        PLUGIN_ROOT / "models" / "entity_schedule.py", "elitea_core_run_link.models.entity_schedule"
    )
    return module.EntityScheduleRun


@pytest.fixture(scope="module")
def link_block():
    """Statements of the newest section of db_migrations.txt, whitespace collapsed."""
    text = (PLUGIN_ROOT / "db_migrations.txt").read_text()
    section = text.split("# R-2.0.10", 1)[1].split("\n# R-2.0.9", 1)[0]
    body = "\n".join(line for line in section.splitlines()[1:] if not line.startswith("#"))
    return [" ".join(stmt.split()) for stmt in body.split(";") if stmt.strip()]


def _ddl(model):
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    return " ".join(str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())).split())


def test_run_without_chat_link_has_empty_link_columns(run_model):
    columns = run_model.__table__.c
    for name in LINK_COLUMNS:
        assert columns[name].nullable is True
        assert columns[name].default is None
        assert columns[name].server_default is None


def test_run_link_columns_have_uuid_and_integer_types(run_model):
    ddl = _ddl(run_model)
    assert "conversation_uuid UUID," in ddl
    assert "message_group_id INTEGER," in ddl


def test_run_keeps_its_chat_id_column(run_model):
    column = run_model.__table__.c.conversation_id
    assert column.nullable is True
    assert "conversation_id INTEGER," in _ddl(run_model)


def test_migration_adds_the_link_columns_to_an_existing_run_table(link_block):
    assert link_block == [
        (
            "ALTER TABLE {pid}.entity_schedule_runs "
            "ADD COLUMN IF NOT EXISTS conversation_uuid UUID NULL, "
            "ADD COLUMN IF NOT EXISTS message_group_id INTEGER NULL"
        ),
    ]


def test_migration_statements_are_idempotent(link_block):
    for stmt in link_block:
        assert re.match(r"ALTER TABLE \{pid\}\.entity_schedule_runs ", stmt)
        assert stmt.count("ADD COLUMN IF NOT EXISTS") == stmt.count("ADD COLUMN")
