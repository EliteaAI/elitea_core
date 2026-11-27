"""The Test Run schema helper and its admin task (utils/schedule_test_run_schema.py, methods/admin_tasks.py)."""
import pathlib
import sys
import types

import pytest
from unit._tool_spend_loader import load

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from test_6921_skill_run_settings_migration_task import PROJECTS, admin_tasks  # noqa: E402,F401  pylint: disable=C0413


@pytest.fixture
def schema(monkeypatch):
    package = types.ModuleType("plugins.elitea_core.utils.skill_publish_schema")
    package.apply_project_statements = lambda project_ids, statements: (list(project_ids), [])
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.skill_publish_schema", package)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.schedule_test_run_schema", None)
    load("utils/admin_task_params.py", fresh=True)
    return load("utils/schedule_test_run_schema.py", fresh=True)


def test_every_statement_is_idempotent(schema):
    for statement in schema.TEST_RUN_STATEMENTS:
        assert (
            "IF NOT EXISTS" in statement or statement.endswith("DROP NOT NULL")
        ), statement
    assert all(statement.startswith(("ALTER TABLE", "CREATE")) for statement in schema.TEST_RUN_STATEMENTS)


def test_the_statements_add_columns_before_the_indexes_that_use_them(schema):
    joined = "\n".join(schema.TEST_RUN_STATEMENTS)
    assert joined.index("ADD COLUMN IF NOT EXISTS form") < joined.index("uq_entity_schedule_runs_active_test")
    assert joined.index("ADD COLUMN IF NOT EXISTS application_id") < joined.index("ix_entity_schedule_runs_test_owner")
    assert "trigger_status" not in joined


def test_the_helper_runs_the_statements_through_the_shared_function(schema, monkeypatch):
    seen = []
    monkeypatch.setattr(
        schema, "apply_project_statements",
        lambda project_ids, statements: (seen.append((list(project_ids), statements)) or [1], [{"project_id": 2}]),
    )
    assert schema.apply_schedule_test_run_columns([1, 2]) == ([1], [{"project_id": 2}])
    assert seen == [([1, 2], schema.TEST_RUN_STATEMENTS)]


@pytest.mark.parametrize("param, expected", [
    (None, True), ("project_id=all", True), ("project_id=all;dry_run", True), ("dry_run=true", True),
    ("project_id=5;dry_run=false", False), ("DRY_RUN=False;project_id=5", False),
])
def test_only_dry_run_false_turns_the_dry_run_off(schema, param, expected):
    assert schema.dry_run_flag(param) is expected


def _run_task(admin_tasks, schema, monkeypatch, param, apply):
    monkeypatch.setattr(schema, "apply_schedule_test_run_columns", apply)
    monkeypatch.setitem(sys.modules, "elitea_core.utils.schedule_test_run_schema", schema)
    pylon_module = types.SimpleNamespace(
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
            call=types.SimpleNamespace(project_list=lambda **kwargs: PROJECTS))),
    )
    return admin_tasks.Method.migrate_schedule_test_runs(pylon_module, param=param)


def test_the_admin_task_reports_migrated_and_failed_projects(admin_tasks, schema, monkeypatch):
    result = _run_task(
        admin_tasks, schema, monkeypatch, "project_id=all;dry_run=false",
        lambda project_ids: ([1, 5], [{"project_id": 2}]),
    )
    assert result == {"migrated": 2, "failed": 1, "failed_projects": [{"project_id": 2}], "dry_run": False}


def test_the_admin_task_dry_run_changes_nothing(admin_tasks, schema, monkeypatch):
    calls = []
    result = _run_task(admin_tasks, schema, monkeypatch, "project_id=all", lambda ids: calls.append(ids) or (ids, []))
    assert calls == []
    assert result["dry_run"] is True and result["migrated"] == len(PROJECTS)


def test_the_admin_task_runs_one_named_project(admin_tasks, schema, monkeypatch):
    calls = []
    _run_task(admin_tasks, schema, monkeypatch, "project_id=5;dry_run=false", lambda ids: calls.append(ids) or (ids, []))
    assert calls == [[5]]
