"""#7398 AC11: the `ensure_webhook_trigger_columns` admin task and its DDL list.

The DDL itself runs against PostgreSQL in integration/test_7398_webhook_rows_pg.py; here the param parsing,
project selection, dry run and the guard of every statement are pinned without a database.
"""
import importlib.util
import os
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from test_6724_chat_templates import PLUGIN_ROOT, _make_migration_stubs  # noqa: E402

PROJECTS = [{"id": 1}, {"id": 2}, {"id": 5}]


@pytest.fixture
def admin_tasks(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                    "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [PLUGIN_ROOT]
        sys.modules.setdefault(package, module)
    fake = types.ModuleType("elitea_core.utils.webhook_trigger_schema")
    fake.pending, fake.applied, fake.failing = [], [], set()
    fake.project_ids_pending = lambda: list(fake.pending)

    def apply(project_ids, dry_run=False):
        fake.applied.append((list(project_ids), dry_run))
        return [p for p in project_ids if p not in fake.failing], [p for p in project_ids if p in fake.failing]

    fake.apply_webhook_trigger_columns = apply
    sys.modules[fake.__name__] = fake
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"))
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.fake = fake
    return module


def run(admin_tasks, param, pending=(1, 5, 77)):
    admin_tasks.fake.pending = list(pending)
    listed = []
    pylon_module = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
        call=types.SimpleNamespace(project_list=lambda **kw: listed.append(kw) or PROJECTS))))
    return admin_tasks.Method.ensure_webhook_trigger_columns(pylon_module, param=param), listed


@pytest.mark.parametrize("param", [None, "", "dry_run"])
def test_7398_project_id_is_required(admin_tasks, param):
    result, _ = run(admin_tasks, param)
    assert result["migrated"] == 0 and "project_id= is required" in result["error"]
    assert admin_tasks.fake.applied == []


def test_7398_all_touches_only_created_projects_still_pending(admin_tasks):
    result, listed = run(admin_tasks, "project_id=all")
    assert admin_tasks.fake.applied == [([1, 5], False)]
    assert listed == [{"filter_": {"create_success": True}}]
    assert result == {"migrated": 2, "projects": [1, 5], "failed": 0, "failed_projects": [], "dry_run": False}


def test_7398_dry_run_lists_the_projects_still_missing_the_columns(admin_tasks):
    result, _ = run(admin_tasks, "project_id=all;dry_run")
    assert admin_tasks.fake.applied == [([1, 5], True)]
    assert result["projects"] == [1, 5] and result["dry_run"] is True


def test_7398_a_migrated_project_is_left_alone_and_failures_are_reported(admin_tasks):
    assert run(admin_tasks, "project_id=2")[0]["migrated"] == 0
    admin_tasks.fake.failing = {5}
    result, _ = run(admin_tasks, "project_id=all")
    assert (result["failed"], result["failed_projects"]) == (1, [5])


@pytest.fixture
def schema_module(isolated_sys_modules, monkeypatch):
    for name in ("pylon", "pylon.core"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", types.SimpleNamespace(log=types.SimpleNamespace()))
    monkeypatch.setitem(sys.modules, "tools", types.SimpleNamespace(db=None))
    spec = importlib.util.spec_from_file_location("w7398_schema", os.path.join(PLUGIN_ROOT, "utils", "webhook_trigger_schema.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_7398_every_statement_is_guarded_so_a_rerun_is_a_no_op(schema_module):
    for sql in schema_module.statements("p_3"):
        guarded = ("IF NOT EXISTS" in sql or "DROP NOT NULL" in sql or sql.startswith("COMMENT ON")
                   or "IF EXISTS (SELECT 1 FROM pg_indexes" in sql)
        assert guarded, sql
        assert '"p_3"' in sql


def test_7398_every_new_or_relaxed_column_gets_a_comment(schema_module):
    sql = "\n".join(schema_module.statements("p_3"))
    for name, *_ in (*schema_module.COLUMNS, *schema_module.RELAXED):
        assert f'COMMENT ON COLUMN "p_3".entity_schedules.{name} IS' in sql
    assert 'COMMENT ON TABLE "p_3".entity_schedules' in sql
