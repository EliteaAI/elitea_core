"""The `ensure_toolkit_index_schedule_columns` admin task and its DDL list.

The DDL itself runs against PostgreSQL in a PostgreSQL run of the same list; here the param parsing,
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
    fake = types.ModuleType("elitea_core.utils.toolkit_index_schedule_ddl")
    fake.pending, fake.applied, fake.failing = [], [], set()
    fake.toolkit_index_pending_project_ids = lambda: list(fake.pending)

    def apply(project_ids, dry_run=False):
        fake.applied.append((list(project_ids), dry_run))
        return [p for p in project_ids if p not in fake.failing], [p for p in project_ids if p in fake.failing]

    fake.apply_toolkit_index_schedule_columns = apply
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
    return admin_tasks.Method.ensure_toolkit_index_schedule_columns(pylon_module, param=param), listed


@pytest.mark.parametrize("param", [None, "", "dry_run"])
def test_toolkit_index_ddl_project_id_is_required(admin_tasks, param):
    result, _ = run(admin_tasks, param)
    assert result["migrated"] == 0 and "project_id= is required" in result["error"]
    assert admin_tasks.fake.applied == []


def test_toolkit_index_ddl_all_touches_only_created_projects_still_pending(admin_tasks):
    result, listed = run(admin_tasks, "project_id=all")
    assert admin_tasks.fake.applied == [([1, 5], False)]
    assert listed == [{"filter_": {"create_success": True}}]
    assert result == {"migrated": 2, "projects": [1, 5], "failed": 0, "failed_projects": [], "dry_run": False}


def test_toolkit_index_ddl_dry_run_lists_the_projects_still_missing_the_columns(admin_tasks):
    result, _ = run(admin_tasks, "project_id=all;dry_run")
    assert admin_tasks.fake.applied == [([1, 5], True)]
    assert result["projects"] == [1, 5] and result["dry_run"] is True


def test_toolkit_index_ddl_a_migrated_project_is_left_alone_and_failures_are_reported(admin_tasks):
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
    spec = importlib.util.spec_from_file_location(
        "toolkit_index_ddl_schema", os.path.join(PLUGIN_ROOT, "utils", "toolkit_index_schedule_ddl.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_toolkit_index_ddl_every_statement_is_guarded_so_a_rerun_is_a_no_op(schema_module):
    for sql in schema_module.toolkit_index_statements():
        guarded = ("IF NOT EXISTS" in sql or "DROP NOT NULL" in sql or sql.startswith("COMMENT ON")
                   or "IF EXISTS (SELECT 1 FROM pg_constraint" in sql)
        assert guarded, sql
        assert '"p_' not in sql


def test_toolkit_index_ddl_every_new_column_gets_a_comment(schema_module):
    sql = "\n".join(schema_module.toolkit_index_statements())
    for name, _ in schema_module.COMMENTS:
        assert f"COMMENT ON COLUMN entity_schedules.{name} IS" in sql


def test_toolkit_index_ddl_statement_order_relaxes_before_it_constrains(schema_module):
    statements = schema_module.toolkit_index_statements()
    first = {key: next(i for i, sql in enumerate(statements) if key in sql)
             for key in ("DROP NOT NULL", "ADD COLUMN IF NOT EXISTS toolkit_id", "ck_entity_schedules_target",
                         "uq_entity_schedules_toolkit_index")}
    assert first["DROP NOT NULL"] < first["ck_entity_schedules_target"]
    assert first["ADD COLUMN IF NOT EXISTS toolkit_id"] < first["ck_entity_schedules_target"]
    assert first["ck_entity_schedules_target"] < first["uq_entity_schedules_toolkit_index"]


def test_toolkit_index_ddl_runs_in_the_project_schema_through_a_bound_search_path(schema_module):
    executed = []

    class Session:
        def execute(self, statement, params=None):
            executed.append((str(statement), params))

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    schema_module.db = types.SimpleNamespace(get_session=lambda pid: Session())
    migrated, failed = schema_module.apply_toolkit_index_schedule_columns([3])
    assert (migrated, failed) == ([3], [])
    schema_sets = [i for i, (sql, _) in enumerate(executed) if "set_config('search_path', :path, true)" in sql]
    assert [executed[i][1] for i in schema_sets] == [{"path": "p_3"}, {"path": "p_3"}]
    first, second = schema_sets
    assert [sql for sql, _ in executed[first + 1:second]] == list(schema_module.toolkit_index_statements())
    assert [sql for sql, _ in executed[second + 1:]] == list(schema_module.toolkit_index_validate_statements())
    assert all(params is None for i, (_, params) in enumerate(executed) if i not in schema_sets)


def test_toolkit_index_ddl_adds_both_checks_not_valid_and_validates_them_after_every_other_statement(schema_module):
    statements = schema_module.toolkit_index_statements()
    added = [sql for sql in statements if "ADD CONSTRAINT" in sql]
    assert len(added) == 2 and all(sql.rstrip().endswith("NOT VALID; END IF; END $$") for sql in added)
    validated = schema_module.toolkit_index_validate_statements()
    assert [sql.split()[-1] for sql in validated] == [schema_module.TYPE_CHECK, schema_module.TARGET_CHECK]
    assert all("VALIDATE CONSTRAINT" in sql for sql in validated)
    assert not any("VALIDATE CONSTRAINT" in sql for sql in statements)


def test_toolkit_index_ddl_a_failed_validation_is_reported_as_failed_after_the_columns_were_added(schema_module):
    runs = []
    schema_module.log = types.SimpleNamespace(exception=lambda *args: runs.append("logged"))

    class Session:
        def execute(self, statement, params=None):
            if "VALIDATE CONSTRAINT" in str(statement):
                raise RuntimeError("check violated")
            runs.append(str(statement))

        def commit(self):
            runs.append("commit")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    schema_module.db = types.SimpleNamespace(get_session=lambda pid: Session())
    assert schema_module.apply_toolkit_index_schedule_columns([3]) == ([], [3])
    assert "commit" in runs and runs[-1] == "logged"


def test_toolkit_index_ddl_pending_query_also_selects_schemas_with_an_unvalidated_check(schema_module):
    assert "NOT v.convalidated" in str(schema_module._PENDING_QUERY)
