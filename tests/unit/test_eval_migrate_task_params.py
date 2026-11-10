"""The four ``migrate_eval_*`` admin tasks share ``_parse_project_param``: a bad project_id is an error,
not a silent scan of every project, and an empty param still means all projects."""
import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_6724_chat_templates import _make_migration_stubs  # noqa: E402  pylint: disable=C0413

TASKS = {
    "migrate_eval_dataset_columns": ("eval_dataset_schema", "apply_eval_dataset_columns"),
    "migrate_eval_case_usage_columns": ("eval_case_usage_schema", "apply_eval_case_usage_columns"),
    "migrate_eval_suite_columns": ("eval_suite_schema", "apply_eval_suite_columns"),
    "migrate_eval_dimension_columns": ("eval_dimension_schema", "apply_eval_dimension_columns"),
}


@pytest.fixture
def run_task(monkeypatch):
    applied = []
    stubs = _make_migration_stubs({})
    for module_name, fn in TASKS.values():
        schema = types.ModuleType(f"elitea_core.utils.{module_name}")
        if module_name == "eval_dataset_schema":
            apply = lambda pids, dry_run=False: (applied.append(list(pids)) or (list(pids), [], []))
        else:
            apply = lambda pids: (applied.append(list(pids)) or (list(pids), []))
        setattr(schema, fn, apply)
        stubs[f"elitea_core.utils.{module_name}"] = schema
    for name, module in stubs.items():
        monkeypatch.setitem(sys.modules, name, module)
    for pkg in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                "elitea_core.methods"):
        if pkg not in sys.modules:
            package = types.ModuleType(pkg)
            package.__path__ = [str(PLUGIN_ROOT)]
            monkeypatch.setitem(sys.modules, pkg, package)
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"))
    admin_mod = importlib.util.module_from_spec(spec)
    admin_mod.__package__ = "elitea_core.methods"
    monkeypatch.setitem(sys.modules, spec.name, admin_mod)
    spec.loader.exec_module(admin_mod)

    rpc = types.SimpleNamespace(project_list=lambda filter_=None: [{"id": 1}, {"id": 2}])
    fake_self = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(call=rpc)))

    def run(task, param):
        return getattr(admin_mod.Method, task)(fake_self, param=param), applied

    return run


@pytest.mark.parametrize("task", TASKS)
def test_an_invalid_project_id_is_rejected_without_migrating(run_task, task):
    result, applied = run_task(task, "project_id=7x")
    assert result == {"migrated": 0, "error": "invalid project_id: '7x'"}
    assert applied == []


@pytest.mark.parametrize("task", TASKS)
@pytest.mark.parametrize("param, expected", [("project_id=7", [7]), ("project_id=all", [1, 2]), ("", [1, 2])])
def test_the_requested_projects_are_migrated(run_task, task, param, expected):
    result, applied = run_task(task, param)
    assert applied == [expected]
    assert result["migrated"] == len(expected)
