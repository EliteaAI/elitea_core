"""EL-7011 US-24 R1.1, R1.3: the admin task entry defaults to a dry run, loops projects and survives a failing one."""
import importlib.util
import os
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6724_chat_templates import PLUGIN_ROOT, _make_migration_stubs  # pylint: disable=C0413


@pytest.fixture
def entry(isolated_sys_modules):
    stubs = _make_migration_stubs([])
    calls = []
    failing = set()

    def migrate_project(project_id, dry_run):
        calls.append((project_id, dry_run))
        if project_id in failing:
            raise RuntimeError("boom")
        return {"project_id": project_id, "found": 1}

    legacy = types.ModuleType("elitea_core.utils.legacy_schedule_migration")
    legacy.migrate_project = migrate_project

    def parse_task_params(raw):
        if "project_id=" not in raw:
            raise ValueError("project_id= is required")
        value = raw.split("project_id=")[1].split(";")[0]
        spec = ("all", None) if value == "all" else ("single", int(value))
        return spec, "dry_run=false" not in raw

    legacy.parse_task_params = parse_task_params
    stubs[legacy.__name__] = legacy
    sys.modules.update(stubs)
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                    "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [PLUGIN_ROOT]
        sys.modules.setdefault(package, module)
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"),
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.resolve_target_project_ids = lambda spec: [1, 2] if spec[0] == "all" else [spec[1]]
    return module, calls, failing


def run(entry, param):
    module, _, _ = entry
    return module.Method.migrate_legacy_pipeline_schedules(types.SimpleNamespace(), param=param)


def test_el_7011_r1_1_dry_run_is_the_default(entry):
    result = run(entry, "project_id=all")
    assert entry[1] == [(1, True), (2, True)]
    assert result["dry_run"] is True
    assert result["system_user_schedules_left"] == 0
    assert [project["project_id"] for project in result["projects"]] == [1, 2]


def test_el_7011_r1_1_only_dry_run_false_writes(entry):
    result = run(entry, "project_id=7;dry_run=false")
    assert entry[1] == [(7, False)]
    assert result["dry_run"] is False


def test_el_7011_r1_3_a_failing_project_goes_under_errors_and_the_loop_goes_on(entry):
    entry[2].add(1)
    result = run(entry, "project_id=all")
    assert [project["project_id"] for project in result["projects"]] == [2]
    assert result["errors"] == [{"project_id": 1, "error": "boom"}]


def test_el_7011_r1_1_bad_params_return_an_error_and_touch_nothing(entry):
    result = run(entry, "")
    assert "project_id=" in result["error"]
    assert entry[1] == []
