import importlib.util
import os
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6724_chat_templates import PLUGIN_ROOT, _make_migration_stubs  # noqa: E402  pylint: disable=C0413

PROJECTS = [{"id": 1}, {"id": 2}, {"id": 5}]


@pytest.fixture
def admin_tasks(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
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
    return module


def run_task(admin_tasks, monkeypatch, param, project_list=lambda **kwargs: PROJECTS):
    migrated = []
    monkeypatch.setattr(
        admin_tasks, "apply_skill_run_settings_column",
        lambda project_ids: (migrated.extend(project_ids) or list(project_ids), []),
    )
    pylon_module = types.SimpleNamespace(
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
            call=types.SimpleNamespace(project_list=project_list),
        )),
    )
    result = admin_tasks.Method.migrate_skill_run_settings_column(pylon_module, param=param)
    return result, migrated


@pytest.mark.parametrize("param", ["project_id=all", "", None, "PROJECT_ID = all", "project_id=abc", "project_id=²", "dry_run"])
def test_every_created_project_is_migrated_unless_one_is_named(admin_tasks, monkeypatch, param):
    result, migrated = run_task(admin_tasks, monkeypatch, param)
    assert migrated == [1, 2, 5]
    assert result == {"migrated": 3, "failed": 0, "failed_projects": []}


@pytest.mark.parametrize("param", ["project_id=5", " project_id = 5 ", "dry_run;project_id=5", "project_id=+5"])
def test_a_named_project_is_the_only_one_migrated(admin_tasks, monkeypatch, param):
    result, migrated = run_task(admin_tasks, monkeypatch, param)
    assert migrated == [5] and result["migrated"] == 1


def test_a_failed_project_listing_is_reported_not_raised(admin_tasks, monkeypatch):
    def broken(**kwargs):
        raise ConnectionError("rpc down")

    result, migrated = run_task(admin_tasks, monkeypatch, "project_id=all", project_list=broken)
    assert result == {"migrated": 0, "error": "failed to list projects"} and migrated == []
