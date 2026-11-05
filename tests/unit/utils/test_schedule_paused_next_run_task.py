"""The admin task that clears next_run of schedules paused before a pause cleared it (methods/admin_tasks.py)."""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from test_6921_skill_run_settings_migration_task import PROJECTS, admin_tasks  # noqa: E402,F401  pylint: disable=C0413

STATEMENT = "UPDATE p_{pid}.entity_schedules SET next_run = NULL WHERE NOT enabled AND next_run IS NOT NULL"


@pytest.fixture
def run_task(admin_tasks, monkeypatch):  # noqa: F811 -- the fixture is imported from the sibling test
    calls = []
    stubs = {
        "schedule_lifecycle": {"CLEAR_PAUSED_NEXT_RUN": STATEMENT},
        "schedule_test_run_schema": {"dry_run_flag": lambda param: "dry_run=false" not in (param or "")},
        "skill_publish_schema": {
            "apply_project_statements":
                lambda project_ids, statements: (calls.append((list(project_ids), statements)) or list(project_ids), []),
        },
    }
    for name, attributes in stubs.items():
        module = types.ModuleType(f"elitea_core.utils.{name}")
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, module.__name__, module)
    pylon_module = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
        call=types.SimpleNamespace(project_list=lambda **kwargs: PROJECTS))))

    def run(param):
        return admin_tasks.Method.clear_paused_schedule_next_run(pylon_module, param=param), calls

    return run


def test_a_dry_run_changes_nothing(run_task):
    result, calls = run_task("project_id=all")
    assert calls == []
    assert result == {"migrated": len(PROJECTS), "failed": 0, "failed_projects": [], "dry_run": True}


def test_a_real_run_sends_the_statement_for_every_project(run_task):
    result, calls = run_task("project_id=all;dry_run=false")
    assert calls == [([1, 2, 5], (STATEMENT,))]
    assert result == {"migrated": 3, "failed": 0, "failed_projects": [], "dry_run": False}


def test_a_named_project_is_the_only_one_cleared(run_task):
    _, calls = run_task("project_id=5;dry_run=false")
    assert [project_ids for project_ids, _ in calls] == [[5]]

