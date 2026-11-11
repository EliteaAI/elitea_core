"""The `restore_legacy_index_schedules` admin task: a missing step setting counts as off, the report warns about the history."""
import sys
import types

import pytest
from unit import test_toolkit_index_schedule_ddl_task as ddl_task

admin_tasks = ddl_task.admin_tasks  # the fixture of the DDL task test, bound here so pytest finds it
WARNING = "a restored schedule is deleted with its run history"


@pytest.fixture
def restore_task(admin_tasks, monkeypatch):
    calls = []

    def refusal(settings):
        return "the legacy index step is on" if (settings or {}).get("legacy_index_step_enabled", False) else None

    monkeypatch.setitem(sys.modules, "elitea_core.utils.legacy_index_schedule_migration", types.SimpleNamespace(
        RESTORE_HISTORY_WARNING=WARNING, index_restore_refusal=refusal,
        restore_index_schedules=lambda project_id: calls.append(project_id) or {"restored": [1], "errors": []}))

    def parse(raw):
        params = dict(part.split("=") for part in raw.split(";") if part)
        return ("single", int(params["project_id"])), params.get("dry_run", "true") != "false"

    monkeypatch.setitem(sys.modules, "elitea_core.utils.legacy_schedule_migration",
                        types.SimpleNamespace(parse_task_params=parse))

    def run(param, settings):
        owner = types.SimpleNamespace(descriptor=types.SimpleNamespace(
            config={"scheduler": {"schedules": settings}}))
        return admin_tasks.Method.restore_legacy_index_schedules(owner, param=param)

    run.calls = calls
    return run


def test_a_restore_runs_when_the_step_setting_is_missing(restore_task):
    result = restore_task("project_id=5;dry_run=false", {})
    assert result["dry_run"] is False and result["restored"] == [1]
    assert restore_task.calls == [5]


def test_a_restore_is_refused_while_the_step_setting_is_on(restore_task):
    result = restore_task("project_id=5;dry_run=false", {"legacy_index_step_enabled": True})
    assert "the legacy index step is on" in result["error"]
    assert restore_task.calls == []


def test_a_dry_run_restore_warns_about_the_run_history_and_writes_nothing(restore_task):
    assert restore_task("project_id=5", {}) == {"dry_run": True, "project_id": 5, "warning": WARNING}
    assert restore_task.calls == []
