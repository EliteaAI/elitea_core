"""The `restore_legacy_index_schedules` admin task: one, a range or all projects; a missing step setting counts as off."""
import sys
import types

import pytest
from unit import test_toolkit_index_schedule_ddl_task as ddl_task

admin_tasks = ddl_task.admin_tasks  # the fixture of the DDL task test, bound here so pytest finds it
WARNING = "a restored schedule is deleted with its run history"
ALL_PROJECTS = [1, 2, 5]


@pytest.fixture
def restore_task(admin_tasks, monkeypatch):
    calls, resolved, logged = [], [], []
    failing = set()

    def refusal(settings):
        return "the legacy index step is on" if (settings or {}).get("legacy_index_step_enabled", False) else None

    def restore(project_id, dry_run=False):
        calls.append((project_id, dry_run))
        if project_id in failing:
            raise RuntimeError("secret database detail")
        return {"project_id": project_id, "restored": [project_id * 10], "errors": [], "without_original_trigger": 2,
                "warning": WARNING}

    monkeypatch.setitem(sys.modules, "elitea_core.utils.legacy_index_schedule_migration", types.SimpleNamespace(
        RESTORE_HISTORY_WARNING=WARNING, index_restore_refusal=refusal, restore_index_schedules=restore))

    def parse(raw):
        params = dict(part.split("=") for part in raw.split(";") if part)
        project = params["project_id"]
        spec = ("all",) if project == "all" else ("single", int(project))
        return spec, params.get("dry_run", "true") != "false"

    monkeypatch.setitem(sys.modules, "elitea_core.utils.legacy_schedule_migration",
                        types.SimpleNamespace(parse_task_params=parse))

    def resolve(spec):
        resolved.append(spec)
        return list(ALL_PROJECTS) if spec[0] == "all" else [spec[1]]

    monkeypatch.setattr(admin_tasks, "resolve_target_project_ids", resolve)
    monkeypatch.setattr(admin_tasks, "log", types.SimpleNamespace(
        info=lambda *args: logged.append(args), error=lambda *args: None, exception=lambda *args: None))

    def run(param, settings):
        owner = types.SimpleNamespace(descriptor=types.SimpleNamespace(
            config={"scheduler": {"schedules": settings}}))
        return admin_tasks.Method.restore_legacy_index_schedules(owner, param=param)

    run.calls, run.resolved, run.logged, run.failing = calls, resolved, logged, failing
    return run


def test_a_restore_runs_when_the_step_setting_is_missing(restore_task):
    result = restore_task("project_id=5;dry_run=false", {})
    assert result["dry_run"] is False and result["projects"][0]["restored"] == [50]
    assert restore_task.calls == [(5, False)]


def test_a_restore_is_refused_while_the_step_setting_is_on(restore_task):
    result = restore_task("project_id=5;dry_run=false", {"legacy_index_step_enabled": True})
    assert "the legacy index step is on" in result["error"]
    assert restore_task.calls == []


def test_project_id_all_restores_every_project(restore_task):
    result = restore_task("project_id=all;dry_run=false", {})
    assert restore_task.calls == [(1, False), (2, False), (5, False)]
    assert [p["project_id"] for p in result["projects"]] == ALL_PROJECTS
    assert result["errors"] == [] and result["dry_run"] is False


def test_a_failing_project_goes_under_errors_and_the_next_project_runs(restore_task):
    restore_task.failing.add(2)
    result = restore_task("project_id=all;dry_run=false", {})
    assert [p["project_id"] for p in result["projects"]] == [1, 5]
    assert result["errors"] == [{"project_id": 2, "error": "the project could not be restored; see the log"}]
    assert "secret" not in str(result)


def test_project_id_all_is_refused_while_the_step_setting_is_on_and_reads_no_project(restore_task):
    result = restore_task("project_id=all;dry_run=false", {"legacy_index_step_enabled": True})
    assert "the legacy index step is on" in result["error"]
    assert restore_task.resolved == [] and restore_task.calls == []


def test_a_dry_run_reports_each_project_and_writes_nothing(restore_task):
    result = restore_task("project_id=all", {})
    assert restore_task.calls == [(1, True), (2, True), (5, True)]
    assert result["dry_run"] is True
    assert [p["restored"] for p in result["projects"]] == [[10], [20], [50]]


def test_a_project_not_found_returns_the_resolver_error(restore_task, admin_tasks, monkeypatch):
    def missing(spec):
        raise ValueError("Project 9 not found")

    monkeypatch.setattr(admin_tasks, "resolve_target_project_ids", missing)
    assert restore_task("project_id=9", {}) == {"error": "Project 9 not found"}
    assert restore_task.calls == []


def test_the_task_logs_one_summary_line(restore_task):
    restore_task.failing.add(2)
    restore_task("project_id=all;dry_run=false", {})
    summaries = [args for args in restore_task.logged if "Exiting restore_legacy_index_schedules" in args[0]]
    assert len(summaries) == 1
    assert summaries[0][2] == [(1, 1, 2, 0), (5, 1, 2, 0)] and summaries[0][3] == 1
