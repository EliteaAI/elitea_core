"""The admin task that clears next_run of schedules paused before a pause cleared it (methods/admin_tasks.py)."""
import ast
import pathlib
import sys
import types
from unittest.mock import MagicMock, create_autospec

import pytest
from fixtures.cut_source import load_cut

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import test_6921_skill_run_settings_migration_task as migration_task  # noqa: E402  pylint: disable=C0413

PROJECTS = migration_task.PROJECTS
admin_tasks = migration_task.admin_tasks  # the fixture is bound here so pytest finds it in this module

PACKAGE = "plugins.elitea_core"
TASK = "clear_paused_schedule_next_run"
STATEMENT = "UPDATE p_{pid}.entity_schedules SET next_run = NULL WHERE NOT enabled AND next_run IS NOT NULL"


@pytest.fixture
def run_task(admin_tasks, monkeypatch):  # pylint: disable=W0621
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


class AdminModule:
    """The two registration methods of the admin plugin, with their real signatures (admin/methods/tasks.py)."""

    def register_admin_task(self, name, func, group=None):
        raise NotImplementedError

    def unregister_admin_task(self, name, func):
        raise NotImplementedError


@pytest.fixture
def module_methods(monkeypatch):
    """The real `ready` and `deinit` of module.py, cut out of its source, and a `self` that records their calls."""
    admin = create_autospec(AdminModule, instance=True)
    tools = types.ModuleType("tools")
    tools.this = types.SimpleNamespace(for_module=lambda name: types.SimpleNamespace(module=admin))
    trace_step_schema = types.ModuleType(f"{PACKAGE}.utils.trace_step_schema")
    trace_step_schema.ensure_trace_step_schema = lambda engine: None
    monkeypatch.setitem(sys.modules, "tools", tools)
    monkeypatch.setitem(sys.modules, trace_step_schema.__name__, trace_step_schema)

    tree = ast.parse((pathlib.Path(__file__).resolve().parents[3] / "module.py").read_text())
    [module_class] = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Module"]
    nodes = [node for node in module_class.body if getattr(node, "name", None) in ("ready", "deinit")]
    namespace = load_cut(nodes, {
        "db": MagicMock(), "auth": MagicMock(), "log": MagicMock(), "prompt_2_agent_migration": MagicMock(),
        "CAPABILITIES_EVENT": "capabilities", "WITHHOLD_EVENT": "withhold",
    }, f"{PACKAGE}.module")
    return namespace["ready"], namespace["deinit"], MagicMock(name="module"), admin


def test_the_task_is_registered_in_ready_under_a_release_group(module_methods):
    ready, _, module, admin = module_methods
    ready(module)
    [call] = [call for call in admin.register_admin_task.call_args_list if call.args[0] == TASK]
    assert call.args[1] is module.clear_paused_schedule_next_run
    assert call.kwargs.get("group")


def test_the_task_is_unregistered_in_deinit(module_methods):
    _, deinit, module, admin = module_methods
    deinit(module)
    admin.unregister_admin_task.assert_any_call(TASK, module.clear_paused_schedule_next_run)
