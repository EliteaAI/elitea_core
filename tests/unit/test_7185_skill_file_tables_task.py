"""#7185: the skill file admin tasks - ``create_skill_file_tables`` creates the manifest table only where it is
missing, ``sweep_skill_files`` sweeps the buckets of the projects the parameter names; the startup guard in
``module.py`` creates the table off-thread in the schemas that lack it."""
import ast
import importlib.util
import os
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6724_chat_templates import PLUGIN_ROOT, _make_migration_stubs  # noqa: E402  pylint: disable=C0413

PROJECTS = [{"id": 1}, {"id": 2}, {"id": 5}]
TABLES = ('skill_version_files',)


@pytest.fixture
def task(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                    "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [PLUGIN_ROOT]
        sys.modules.setdefault(package, module)

    created = []
    schema = types.ModuleType("elitea_core.utils.agent_identity_schema")
    schema.project_ids_missing_tables = lambda models: [2, 5] if models == TABLES else []
    schema.apply_tables = lambda project_ids, models, dry_run=False: (
        created.extend((pid, models, dry_run) for pid in project_ids) or list(project_ids), [])
    sys.modules[schema.__name__] = schema
    models_skill = types.ModuleType("elitea_core.models.skill")
    models_skill.SKILL_FILE_TABLES = TABLES
    sys.modules[models_skill.__name__] = models_skill
    swept = []
    skill_files = types.ModuleType("elitea_core.utils.skill_files")

    def sweep(project_id, dry_run=False):
        swept.append((project_id, dry_run))
        if project_id == 5:
            raise RuntimeError("storage down")
        return {"project_id": project_id, "deleted": ["a" * 64] if project_id == 2 else [], "dry_run": dry_run}

    skill_files.sweep_skill_files = sweep
    sys.modules[skill_files.__name__] = skill_files

    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"),
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    pylon_module = types.SimpleNamespace(
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
            call=types.SimpleNamespace(project_list=lambda **kwargs: PROJECTS),
        )),
    )
    return types.SimpleNamespace(
        run=lambda param: module.Method.create_skill_file_tables(pylon_module, param=param), created=created,
        sweep=lambda param: module.Method.sweep_skill_files(pylon_module, param=param), swept=swept,
    )


def test_only_projects_lacking_the_tables_get_them(task):
    result = task.run("project_id=all")

    assert task.created == [(2, TABLES, False), (5, TABLES, False)]
    assert result == {"migrated": 2, "failed": 0, "failed_projects": [], "dry_run": False}


def test_a_dry_run_reports_without_creating(task):
    result = task.run("project_id=all;dry_run")

    assert task.created == [(2, TABLES, True), (5, TABLES, True)]
    assert result["dry_run"] is True and result["migrated"] == 2


def test_a_named_project_that_already_has_the_tables_is_left_alone(task):
    assert task.run("project_id=1")["migrated"] == 0 and task.created == []


def test_a_named_missing_project_is_the_only_one_created(task):
    assert task.run("project_id=5")["migrated"] == 1 and task.created == [(5, TABLES, False)]


def test_the_project_parameter_is_required(task):
    result = task.run("")

    assert result["migrated"] == 0 and "project_id=" in result["error"] and task.created == []


def test_the_sweep_runs_per_project_and_reports_a_failure_without_stopping(task):
    result = task.sweep("project_id=all")

    assert task.swept == [(1, False), (2, False), (5, False)]
    assert result == {
        "deleted": 1, "projects": [{"project_id": 2, "deleted": ["a" * 64], "dry_run": False}],
        "failed": 1, "failed_projects": [{"project_id": 5}], "dry_run": False,
    }


def test_a_dry_run_sweep_is_passed_down(task):
    task.sweep("project_id=2;dry_run")

    assert task.swept == [(2, True)]


def test_the_sweep_needs_the_project_parameter(task):
    result = task.sweep("dry_run")

    assert result["deleted"] == 0 and "project_id=" in result["error"] and task.swept == []


def _module_method(name):
    """The real ``Module.<name>`` definition from module.py, compiled alone so it runs without the plugin's imports."""
    tree = ast.parse(pathlib.Path(PLUGIN_ROOT, "module.py").read_text())
    module_class = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Module")
    return next(node for node in module_class.body if isinstance(node, ast.FunctionDef) and node.name == name)


@pytest.fixture
def guard():
    started, applied = [], []

    class InlineThread:
        def __init__(self, target, daemon=False):
            self.target, self.daemon = target, daemon

        def start(self):
            started.append(self.daemon)
            self.target()

    namespace = {
        "Thread": InlineThread,
        "SKILL_FILE_TABLES": TABLES,
        "project_ids_missing_tables": lambda models: list(namespace["missing"]) if models == TABLES else [],
        "apply_tables": lambda project_ids, models: applied.append((project_ids, models)) or (project_ids, []),
        "log": types.SimpleNamespace(info=lambda *args: None, exception=lambda *args: None),
        "missing": [2, 5],
    }
    exec(compile(ast.Module(body=[_module_method("_ensure_skill_file_schema")], type_ignores=[]), "module.py", "exec"),
         namespace)
    return types.SimpleNamespace(run=lambda: namespace["_ensure_skill_file_schema"](None), namespace=namespace,
                                 started=started, applied=applied)


def test_the_startup_guard_creates_missing_tables_off_thread(guard):
    guard.run()

    assert guard.started == [True] and guard.applied == [([2, 5], TABLES)]


def test_the_startup_guard_issues_no_ddl_when_every_schema_has_the_table(guard):
    guard.namespace["missing"] = []

    guard.run()

    assert guard.applied == []


def test_the_module_runs_the_startup_guard_when_ready():
    calls = {
        node.func.attr for node in ast.walk(_module_method("ready"))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name) and node.func.value.id == "self"
    }

    assert "_ensure_skill_file_schema" in calls
