"""The admin task that indexes chat_message_group by (conversation_id, id) (methods/admin_tasks.py, PR 766 review).

The task only picks the projects and the dry-run flag; the per-project work is `run_message_group_index`, which is
stubbed here so no database is needed. Its own rules (present, changed, failed) are checked against a fake connection.
"""
import pathlib
import sys
import types

import pytest
from fixtures.cut_source import load_cut  # noqa: F401  (the same fixture the other admin task tests use)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import test_6921_skill_run_settings_migration_task as migration_task  # noqa: E402  pylint: disable=C0413

PROJECTS = migration_task.PROJECTS
admin_tasks = migration_task.admin_tasks
TASK = "index_message_groups_by_conversation"


@pytest.fixture
def run_task(admin_tasks, monkeypatch):  # pylint: disable=W0621
    calls = []

    def fake_run(project_ids, dry_run):
        calls.append((list(project_ids), dry_run))
        return {"changed": list(project_ids), "present": [], "failed": [], "dry_run": dry_run}

    module = types.ModuleType("elitea_core.utils.message_group_index")
    module.run_message_group_index = fake_run
    monkeypatch.setitem(sys.modules, module.__name__, module)
    params = types.ModuleType("elitea_core.utils.admin_task_params")
    params.dry_run_flag = lambda param, default=True: "dry_run=false" not in (param or "")
    monkeypatch.setitem(sys.modules, params.__name__, params)
    pylon_module = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
        call=types.SimpleNamespace(project_list=lambda **kwargs: PROJECTS))))

    def run(param):
        return admin_tasks.Method.index_message_groups_by_conversation(pylon_module, param=param), calls

    return run


def test_the_default_is_a_dry_run_across_every_project(run_task):
    _, calls = run_task("project_id=all")
    assert calls == [([1, 2, 5], True)]


def test_dry_run_false_builds_the_index_in_every_project(run_task):
    _, calls = run_task("project_id=all;dry_run=false")
    assert calls == [([1, 2, 5], False)]


def test_a_named_project_is_the_only_one_indexed(run_task):
    _, calls = run_task("project_id=5;dry_run=false")
    assert calls == [([5], False)]


def test_a_project_list_failure_returns_an_error_and_runs_nothing(admin_tasks, monkeypatch):
    calls = []
    module = types.ModuleType("elitea_core.utils.message_group_index")
    module.run_message_group_index = lambda *a, **k: calls.append(a)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    params = types.ModuleType("elitea_core.utils.admin_task_params")
    params.dry_run_flag = lambda param, default=True: "dry_run=false" not in (param or "")
    monkeypatch.setitem(sys.modules, params.__name__, params)
    broken = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
        call=types.SimpleNamespace(project_list=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("down"))))))
    result = admin_tasks.Method.index_message_groups_by_conversation(broken, param="project_id=all;dry_run=false")
    assert result == {"error": "failed to list projects"}
    assert calls == []


# --- the per-project runner (utils/message_group_index.py), against a fake autocommit connection ---------------

@pytest.fixture
def runner(monkeypatch):
    """Load the real runner module with `tools.db` replaced by a fake engine that records what runs."""
    import importlib.util  # noqa: C0415
    for name in ("pylon", "pylon.core", "pylon.core.tools"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules["pylon.core.tools"].log = types.SimpleNamespace(exception=lambda *a, **k: None)
    path = pathlib.Path(__file__).resolve().parents[3] / "utils" / "message_group_index.py"
    spec = importlib.util.spec_from_file_location("message_group_index_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    state = {"present": set(), "created": [], "fail": set()}

    class Result:
        def __init__(self, found):
            self.found = found

        def first(self):
            return (1,) if self.found else None

    class Connection:
        def execution_options(self, **_):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, statement, params=None):
            text_sql = str(statement)
            if "pg_indexes" in text_sql:
                return Result(params["schema"] in state["present"] and params["index"] == module.INDEX_NAME)
            pid = int(text_sql.split(" ON p_")[1].split(".")[0])
            if pid in state["fail"]:
                raise RuntimeError("cannot build")
            state["created"].append(pid)
            return Result(False)

    engine = types.SimpleNamespace(connect=lambda: Connection())
    tools = types.ModuleType("tools")
    tools.db = types.SimpleNamespace(engine=engine)
    monkeypatch.setitem(sys.modules, "tools", tools)
    return module, state


def test_a_dry_run_creates_nothing(runner):
    module, state = runner
    result = module.run_message_group_index([1, 2], dry_run=True)
    assert state["created"] == []
    assert result == {"changed": [1, 2], "present": [], "failed": [], "dry_run": True}


def test_a_project_that_has_the_index_is_left_alone(runner):
    module, state = runner
    state["present"] = {"p_1"}
    result = module.run_message_group_index([1, 2], dry_run=False)
    assert state["created"] == [2]
    assert result == {"changed": [2], "present": [1], "failed": [], "dry_run": False}


def test_a_failing_project_is_listed_and_the_run_goes_on(runner):
    module, state = runner
    state["fail"] = {1}
    result = module.run_message_group_index([1, 2], dry_run=False)
    assert state["created"] == [2]
    assert result == {"changed": [2], "present": [], "failed": [{"project_id": 1}], "dry_run": False}
