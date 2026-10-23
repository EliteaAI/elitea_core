"""C-1009 (#6947): the mandate table's rollout (schema helpers, startup guard, ``create_mandate_tables``), the thin
admin tasks around the mandate and report modules, and their registration in ``module.py``."""

import contextlib
import importlib.util
import os
import pathlib
import sys
import types

import pytest
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from test_6724_chat_templates import PLUGIN_ROOT, _make_migration_stubs  # noqa: E402  pylint: disable=C0413
from test_6756_identity_schema_task import Session, record_table_creates, use_sessions  # noqa: E402  pylint: disable=C0413
from fixtures import identity  # noqa: E402

PROJECTS = [{"id": 1}, {"id": 2}, {"id": 5}]
USAGE = "project_id= is required. Format: project_id=<all|N>[;dry_run]"
REPORT_USAGE = "project_id= is required. Format: project_id=<all|N>[;principal=<legacy_system|delegation|mandate>]"


def source_of(relative):
    return (pathlib.Path(PLUGIN_ROOT) / relative).read_text()


# --- schema helpers (real module, real models, fake sessions) -------------------------------------

@pytest.fixture
def schema(isolated_sys_modules):
    return identity.install(pathlib.Path(PLUGIN_ROOT))


def test_only_the_mandate_table_is_created_per_project_in_one_transaction_and_only_if_missing(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log)
    assert schema.schema.apply_mandate_tables([4, 9]) == ([4, 9], [])
    assert log == [
        ("create", 4, "autonomous_mandate", True), ("commit", 4),
        ("create", 9, "autonomous_mandate", True), ("commit", 9),
    ]


def test_the_identity_task_still_creates_the_identity_tables_only(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log)
    schema.schema.apply_identity_tables([4])
    assert [entry[2] for entry in log if entry[0] == "create"] == ["agent_identity_binding", "execution_security_context"]


def test_the_startup_guard_covers_both_sets_with_one_catalog_query_and_one_transaction(schema, monkeypatch):
    log = []
    use_sessions(schema, log, rows=[("p_3",)])
    tables = schema.schema.IDENTITY_TABLES + schema.schema.MANDATE_TABLES
    assert schema.schema.project_ids_missing_tables(tables) == [3]
    [(_, sql, params)] = [entry for entry in log if entry[0] == "execute"]
    assert "information_schema.tables" in sql and "p_[0-9]+" in sql
    assert params == {"names": ["agent_identity_binding", "execution_security_context", "autonomous_mandate"],
                      "expected": 3}
    record_table_creates(monkeypatch, schema.sa.Table, log)
    assert schema.schema.apply_tables([3], tables) == ([3], [])
    assert [entry[2] for entry in log if entry[0] == "create"] == [
        "agent_identity_binding", "execution_security_context", "autonomous_mandate"]


def test_the_mandate_catalog_query_asks_for_its_table_only(schema):
    log = []
    use_sessions(schema, log, rows=[("p_3",), ("p_12",)])
    assert schema.schema.project_ids_missing_mandate_tables() == [3, 12]
    [(_, _, params)] = [entry for entry in log if entry[0] == "execute"]
    assert params == {"names": ["autonomous_mandate"], "expected": 1}


def test_a_steady_boot_issues_no_ddl(schema, monkeypatch):
    log = []
    use_sessions(schema, log, rows=[])
    record_table_creates(monkeypatch, schema.sa.Table, log)
    assert schema.schema.project_ids_missing_tables(schema.schema.IDENTITY_TABLES + schema.schema.MANDATE_TABLES) == []
    assert not [entry for entry in log if entry[0] in ("create", "commit")]


def test_dry_run_and_isolation_apply_to_the_mandate_table_too(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log, fail_for={2})
    assert schema.schema.apply_mandate_tables([1, 2], dry_run=True) == ([1, 2], [])
    assert log == []
    migrated, failed = schema.schema.apply_mandate_tables([1, 2, 3])
    assert migrated == [1, 3] and failed == [{"project_id": 2}]
    assert ("commit", 2) not in log


def test_the_model_is_registered_for_create_all():
    assert "AutonomousMandate" in source_of("models/all.py")


# --- admin tasks ---------------------------------------------------------------------------------

@pytest.fixture
def admin_tasks(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                    "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [PLUGIN_ROOT]
        sys.modules.setdefault(package, module)
    fake = types.ModuleType("elitea_core.utils.agent_identity_schema")
    fake.missing, fake.applied, fake.failing = [], [], set()

    def apply_mandate_tables(project_ids, dry_run=False):
        fake.applied.append((list(project_ids), dry_run))
        return ([pid for pid in project_ids if pid not in fake.failing],
                [{"project_id": pid} for pid in project_ids if pid in fake.failing])

    fake.project_ids_missing_mandate_tables = lambda: list(fake.missing)
    fake.apply_mandate_tables = apply_mandate_tables
    sys.modules[fake.__name__] = fake

    admin = types.ModuleType("elitea_core.utils.mandate_admin")
    admin.calls = []
    admin.manage_mandate = lambda module, param, actor: admin.calls.append((module, param, actor)) or {
        "action": "create", "mandate_ref": "m-1", "state": "draft", "dry_run": False}
    sys.modules[admin.__name__] = admin

    report = types.ModuleType("elitea_core.utils.trigger_report")
    report.PRINCIPALS = ("legacy_system", "delegation", "mandate")
    report.calls, report.rows, report.failed = [], [], []
    report.build_trigger_report = lambda project_ids, principal=None: report.calls.append((list(project_ids), principal)) or (
        list(report.rows), list(report.failed))
    sys.modules[report.__name__] = report

    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"))
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.schema_fake, module.admin_fake, module.report_fake = fake, admin, report
    return module


def pylon_module(project_list=None):
    calls = []

    def default_list(**kwargs):
        calls.append(kwargs)
        return PROJECTS

    return types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
        call=types.SimpleNamespace(project_list=project_list or default_list)))), calls


def run_create(admin_tasks, param, project_list=None, missing=(1, 2, 5)):
    admin_tasks.schema_fake.missing = list(missing)
    module, calls = pylon_module(project_list)
    return admin_tasks.Method.create_mandate_tables(module, param=param), calls


@pytest.mark.parametrize("param", [None, "", "dry_run", "foo=1", "project_id", ";"])
def test_the_project_is_required(admin_tasks, param):
    result, calls = run_create(admin_tasks, param)
    assert result == {"migrated": 0, "error": USAGE}
    assert admin_tasks.schema_fake.applied == [] and calls == []


@pytest.mark.parametrize("param", ["project_id=abc", "project_id=", "project_id=1.5;dry_run"])
def test_a_bad_project_id_is_refused_readably(admin_tasks, param):
    result, _ = run_create(admin_tasks, param)
    assert result["migrated"] == 0 and result["error"].startswith("invalid project_id")
    assert admin_tasks.schema_fake.applied == []


@pytest.mark.parametrize("param", ["project_id=all", "PROJECT_ID=ALL", " project_id=all ;"])
def test_all_creates_the_table_in_every_created_project_that_lacks_it(admin_tasks, param):
    result, calls = run_create(admin_tasks, param, missing=[1, 5, 77])
    assert admin_tasks.schema_fake.applied == [([1, 5], False)]
    assert calls == [{"filter_": {"create_success": True}}]
    assert result == {"migrated": 2, "failed": 0, "failed_projects": [], "dry_run": False}


@pytest.mark.parametrize("param", ["project_id=5", " project_id=5 ", "dry_run;project_id=5"])
def test_a_named_project_is_the_only_one_touched_and_projects_are_not_listed(admin_tasks, param):
    result, calls = run_create(admin_tasks, param)
    assert admin_tasks.schema_fake.applied == [([5], "dry_run" in param)]
    assert calls == [] and result["migrated"] == 1


@pytest.mark.parametrize("param", ["project_id=all;dry_run", "dry_run;project_id=all", "project_id=all;DRY_RUN"])
def test_dry_run_is_passed_through(admin_tasks, param):
    result, _ = run_create(admin_tasks, param)
    assert admin_tasks.schema_fake.applied == [([1, 2, 5], True)]
    assert result["dry_run"] is True and result["migrated"] == 3


def test_failures_are_reported_per_project(admin_tasks):
    admin_tasks.schema_fake.failing = {2}
    result, _ = run_create(admin_tasks, "project_id=all")
    assert result == {"migrated": 2, "failed": 1, "failed_projects": [{"project_id": 2}], "dry_run": False}


def test_a_failing_project_list_is_a_readable_error_and_changes_nothing(admin_tasks):
    def boom(**kwargs):
        raise RuntimeError("rpc down")

    result, _ = run_create(admin_tasks, "project_id=all", project_list=boom)
    assert result == {"migrated": 0, "error": "failed to list projects"}
    assert admin_tasks.schema_fake.applied == []


def test_the_mandate_task_hands_the_param_and_the_operator_to_the_module_and_returns_its_result(admin_tasks):
    module, _ = pylon_module()
    result = admin_tasks.Method.manage_autonomous_mandate(module, param="project_id=1;action=create", _executing_user_id=7)
    assert admin_tasks.admin_fake.calls == [(module, "project_id=1;action=create", 7)]
    assert result["mandate_ref"] == "m-1"
    admin_tasks.Method.manage_autonomous_mandate(module)
    assert admin_tasks.admin_fake.calls[-1] == (module, "", 0)


@pytest.mark.parametrize("param", [None, "", "principal=mandate", "dry_run"])
def test_the_report_needs_a_project(admin_tasks, param):
    module, calls = pylon_module()
    assert admin_tasks.Method.report_trigger_principals(module, param=param) == {"error": REPORT_USAGE}
    assert admin_tasks.report_fake.calls == [] and calls == []


def test_the_report_refuses_an_unknown_principal(admin_tasks):
    module, _ = pylon_module()
    result = admin_tasks.Method.report_trigger_principals(module, param="project_id=all;principal=root")
    assert result == {"error": "invalid principal: 'root'"} and admin_tasks.report_fake.calls == []


def test_the_report_of_all_projects_and_a_filter(admin_tasks):
    admin_tasks.report_fake.rows = [{"governed_outcome": "MANDATE_REQUIRED"}, {"governed_outcome": "ADMISSIBLE"},
                                    {"governed_outcome": "MANDATE_REQUIRED"}]
    admin_tasks.report_fake.failed = [2]
    module, calls = pylon_module()
    result = admin_tasks.Method.report_trigger_principals(module, param="project_id=all;principal=legacy_system")
    assert admin_tasks.report_fake.calls == [([1, 2, 5], "legacy_system")] and calls
    assert result == {"count": 3, "outcomes": {"MANDATE_REQUIRED": 2, "ADMISSIBLE": 1}, "failed": 1,
                      "failed_projects": [2], "rows": admin_tasks.report_fake.rows}


def test_the_report_of_one_project_does_not_list_projects(admin_tasks):
    module, calls = pylon_module()
    result = admin_tasks.Method.report_trigger_principals(module, param="project_id=5")
    assert admin_tasks.report_fake.calls == [([5], None)] and calls == []
    assert result["count"] == 0


def test_the_report_logs_counts_never_rows(admin_tasks):
    admin_tasks.report_fake.rows = [{"governed_outcome": "ADMISSIBLE", "mandate_ref": "m-secret-shaped"}]
    lines = []
    admin_tasks.log = types.SimpleNamespace(
        info=lambda message, *args: lines.append(message % args), error=lambda *a: None, exception=lambda *a, **k: None,
        warning=lambda *a, **k: None)
    module, _ = pylon_module()
    admin_tasks.Method.report_trigger_principals(module, param="project_id=1")
    assert lines == ["report_trigger_principals: rows=1 failed=0 outcomes={'ADMISSIBLE': 1}"]


def test_a_failing_project_list_is_a_readable_error_for_the_report(admin_tasks):
    def boom(**kwargs):
        raise RuntimeError("rpc down")

    module, _ = pylon_module(boom)
    assert admin_tasks.Method.report_trigger_principals(module, param="project_id=all") == {
        "error": "failed to list projects"}


# --- registration -------------------------------------------------------------------------------

@pytest.mark.parametrize("task", ["create_mandate_tables", "manage_autonomous_mandate", "report_trigger_principals"])
def test_module_registers_and_unregisters_the_admin_tasks(task):
    text = source_of("module.py")
    for call in ("register_admin_task", "unregister_admin_task"):
        assert f'{call}(\n                "{task}", self.{task}' in text
    assert f'"{task}", self.{task},\n                group="R-2.0.8"' in text


def test_module_registers_the_mandate_permissions_whatever_the_governance_flag_says():
    text = source_of("module.py")
    for permission in ("manage", "approve", "view"):
        assert f'"governance.mandates.{permission}"' in text
    assert text.index('"governance.mandates.manage"') < text.index("self.task_node.start()")
    assert text.index("init_governance(self)") < text.index('"governance.mandates.manage"')
    block = text[text.index('"governance.mandates.manage"') - 400:text.index('"governance.mandates.view"') + 600]
    for permission in ("manage", "approve", "view"):
        assert f'("governance.mandates.{permission}", {{"admin": True, "viewer": False, "editor": False}})' in block
    assert "agent_identity.mandates" not in text
    assert 'if is_governed' not in text[text.index('"governance.mandates.manage"') - 400:
                                        text.index('"governance.mandates.view"') + 600]


def test_the_startup_guard_checks_both_table_sets():
    text = source_of("module.py")
    guard = text[text.index("def _ensure_agent_identity_schema"):text.index("def init(self):")]
    assert "tables = IDENTITY_TABLES + MANDATE_TABLES" in guard
    assert "project_ids_missing_tables(tables)" in guard and "apply_tables(pending, tables)" in guard


def test_the_migration_note_points_at_the_task():
    text = source_of("db_migrations.txt")
    assert "Run admin task create_mandate_tables with param: project_id=all;dry_run" in text
    assert "Run admin task create_mandate_tables with param: project_id=all\n" in text + "\n"


def test_the_method_is_registered_under_its_design_name():
    text = source_of("methods/trigger_report.py")
    assert '@web.method("autonomous_trigger_report")' in text
    assert "rows, failed = build_trigger_report(project_ids, principal)" in text
    assert 'return {"rows": rows, "failed": failed}' in text


def test_the_tracing_model_is_found_by_walking_up_from_a_worktree_or_a_plain_checkout(tmp_path):
    from fixtures import admission_env  # pylint: disable=C0415
    model = tmp_path / "plugins" / "tracing" / "models" / "security_audit_event.py"
    model.parent.mkdir(parents=True)
    model.write_text("")
    plain = tmp_path / "plugins" / "elitea_core"
    worktree = plain / "wt" / "elitea-core-1"
    worktree.mkdir(parents=True)
    assert admission_env.find_tracing_event_model(plain) == model
    assert admission_env.find_tracing_event_model(worktree) == model
    assert admission_env.find_tracing_event_model(tmp_path / "plugins" / "elsewhere") == model
    model.unlink()
    assert admission_env.find_tracing_event_model(worktree) is None
