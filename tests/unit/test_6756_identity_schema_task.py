"""C-1009 (#6756): agent identity tables - the schema helpers, the startup guard's query and the admin task."""

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
from fixtures import identity  # noqa: E402

PROJECTS = [{"id": 1}, {"id": 2}, {"id": 5}]
USAGE = "project_id= is required. Format: project_id=<all|N>[;dry_run]"


# --- schema helpers (real module, real models, fake sessions) -------------------------------------

@pytest.fixture
def schema(isolated_sys_modules):
    return identity.install(pathlib.Path(PLUGIN_ROOT))


class Session:
    def __init__(self, pid, log, rows=()):
        self.pid, self.log, self.rows = pid, log, rows

    def connection(self):
        return f"connection-{self.pid}"

    def commit(self):
        self.log.append(("commit", self.pid))

    def execute(self, statement, params=None):
        self.log.append(("execute", str(statement), params))
        return types.SimpleNamespace(fetchall=lambda: list(self.rows))


def use_sessions(schema, log, rows=()):
    @contextlib.contextmanager
    def get_session(pid):
        yield Session(pid, log, rows)

    schema.tools.db.get_session = get_session


def record_table_creates(monkeypatch, table_class, log, fail_for=()):
    def create(self, bind, checkfirst=False):
        pid = int(bind.rsplit("-", 1)[1])
        if pid in fail_for:
            raise RuntimeError("boom")
        log.append(("create", pid, self.name, checkfirst))

    monkeypatch.setattr(table_class, "create", create)


def test_both_tables_are_created_per_project_in_one_transaction_and_only_if_missing(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log)
    migrated, failed = schema.schema.apply_identity_tables([4, 9])
    assert (migrated, failed) == ([4, 9], [])
    assert log == [
        ("create", 4, "agent_identity_binding", True), ("create", 4, "execution_security_context", True),
        ("commit", 4),
        ("create", 9, "agent_identity_binding", True), ("create", 9, "execution_security_context", True),
        ("commit", 9),
    ]


def test_dry_run_reports_without_touching_the_database(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log)
    assert schema.schema.apply_identity_tables([4, 9], dry_run=True) == ([4, 9], [])
    assert log == []


def test_one_broken_project_does_not_stop_the_others(schema, monkeypatch):
    log = []
    use_sessions(schema, log)
    record_table_creates(monkeypatch, schema.sa.Table, log, fail_for={2})
    migrated, failed = schema.schema.apply_identity_tables([1, 2, 3])
    assert migrated == [1, 3] and failed == [{"project_id": 2}]
    assert ("commit", 2) not in log and ("commit", 1) in log and ("commit", 3) in log


def test_a_session_that_cannot_open_is_a_failure_of_that_project_only(schema, monkeypatch):
    log = []
    record_table_creates(monkeypatch, schema.sa.Table, log)

    @contextlib.contextmanager
    def get_session(pid):
        if pid == 1:
            raise RuntimeError("no connection")
        yield Session(pid, log)

    schema.tools.db.get_session = get_session
    migrated, failed = schema.schema.apply_identity_tables([1, 2])
    assert migrated == [2] and failed == [{"project_id": 1}]


def test_the_startup_guard_asks_the_catalog_once_and_decodes_project_ids(schema):
    log = []
    use_sessions(schema, log, rows=[("p_3",), ("p_12",)])
    assert schema.schema.project_ids_missing_identity_tables() == [3, 12]
    executes = [entry for entry in log if entry[0] == "execute"]
    assert len(executes) == 1
    _, sql, params = executes[0]
    assert "information_schema.tables" in sql and "p_[0-9]+" in sql
    assert params == {"names": ["agent_identity_binding", "execution_security_context"], "expected": 2}


def test_the_startup_guard_finds_the_schemas_that_still_refuse_a_null_binding_reference(schema):
    log = []
    use_sessions(schema, log, rows=[("p_4",)])
    assert schema.schema.project_ids_with_not_null_binding_ref() == [4]
    [(_, sql, params)] = [entry for entry in log if entry[0] == "execute"]
    assert "information_schema.columns" in sql and "is_nullable = 'NO'" in sql and "p_[0-9]+" in sql
    assert params == {"table": "execution_security_context",
                      "columns": ["agent_binding_ref", "application_id", "version_id"]}   # #7213, #7229


def test_relaxing_the_nullable_columns_is_one_statement_each_per_project_and_a_dry_run_touches_nothing(schema):
    log = []
    use_sessions(schema, log)
    assert schema.schema.apply_nullable_binding_ref([3, 8]) == ([3, 8], [])
    statements = [(pid, str(sql)) for kind, sql, *_ in log if kind == "execute" for pid in [None]]
    # one statement per relaxed column (#7213 binding reference, #7229 application and version) per project
    assert len(statements) == 6 and [sql.split("ALTER COLUMN ")[1] for _, sql in statements[:3]] == [
        "agent_binding_ref DROP NOT NULL", "application_id DROP NOT NULL", "version_id DROP NOT NULL"]
    assert 'ALTER TABLE "p_3".execution_security_context' in statements[0][1]
    assert [entry[0] for entry in log].count("commit") == 2
    log.clear()
    assert schema.schema.apply_nullable_binding_ref([3], dry_run=True) == ([3], [])
    assert log == []


def test_nothing_to_do_means_no_ddl_at_boot(schema, monkeypatch):
    log = []
    use_sessions(schema, log, rows=[])
    record_table_creates(monkeypatch, schema.sa.Table, log)
    assert schema.schema.project_ids_missing_identity_tables() == []
    assert not [entry for entry in log if entry[0] in ("create", "commit")]


def test_the_ddl_matches_the_design(schema):
    sa = schema.sa
    dialect = sa.postgresql.dialect()
    binding = str(sa.CreateTable(schema.models.AgentIdentityBinding.__table__).compile(dialect=dialect))
    context = str(sa.CreateTable(schema.models.ExecutionSecurityContext.__table__).compile(dialect=dialect))
    assert "CONSTRAINT agent_identity_binding_uc UNIQUE (project_id, application_id, version_id, identity_mode)" in binding
    assert "CHECK (state <> 'active' OR (approval_ref IS NOT NULL AND policy_revision IS NOT NULL))" in binding
    assert "CHECK (identity_mode IN ('dedicated_agent', 'runtime_domain_binding', 'elitea_native'))" in binding
    assert "CHECK (state IN ('draft', 'provisioning', 'pending_approval', 'active', 'suspended', 'retiring', 'retired'))" in binding
    assert "state VARCHAR(24) DEFAULT 'draft' NOT NULL" in binding and "revocation_epoch BIGINT" in binding
    assert "UNIQUE (run_id, attempt)" in context
    assert "CHECK (state IN ('admitted', 'running', 'completed', 'revoked'))" in context
    assert "expires_at TIMESTAMP WITH TIME ZONE NOT NULL" in context
    indexes = {
        str(sa.CreateIndex(index).compile(dialect=dialect)).strip()
        for model in (schema.models.AgentIdentityBinding, schema.models.ExecutionSecurityContext)
        for index in model.__table__.indexes
    }
    assert indexes == {
        "CREATE INDEX ix_agent_identity_binding_app_version ON tenant.agent_identity_binding (application_id, version_id)",
        "CREATE INDEX ix_execution_security_context_state_expires ON tenant.execution_security_context (state, expires_at)",
        "CREATE INDEX ix_execution_security_context_binding ON tenant.execution_security_context (agent_binding_ref, state)",
    }


def test_the_context_row_holds_no_secret_material(schema):
    columns = set(schema.models.ExecutionSecurityContext.__table__.columns.keys())
    assert not [name for name in columns if any(word in name for word in ("token", "cookie", "secret", "password", "key"))]


# --- admin task -------------------------------------------------------------------------------

@pytest.fixture
def admin_tasks(isolated_sys_modules):
    sys.modules.update(_make_migration_stubs([]))
    for package in ("elitea_core", "elitea_core.scripts", "elitea_core.utils", "elitea_core.models",
                    "elitea_core.methods"):
        module = types.ModuleType(package)
        module.__path__ = [PLUGIN_ROOT]
        sys.modules.setdefault(package, module)
    fake = types.ModuleType("elitea_core.utils.agent_identity_schema")
    fake.missing = []
    fake.applied = []

    def apply_identity_tables(project_ids, dry_run=False):
        fake.applied.append((list(project_ids), dry_run))
        failed = [{"project_id": pid} for pid in project_ids if pid in fake.failing]
        return [pid for pid in project_ids if pid not in fake.failing], failed

    fake.failing = set()
    fake.project_ids_missing_identity_tables = lambda: list(fake.missing)
    fake.apply_identity_tables = apply_identity_tables
    # the binding reference that must accept NULL (#7213): which projects still refuse it, and what was relaxed
    fake.strict, fake.relaxed = [], []

    def apply_nullable_binding_ref(project_ids, dry_run=False):
        fake.relaxed.append((list(project_ids), dry_run))
        return list(project_ids), []

    fake.project_ids_with_not_null_binding_ref = lambda: list(fake.strict)
    fake.apply_nullable_binding_ref = apply_nullable_binding_ref
    sys.modules[fake.__name__] = fake
    spec = importlib.util.spec_from_file_location(
        "elitea_core.methods.admin_tasks", os.path.join(PLUGIN_ROOT, "methods", "admin_tasks.py"),
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "elitea_core.methods"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.schema_fake = fake
    return module


def run_task(admin_tasks, param, project_list=None, missing=(1, 2, 5)):
    admin_tasks.schema_fake.missing = list(missing)
    calls = []

    def default_list(**kwargs):
        calls.append(kwargs)
        return PROJECTS

    pylon_module = types.SimpleNamespace(
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
            call=types.SimpleNamespace(project_list=project_list or default_list))),
    )
    return admin_tasks.Method.create_agent_identity_tables(pylon_module, param=param), calls


@pytest.mark.parametrize("param", [None, "", "dry_run", "foo=1", "project_id", ";"])
def test_the_project_is_required(admin_tasks, param):
    result, calls = run_task(admin_tasks, param)
    assert result == {"migrated": 0, "error": USAGE}
    assert admin_tasks.schema_fake.applied == [] and calls == []


@pytest.mark.parametrize("param", ["project_id=abc", "project_id=", "project_id=1.5;dry_run"])
def test_a_bad_project_id_is_refused_readably(admin_tasks, param):
    result, _ = run_task(admin_tasks, param)
    assert result["migrated"] == 0 and result["error"].startswith("invalid project_id")
    assert admin_tasks.schema_fake.applied == []


@pytest.mark.parametrize("param", ["project_id=all", "PROJECT_ID=ALL", " project_id=all ;"])
def test_all_creates_the_tables_in_every_created_project_that_lacks_them(admin_tasks, param):
    result, calls = run_task(admin_tasks, param, missing=[1, 5, 77])
    assert admin_tasks.schema_fake.applied == [([1, 5], False)]
    assert calls == [{"filter_": {"create_success": True}}]
    assert result == {"migrated": 2, "failed": 0, "failed_projects": [], "relaxed": 0, "dry_run": False}


@pytest.mark.parametrize("param", ["project_id=5", " project_id=5 ", "dry_run;project_id=5"])
def test_a_named_project_is_the_only_one_touched_and_projects_are_not_listed(admin_tasks, param):
    result, calls = run_task(admin_tasks, param)
    assert admin_tasks.schema_fake.applied == [([5], "dry_run" in param)]
    assert calls == [] and result["migrated"] == 1


def test_a_named_project_that_already_has_the_tables_is_left_alone(admin_tasks):
    result, _ = run_task(admin_tasks, "project_id=5", missing=[1])
    assert admin_tasks.schema_fake.applied == [([], False)]
    assert result["migrated"] == 0


@pytest.mark.parametrize("param", ["project_id=all;dry_run", "dry_run;project_id=all", "project_id=all;DRY_RUN"])
def test_dry_run_is_passed_through(admin_tasks, param):
    result, _ = run_task(admin_tasks, param)
    assert admin_tasks.schema_fake.applied == [([1, 2, 5], True)]
    assert result["dry_run"] is True and result["migrated"] == 3


def test_failures_are_reported_per_project(admin_tasks):
    admin_tasks.schema_fake.failing = {2}
    result, _ = run_task(admin_tasks, "project_id=all")
    assert result == {"migrated": 2, "failed": 1, "failed_projects": [{"project_id": 2}], "relaxed": 0, "dry_run": False}


def test_projects_whose_context_table_refuses_a_null_binding_are_relaxed_and_reported(admin_tasks):
    admin_tasks.schema_fake.strict = [2, 5, 77]   # 77 is not a created project: not touched
    result, _ = run_task(admin_tasks, "project_id=all", missing=[1])
    assert admin_tasks.schema_fake.relaxed == [([2, 5], False)] and result["relaxed"] == 2 and result["migrated"] == 1
    admin_tasks.schema_fake.relaxed.clear()
    result, _ = run_task(admin_tasks, "project_id=5;dry_run", missing=[])
    assert admin_tasks.schema_fake.relaxed == [([5], True)] and result == {
        "migrated": 0, "failed": 0, "failed_projects": [], "relaxed": 1, "dry_run": True}


def test_a_failing_project_list_is_a_readable_error_and_changes_nothing(admin_tasks):
    def boom(**kwargs):
        raise RuntimeError("rpc down")

    result, _ = run_task(admin_tasks, "project_id=all", project_list=boom)
    assert result == {"migrated": 0, "error": "failed to list projects"}
    assert admin_tasks.schema_fake.applied == []
