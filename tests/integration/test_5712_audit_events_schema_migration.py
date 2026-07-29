"""Issue #5712 - auto-migrate the shared audit_events table at boot.

Tests utils/audit_events_schema.py, the compatibility guard that applies the
ADR-0008 token/cost columns (and index) to an existing audit_events table
without an operator having to run a migration by hand. Uses a fake
connection/engine (no live Postgres) so the test exercises the DDL statement
shape and idempotency directly.

Run via:
    python tests/run_tests.py integration/test_5712_audit_events_schema_migration.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest
from sqlalchemy import text


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _Log:
    @staticmethod
    def info(*_args, **_kwargs): pass
    @staticmethod
    def debug(*_args, **_kwargs): pass
    @staticmethod
    def warning(*_args, **_kwargs): pass
    @staticmethod
    def error(*_args, **_kwargs): pass
    @staticmethod
    def exception(*_args, **_kwargs): pass


@pytest.fixture(scope='module')
def audit_events_schema_module():
    """Load audit_events_schema with minimal stubs (no live DB)."""
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = _Log()
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.config = types.SimpleNamespace(POSTGRES_SCHEMA="centry")
    sys.modules["tools"] = tools_pkg
    sys.modules["tools.config"] = tools_pkg.config

    spec = importlib.util.spec_from_file_location(
        "audit_events_schema",
        PLUGIN_ROOT / "utils" / "audit_events_schema.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)

    def __iter__(self):
        return iter(self._values)


class FakeIdentifierPreparer:
    @staticmethod
    def quote(name):
        return f'"{name}"'


class FakeDialect:
    identifier_preparer = FakeIdentifierPreparer()


class FakeConnection:
    """Records executed statements; simulates catalog state for one schema."""

    def __init__(self, columns, indexes):
        self.columns = set(columns)
        self.indexes = set(indexes)
        self.executed = []
        self.dialect = FakeDialect()

    def execute(self, clause, params=None):
        sql = str(clause)
        self.executed.append(sql)

        if "FROM information_schema.columns" in sql:
            return FakeResult(self.columns)
        if "FROM pg_indexes" in sql:
            return FakeResult(self.indexes)
        if "ALTER TABLE" in sql:
            for name in ("input_tokens", "output_tokens", "llm_cost"):
                if f'"{name}"' in sql:
                    self.columns.add(name)
        if "CREATE INDEX" in sql:
            self.indexes.add("ix_audit_events_model_name")
        return FakeResult([])


class FakeEngine:
    def __init__(self, connection):
        self._connection = connection

    def begin(self):
        return self

    def connect(self):
        return self

    def __enter__(self):
        return self._connection

    def __exit__(self, *_exc_info):
        return False


class TestEnsureAuditEventsSchema:
    def test_missing_table_is_a_noop(self, audit_events_schema_module):
        """No audit_events table yet (fresh deploy) - create_all will provision it."""
        connection = FakeConnection(columns=[], indexes=[])
        engine = FakeEngine(connection)

        result = audit_events_schema_module.ensure_audit_events_schema(engine)

        assert result == {"table_present": False, "added_columns": [], "added_indexes": []}
        assert not any("ALTER TABLE" in sql for sql in connection.executed)

    def test_adds_missing_columns_and_index(self, audit_events_schema_module):
        """Pre-ADR-0008 table gets the new columns and index added."""
        connection = FakeConnection(columns=["id", "timestamp", "model_name"], indexes=[])
        engine = FakeEngine(connection)

        result = audit_events_schema_module.ensure_audit_events_schema(engine)

        assert sorted(result["added_columns"]) == ["input_tokens", "llm_cost", "output_tokens"]
        assert result["added_indexes"] == ["ix_audit_events_model_name"]

        alter_statements = [sql for sql in connection.executed if "ALTER TABLE" in sql]
        assert len(alter_statements) == 1
        assert '"input_tokens" INTEGER' in alter_statements[0]
        assert '"output_tokens" INTEGER' in alter_statements[0]
        assert '"llm_cost" NUMERIC(18, 8)' in alter_statements[0]
        assert any("CREATE INDEX" in sql for sql in connection.executed)

        # Takes the advisory lock before touching the catalog.
        assert any("pg_advisory_xact_lock" in sql for sql in connection.executed)

    def test_is_idempotent_once_columns_present(self, audit_events_schema_module):
        """A second run against an already-migrated table issues no DDL."""
        connection = FakeConnection(
            columns=["id", "timestamp", "model_name", "input_tokens", "output_tokens", "llm_cost"],
            indexes=["ix_audit_events_model_name"],
        )
        engine = FakeEngine(connection)

        result = audit_events_schema_module.ensure_audit_events_schema(engine)

        assert result == {"table_present": True, "added_columns": [], "added_indexes": []}
        assert not any("ALTER TABLE" in sql for sql in connection.executed)
        assert not any("CREATE INDEX" in sql for sql in connection.executed)

    def test_dry_run_reports_without_mutating(self, audit_events_schema_module):
        """dry_run=True reports what would change and takes no lock, no DDL."""
        connection = FakeConnection(columns=["id", "timestamp", "model_name"], indexes=[])
        engine = FakeEngine(connection)

        result = audit_events_schema_module.ensure_audit_events_schema(engine, dry_run=True)

        assert sorted(result["added_columns"]) == ["input_tokens", "llm_cost", "output_tokens"]
        assert result["added_indexes"] == ["ix_audit_events_model_name"]
        assert not any("ALTER TABLE" in sql for sql in connection.executed)
        assert not any("CREATE INDEX" in sql for sql in connection.executed)
        assert not any("pg_advisory_xact_lock" in sql for sql in connection.executed)

        # Second dry run against the (untouched) fake reports the same gap again.
        second = audit_events_schema_module.ensure_audit_events_schema(engine, dry_run=True)
        assert second == result
