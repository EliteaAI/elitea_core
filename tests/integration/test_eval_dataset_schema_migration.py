"""Issue #6350 - eval dataset scoping schema migration.

Tests utils/eval_dataset_schema.py, the admin task that backfills the agent_id/is_shared
columns (plus FK + index) onto existing project schemas. Uses a fake session (no live
Postgres) so the test exercises the DDL statement shape and re-runnability directly.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

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


class _FakeSession:
    def __init__(self):
        self.executed = []
        self.orphans = []
        self.committed = self.rolled_back = 0

    def execute(self, clause):
        sql = str(clause)
        self.executed.append(sql)
        if "NOT EXISTS (SELECT 1" in sql:
            return [(dataset_id,) for dataset_id in self.orphans]
        return []

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


class _FakeSessionCtx:
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, *_exc_info):
        return False


@pytest.fixture
def eval_dataset_schema_module():
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = _Log()
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules["pylon.core.tools"] = tools_mod

    session = _FakeSession()
    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace(
        with_project_schema_session=lambda pid: _FakeSessionCtx(session)
    )
    sys.modules["tools"] = tools_pkg
    sys.modules["tools.db"] = tools_pkg.db

    spec = importlib.util.spec_from_file_location(
        "eval_dataset_schema",
        PLUGIN_ROOT / "utils" / "eval_dataset_schema.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module._test_session = session
    return module


def test_applies_all_statements_for_a_project(eval_dataset_schema_module):
    migrated, failed, orphans = eval_dataset_schema_module.apply_eval_dataset_columns([7])

    assert migrated == [7]
    assert failed == []
    assert orphans == []
    executed = eval_dataset_schema_module._test_session.executed
    assert any("agent_id" in sql and "ADD COLUMN IF NOT EXISTS" in sql for sql in executed)
    assert any("is_shared" in sql and "ADD COLUMN IF NOT EXISTS" in sql for sql in executed)
    assert any("FOREIGN KEY" in sql for sql in executed)
    assert any("CREATE INDEX IF NOT EXISTS" in sql for sql in executed)
    assert any("time_limit_seconds INTEGER" in sql and "ADD COLUMN IF NOT EXISTS" in sql
               for sql in executed)


def test_migration_statements_are_safe_to_run_twice(eval_dataset_schema_module):
    """Every statement guards against re-running: IF NOT EXISTS, a caught duplicate_object, or an
    existence check."""
    for statement in eval_dataset_schema_module._MIGRATION_STATEMENTS:
        assert (
            "IF NOT EXISTS" in statement
            or "EXCEPTION WHEN duplicate_object" in statement
            or "IS NOT NULL THEN" in statement
        ), f"statement is not idempotent: {statement}"


def test_agent_id_index_uses_the_create_all_name(eval_dataset_schema_module):
    """The index must match create_all's name, or a fresh schema ends up with two identical indexes."""
    creates = [s for s in eval_dataset_schema_module._MIGRATION_STATEMENTS if "CREATE INDEX" in s]
    assert creates == [
        "CREATE INDEX IF NOT EXISTS ix_tenant_eval_dataset_agent_id ON p_{pid}.eval_dataset (agent_id)"
    ]
    legacy = [s for s in eval_dataset_schema_module._MIGRATION_STATEMENTS if "eval_dataset_agent_id_idx" in s]
    assert len(legacy) == 1
    assert "RENAME TO ix_tenant_eval_dataset_agent_id" in legacy[0]
    assert "DROP INDEX p_{pid}.eval_dataset_agent_id_idx" in legacy[0]


def test_a_failing_project_is_reported_without_aborting_the_batch(eval_dataset_schema_module, monkeypatch):
    def _boom(pid):
        raise RuntimeError("boom")

    import tools
    monkeypatch.setattr(tools.db, "with_project_schema_session", _boom)

    migrated, failed, _ = eval_dataset_schema_module.apply_eval_dataset_columns([1, 2])

    assert migrated == []
    assert failed == [{"project_id": 1}, {"project_id": 2}]


def test_orphaned_datasets_are_deleted_before_the_fk_is_added(eval_dataset_schema_module):
    session = eval_dataset_schema_module._test_session
    session.orphans = [64, 12]

    migrated, failed, orphans = eval_dataset_schema_module.apply_eval_dataset_columns([2])

    assert (migrated, failed) == ([2], [])
    assert orphans == [{"project_id": 2, "dataset_ids": [12, 64]}]
    delete_at = next(i for i, sql in enumerate(session.executed) if sql.startswith("DELETE FROM p_2.eval_dataset"))
    fk_at = next(i for i, sql in enumerate(session.executed) if "FOREIGN KEY" in sql)
    assert delete_at < fk_at
    assert session.committed == 1


def test_a_dry_run_reports_orphans_and_changes_nothing(eval_dataset_schema_module):
    session = eval_dataset_schema_module._test_session
    session.orphans = [64]

    migrated, failed, orphans = eval_dataset_schema_module.apply_eval_dataset_columns([2], dry_run=True)

    assert (migrated, failed) == ([2], [])
    assert orphans == [{"project_id": 2, "dataset_ids": [64]}]
    assert not any(sql.startswith("DELETE") for sql in session.executed)
    assert not any("FOREIGN KEY" in sql or "CREATE INDEX" in sql for sql in session.executed)
    assert (session.committed, session.rolled_back) == (0, 1)
