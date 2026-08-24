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

    def execute(self, clause):
        self.executed.append(str(clause))

    def commit(self):
        pass


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
    migrated, failed = eval_dataset_schema_module.apply_eval_dataset_columns([7])

    assert migrated == [7]
    assert failed == []
    executed = eval_dataset_schema_module._test_session.executed
    assert any("agent_id" in sql and "ADD COLUMN IF NOT EXISTS" in sql for sql in executed)
    assert any("is_shared" in sql and "ADD COLUMN IF NOT EXISTS" in sql for sql in executed)
    assert any("FOREIGN KEY" in sql for sql in executed)
    assert any("CREATE INDEX IF NOT EXISTS" in sql for sql in executed)


def test_migration_statements_are_safe_to_run_twice(eval_dataset_schema_module):
    """Every statement guards against re-running: IF NOT EXISTS, or a caught duplicate_object."""
    for statement in eval_dataset_schema_module._MIGRATION_STATEMENTS:
        assert (
            "IF NOT EXISTS" in statement
            or "EXCEPTION WHEN duplicate_object" in statement
        ), f"statement is not idempotent: {statement}"


def test_a_failing_project_is_reported_without_aborting_the_batch(eval_dataset_schema_module, monkeypatch):
    def _boom(pid):
        raise RuntimeError("boom")

    import tools
    monkeypatch.setattr(tools.db, "with_project_schema_session", _boom)

    migrated, failed = eval_dataset_schema_module.apply_eval_dataset_columns([1, 2])

    assert migrated == []
    assert failed == [{"project_id": 1}, {"project_id": 2}]
