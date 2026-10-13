"""Issue #6716 - eval_case_usage.cost precision migration.

Tests utils/eval_case_usage_schema.py, the admin task that widens cost from NUMERIC(18,8) to
NUMERIC(18,9) on existing project schemas, so the nano-USD ledger figure keeps its last digit.
Uses a fake session (no live Postgres), as test_eval_dataset_schema_migration.py does.
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
def eval_case_usage_schema_module():
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
        "eval_case_usage_schema",
        PLUGIN_ROOT / "utils" / "eval_case_usage_schema.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module._test_session = session
    return module


def test_widens_cost_to_nine_decimal_places(eval_case_usage_schema_module):
    migrated, failed = eval_case_usage_schema_module.apply_eval_case_usage_columns([7])

    assert (migrated, failed) == ([7], [])
    [sql] = eval_case_usage_schema_module._test_session.executed
    assert "ALTER TABLE p_7.eval_case_usage ALTER COLUMN cost TYPE NUMERIC(18,9)" in sql


def test_the_alter_is_guarded_so_it_is_safe_to_run_twice(eval_case_usage_schema_module):
    """Only a column whose scale is not 9 is altered, and only where the table exists."""
    for statement in eval_case_usage_schema_module._MIGRATION_STATEMENTS:
        assert "information_schema.columns" in statement
        assert "numeric_scale IS DISTINCT FROM 9" in statement
        assert "table_schema = 'p_{pid}'" in statement


def test_the_model_matches_the_migration():
    """A fresh schema from create_all and a migrated one end up with the same column type."""
    model = (PLUGIN_ROOT / "models" / "evaluation.py").read_text()
    assert "cost = mapped_column(Numeric(18, 9), nullable=True)" in model


def test_a_failing_project_is_reported_without_aborting_the_batch(eval_case_usage_schema_module, monkeypatch):
    def _boom(pid):
        raise RuntimeError("boom")

    import tools
    monkeypatch.setattr(tools.db, "with_project_schema_session", _boom)

    migrated, failed = eval_case_usage_schema_module.apply_eval_case_usage_columns([1, 2])

    assert migrated == []
    assert failed == [{"project_id": 1}, {"project_id": 2}]
