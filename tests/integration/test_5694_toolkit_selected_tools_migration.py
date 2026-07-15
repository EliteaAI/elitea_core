"""Issue #5694 - support admin backfills that add missing toolkit tools.

Tests the toolkit_migration module which handles parsing migration commands
and applying tool operations (add/remove/rename) to selected_tools lists.

Run via:
    python tests/run_tests.py integration/test_5694_toolkit_selected_tools_migration.py -v
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


@pytest.fixture(scope='module')
def toolkit_migration_module():
    """Load toolkit_migration with minimal stubs."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.models.enums",
        "plugins.elitea_core.utils",
    ):
        module = sys.modules.setdefault(name, types.ModuleType(name))
        module.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = _Log()
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy_orm = types.ModuleType("sqlalchemy.orm")
    sqlalchemy_attrs = types.ModuleType("sqlalchemy.orm.attributes")
    sqlalchemy_attrs.flag_modified = lambda *_args, **_kwargs: None
    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.orm"] = sqlalchemy_orm
    sys.modules["sqlalchemy.orm.attributes"] = sqlalchemy_attrs

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    models_enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    models_enums.AgentTypes = type("AgentTypes", (), {"pipeline": "pipeline"})
    sys.modules["plugins.elitea_core.models.enums.all"] = models_enums

    elitea_tools = types.ModuleType("plugins.elitea_core.models.elitea_tools")
    elitea_tools.EliteATool = type("EliteATool", (), {})
    elitea_tools.EntityToolMapping = type("EntityToolMapping", (), {})
    sys.modules["plugins.elitea_core.models.elitea_tools"] = elitea_tools

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.toolkit_migration",
        PLUGIN_ROOT / "utils" / "toolkit_migration.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestParseMigrationArgs:
    """Tests for parse_migration_args function."""

    def test_parse_migration_args_supports_add_operation(self, toolkit_migration_module):
        """Add operation should be parsed correctly with all options."""
        parsed = toolkit_migration_module.parse_migration_args("inventory;+ask;project_id=all;dry_run")

        assert parsed == {
            "toolkit_type": "inventory",
            "operations": [{"action": "add", "tool_name": "ask"}],
            "project_id": "all",
            "dry_run": True,
        }


class TestApplyOperations:
    """Tests for apply_operations_to_selected_tools function."""

    def test_apply_operations_to_selected_tools_adds_missing_tool_once(self, toolkit_migration_module):
        """Add operation should add tool only if not present, idempotent on re-run."""
        updated, changes, added_count, removed_count, renamed_count = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                ["run_ingestion", "search_entities"],
                [{"action": "add", "tool_name": "ask"}],
            )
        )

        assert updated == ["run_ingestion", "search_entities", "ask"]
        assert changes == ["added 'ask'"]
        assert added_count == 1
        assert removed_count == 0
        assert renamed_count == 0

        updated_again, changes_again, added_again, removed_again, renamed_again = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                updated,
                [{"action": "add", "tool_name": "ask"}],
            )
        )

        assert updated_again == updated
        assert changes_again == []
        assert added_again == 0
        assert removed_again == 0
        assert renamed_again == 0

    def test_apply_operations_to_selected_tools_handles_mixed_add_remove_rename(self, toolkit_migration_module):
        """Mixed operations (add, remove, rename) should all be applied correctly."""
        updated, changes, added_count, removed_count, renamed_count = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                ["run_ingestion", "delta_update", "old_tool"],
                [
                    {"action": "add", "tool_name": "ask"},
                    {"action": "remove", "tool_name": "delta_update"},
                    {"action": "rename", "old_name": "old_tool", "new_name": "new_tool"},
                ],
            )
        )

        assert updated == ["run_ingestion", "ask", "new_tool"]
        assert changes == [
            "added 'ask'",
            "removed 'delta_update'",
            "renamed 'old_tool' -> 'new_tool'",
        ]
        assert added_count == 1
        assert removed_count == 1
        assert renamed_count == 1
