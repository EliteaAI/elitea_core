"""Rename `list_collections` -> `list_indexes` migration coverage.

Verifies the reusable rename engine used by the admin task
`migrate_list_collections_to_list_indexes` behaves correctly:
- pure rename semantics on selected_tools
- idempotence on re-run
- pipeline YAML word-boundary rename with per-op skip guard when the new
  name already appears (mixed-state safety)

Uses the same importlib-based stub scaffolding as
`test_5694_toolkit_selected_tools_migration.py`.

Run via:
    python tests/run_tests.py integration/test_list_collections_to_list_indexes_rename.py -v
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
    """Load toolkit_migration with minimal stubs (mirrors 5694 scaffolding)."""
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


RENAME_OPS = [{"action": "rename", "old_name": "list_collections", "new_name": "list_indexes"}]


class TestSelectedToolsRename:
    """Rename engine applied to selected_tools lists."""

    def test_renames_list_collections_in_selected_tools(self, toolkit_migration_module):
        updated, changes, added, removed, renamed = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                ["index_data", "list_collections", "search_index"],
                RENAME_OPS,
            )
        )

        assert updated == ["index_data", "search_index", "list_indexes"]
        assert changes == ["renamed 'list_collections' -> 'list_indexes'"]
        assert (added, removed, renamed) == (0, 0, 1)

    def test_rename_is_idempotent(self, toolkit_migration_module):
        first, _, _, _, _ = toolkit_migration_module.apply_operations_to_selected_tools(
            ["index_data", "list_collections", "search_index"], RENAME_OPS,
        )
        second, changes, added, removed, renamed = (
            toolkit_migration_module.apply_operations_to_selected_tools(first, RENAME_OPS)
        )

        assert second == first
        assert changes == []
        assert (added, removed, renamed) == (0, 0, 0)

    def test_mixed_state_drops_duplicate_old_name(self, toolkit_migration_module):
        # Some entries were partially migrated: both old and new coexist. The
        # engine must drop the old entry rather than end up with duplicates.
        updated, changes, _added, _removed, renamed = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                ["index_data", "list_collections", "list_indexes"], RENAME_OPS,
            )
        )

        assert updated == ["index_data", "list_indexes"]
        assert renamed == 1
        assert changes == [
            "renamed 'list_collections' -> 'list_indexes' (target already present, removed old)"
        ]

    def test_no_op_when_old_name_absent(self, toolkit_migration_module):
        updated, changes, added, removed, renamed = (
            toolkit_migration_module.apply_operations_to_selected_tools(
                ["index_data", "search_index"], RENAME_OPS,
            )
        )

        assert updated == ["index_data", "search_index"]
        assert changes == []
        assert (added, removed, renamed) == (0, 0, 0)


class TestPipelineInstructionsRename:
    """Word-boundary rename applied to pipeline YAML instructions text."""

    def test_renames_word_bounded_tool_reference(self, toolkit_migration_module):
        yaml_text = (
            "nodes:\n"
            "  - id: fetch\n"
            "    tool: list_collections\n"
            "  - id: rank\n"
            "    tool: search_index\n"
        )
        new_text, changes, skipped = toolkit_migration_module.apply_rename_to_instructions(
            yaml_text, RENAME_OPS,
        )

        assert "list_collections" not in new_text
        assert "list_indexes" in new_text
        assert changes == ["renamed 'list_collections' -> 'list_indexes' (1 occurrences)"]
        assert skipped == []

    def test_pipeline_rename_is_idempotent(self, toolkit_migration_module):
        yaml_text = "tool: list_collections\n"
        once, _, _ = toolkit_migration_module.apply_rename_to_instructions(yaml_text, RENAME_OPS)
        twice, changes, skipped = toolkit_migration_module.apply_rename_to_instructions(
            once, RENAME_OPS,
        )

        assert twice == once
        assert changes == []
        # New name already present -> operation guarded and skipped.
        assert skipped == ["SKIP 'list_collections' -> 'list_indexes': new name already present"]

    def test_mixed_state_pipeline_is_skipped_not_corrupted(self, toolkit_migration_module):
        # Partially-migrated pipeline: both names appear. The per-op guard
        # skips the whole rename to avoid touching a mixed file until the
        # operator can inspect it.
        yaml_text = (
            "nodes:\n"
            "  - id: a\n"
            "    tool: list_collections\n"
            "  - id: b\n"
            "    tool: list_indexes\n"
        )
        new_text, changes, skipped = toolkit_migration_module.apply_rename_to_instructions(
            yaml_text, RENAME_OPS,
        )

        assert new_text == yaml_text
        assert changes == []
        assert skipped == ["SKIP 'list_collections' -> 'list_indexes': new name already present"]

    def test_word_boundary_prevents_substring_matches(self, toolkit_migration_module):
        # Custom user code containing `my_list_collections_helper` must not be renamed.
        yaml_text = "tool: my_list_collections_helper\n"
        new_text, changes, skipped = toolkit_migration_module.apply_rename_to_instructions(
            yaml_text, RENAME_OPS,
        )

        assert new_text == yaml_text
        assert changes == []
        assert skipped == []


class TestMigrationParamAssembly:
    """The admin method builds `<type>;list_collections>list_indexes;project_id=<N>[;dry_run]` per type. Verify the engine parses it back correctly."""

    def test_param_string_parses_for_all_operation_variants(self, toolkit_migration_module):
        parsed = toolkit_migration_module.parse_migration_args(
            "indexer;list_collections>list_indexes;project_id=42;dry_run"
        )

        assert parsed == {
            "toolkit_type": "indexer",
            "operations": [{
                "action": "rename",
                "old_name": "list_collections",
                "new_name": "list_indexes",
            }],
            "project_id": 42,
            "dry_run": True,
        }

    def test_param_string_without_dry_run(self, toolkit_migration_module):
        parsed = toolkit_migration_module.parse_migration_args(
            "github;list_collections>list_indexes;project_id=all"
        )

        assert parsed["dry_run"] is False
        assert parsed["project_id"] == "all"
        assert parsed["operations"] == [{
            "action": "rename",
            "old_name": "list_collections",
            "new_name": "list_indexes",
        }]
