"""Issue #5694 - support admin backfills that add missing toolkit tools.

Run from the elitea_core plugin root:

    python3 -m pytest --rootdir=utils/tests --import-mode=importlib \
        utils/tests/test_5694_toolkit_selected_tools_migration.py -v
"""

import importlib.util
import pathlib
import sys
import types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _Log:
    @staticmethod
    def info(*_args, **_kwargs):
        pass

    @staticmethod
    def warning(*_args, **_kwargs):
        pass

    @staticmethod
    def exception(*_args, **_kwargs):
        pass


def _install_stubs():
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
    sys.modules["pylon.core.tools"] = tools_mod

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


def _load_module():
    _install_stubs()
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.toolkit_migration",
        PLUGIN_ROOT / "utils" / "toolkit_migration.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_migration_args_supports_add_operation():
    module = _load_module()

    parsed = module.parse_migration_args("inventory;+ask;project_id=all;dry_run")

    assert parsed == {
        "toolkit_type": "inventory",
        "operations": [{"action": "add", "tool_name": "ask"}],
        "project_id": "all",
        "dry_run": True,
    }


def test_apply_operations_to_selected_tools_adds_missing_tool_once():
    module = _load_module()

    updated, changes, added_count, removed_count, renamed_count = (
        module.apply_operations_to_selected_tools(
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
        module.apply_operations_to_selected_tools(
            updated,
            [{"action": "add", "tool_name": "ask"}],
        )
    )

    assert updated_again == updated
    assert changes_again == []
    assert added_again == 0
    assert removed_again == 0
    assert renamed_again == 0


def test_apply_operations_to_selected_tools_handles_mixed_add_remove_rename():
    module = _load_module()

    updated, changes, added_count, removed_count, renamed_count = (
        module.apply_operations_to_selected_tools(
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
