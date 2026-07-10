"""Issue #5694 - expand toolkit reference arrays without treating them as scalar ids.

Run from the elitea_core plugin root:

    python3 -m pytest --rootdir=utils/tests --import-mode=importlib \
        utils/tests/test_5694_toolkit_reference_expansion.py -v
"""

import importlib.util
import pathlib
import sys
import types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _install_stubs():
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        module = sys.modules.setdefault(name, types.ModuleType(name))
        module.__path__ = []

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    sys.modules["plugins.elitea_core.models.indexer"] = models_indexer

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums.AgentTypes = type("AgentTypes", (), {})
    enums.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums.IndexDataStatus = type(
        "IndexDataStatus",
        (),
        {
            "in_progress": types.SimpleNamespace(value="in_progress"),
            "cancelled": types.SimpleNamespace(value="cancelled"),
        },
    )
    sys.modules["plugins.elitea_core.models.enums.all"] = enums

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    sys.modules["plugins.elitea_core.utils.exceptions"] = exceptions


def _load_module():
    _install_stubs()
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_expand_toolkit_settings_expands_toolkit_reference_lists():
    module = _load_module()
    module.find_toolkit_schema_by_type_everywhere = (
        lambda _type, _project_id, _user_id: (
            {"properties": {"sources": {"toolkit_types": ["github", "ado_repos"]}}},
            True,
        )
    )

    seen_ids = []

    def fake_expand_toolkit_reference(toolkit_id, project_id, user_id):
        seen_ids.append((toolkit_id, project_id, user_id))
        return {
            "id": toolkit_id,
            "toolkit_name": f"tool-{toolkit_id}",
            "type": "github",
            "settings": {},
        }

    module._expand_toolkit_reference = fake_expand_toolkit_reference

    expanded = module.expand_toolkit_settings(
        "inventory",
        {"sources": [1, 2]},
        project_id=8,
        user_id=13,
    )

    assert seen_ids == [(1, 8, 13), (2, 8, 13)]
    assert [item["id"] for item in expanded["sources"]] == [1, 2]


def test_expand_toolkit_settings_keeps_scalar_reference_behavior():
    module = _load_module()
    module.find_toolkit_schema_by_type_everywhere = (
        lambda _type, _project_id, _user_id: (
            {"properties": {"primary_toolkit": {"toolkit_types": ["github"]}}},
            True,
        )
    )

    seen_ids = []

    def fake_expand_toolkit_reference(toolkit_id, project_id, user_id):
        seen_ids.append((toolkit_id, project_id, user_id))
        return {"id": toolkit_id, "type": "github", "settings": {}}

    module._expand_toolkit_reference = fake_expand_toolkit_reference

    expanded = module.expand_toolkit_settings(
        "inventory",
        {"primary_toolkit": 7},
        project_id=8,
        user_id=13,
    )

    assert seen_ids == [(7, 8, 13)]
    assert expanded["primary_toolkit"]["id"] == 7
