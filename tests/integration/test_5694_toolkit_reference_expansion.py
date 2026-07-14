"""Issue #5694 - expand toolkit reference arrays without treating them as scalar ids.

Tests the expand_toolkit_settings function which handles expansion of
toolkit references (both scalar and array) in settings.

Run via:
    python tests/run_tests.py integration/test_5694_toolkit_reference_expansion.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def application_tools_module():
    """Load application_tools with minimal stubs."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    # Pylon stubs
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules["pylon.core.tools"] = tools_mod

    # tools package stub - must have proper attributes for `from tools import ...`
    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

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

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestToolkitReferenceExpansion:
    """Tests for expand_toolkit_settings function."""

    def test_expand_toolkit_settings_expands_toolkit_reference_lists(self, application_tools_module):
        """Array of toolkit IDs should be expanded to array of toolkit objects."""
        application_tools_module.find_toolkit_schema_by_type_everywhere = (
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

        application_tools_module._expand_toolkit_reference = fake_expand_toolkit_reference

        expanded = application_tools_module.expand_toolkit_settings(
            "inventory",
            {"sources": [1, 2]},
            project_id=8,
            user_id=13,
        )

        assert seen_ids == [(1, 8, 13), (2, 8, 13)]
        assert [item["id"] for item in expanded["sources"]] == [1, 2]

    def test_expand_toolkit_settings_keeps_scalar_reference_behavior(self, application_tools_module):
        """Scalar toolkit ID should be expanded to single toolkit object."""
        application_tools_module.find_toolkit_schema_by_type_everywhere = (
            lambda _type, _project_id, _user_id: (
                {"properties": {"primary_toolkit": {"toolkit_types": ["github"]}}},
                True,
            )
        )

        seen_ids = []

        def fake_expand_toolkit_reference(toolkit_id, project_id, user_id):
            seen_ids.append((toolkit_id, project_id, user_id))
            return {"id": toolkit_id, "type": "github", "settings": {}}

        application_tools_module._expand_toolkit_reference = fake_expand_toolkit_reference

        expanded = application_tools_module.expand_toolkit_settings(
            "inventory",
            {"primary_toolkit": 7},
            project_id=8,
            user_id=13,
        )

        assert seen_ids == [(7, 8, 13)]
        assert expanded["primary_toolkit"]["id"] == 7
