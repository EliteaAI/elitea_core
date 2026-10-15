"""#6943 - toolkit credential settings may reference a credential by its immutable configuration_uuid.

Run via:
    python tests/run_tests.py integration/test_6943_expand_toolkit_settings.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
UUID = "6f1b1c0e-3c52-4c27-9a36-2f7d6d4b8f10"


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

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.VaultClient = type("VaultClient", (), {"get_secrets": lambda self: {}})
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    models_indexer.IndexRun = type("IndexRun", (), {})
    models_indexer.INDEX_RUN_PENDING = "pending"
    models_indexer.INDEX_RUN_CANCELLED = "cancelled"
    models_indexer.INDEX_RUN_STATUSES = ("pending", "cancelled", "promoted", "discarded")
    models_indexer.INDEX_RUN_LIVE_INDEX_NAME = "uq_elitea_index_runs_live"
    models_indexer.INDEX_RUN_LIVE_INDEX_PREDICATE = "status = 'pending'"
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

    utils_mod = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_mod.parse_ids_filter = lambda ids, max_ids=100: ids
    sys.modules["plugins.elitea_core.utils.utils"] = utils_mod

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def rpc_calls(application_tools_module):
    calls = []

    def configurations_expand(**kwargs):
        calls.append(kwargs)
        return {"expanded": True}

    application_tools_module.context.rpc_manager = types.SimpleNamespace(
        timeout=lambda _t: types.SimpleNamespace(configurations_expand=configurations_expand)
    )
    return calls


class TestExpandToolkitSettingsKeys:

    @pytest.mark.parametrize("settings", [
        {"elitea_title": "my-cred", "private": False},
        {"elitea_title": "my-cred"},
        {"configuration_uuid": UUID},
        {"configuration_uuid": UUID, "private": True},
        {"configuration_uuid": UUID, "configuration_project_id": 5},
        {"configuration_uuid": UUID, "configuration_project_id": 5, "elitea_title": "my-cred", "private": False},
    ])
    def test_accepted_shapes_reach_the_rpc_unchanged(self, application_tools_module, rpc_calls, settings):
        result = application_tools_module._expand_toolkit_settings(settings, project_id=3, user_id=9)
        assert result == {"expanded": True}
        assert rpc_calls == [{"project_id": 3, "settings": settings, "user_id": 9, "unsecret": True}]

    @pytest.mark.parametrize("settings", [
        {"private": True},
        {"configuration_project_id": 5},
        {"elitea_title": "my-cred", "private": False, "extra": 1},
        {"configuration_uuid": UUID, "api_key": "x"},
    ])
    def test_other_shapes_keep_the_readable_value_error(self, application_tools_module, rpc_calls, settings):
        with pytest.raises(ValueError, match="Toolkit credential settings must contain only fields"):
            application_tools_module._expand_toolkit_settings(settings, project_id=3, user_id=9)
        assert rpc_calls == []

    def test_empty_settings_are_returned_untouched(self, application_tools_module, rpc_calls):
        assert application_tools_module._expand_toolkit_settings({}, project_id=3, user_id=9) == {}
        assert rpc_calls == []
