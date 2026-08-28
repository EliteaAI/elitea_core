"""Issue #6273 follow-up - MCP tool-call dispatch must respect available_by_mcp
and must not silently route to the wrong toolkit on a name collision.

Widening `toolkits_listing(..., filter_mcp=None)` in `mcp_service.py` means
`McpService.__get_toolkit_by_name` (used to resolve an MCP `tools/call` to a
toolkit) now sees MCP-connected toolkits too. Those toolkits carry the
caller's own remote credentials, so two things must hold:

1. Only toolkits with `meta.mcp_options.available_by_mcp == True` may be
   dispatched to (mirrors the gate already applied when *listing* tools in
   `__get_toolkit_tools` / `__get_all_tools`).
2. If two toolkits would export the same MCP tool name, dispatch must not
   silently pick "whichever came first" — the colliding name should resolve
   to neither (safe failure) rather than to an arbitrary toolkit.

This suite exercises the real `__get_toolkit_by_name` method (not a
reimplementation of its logic) by stubbing only its module dependencies.
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _toolkit(id_, name, tools, available_by_mcp=True):
    return {
        "id": id_,
        "name": name,
        "type": "some_type",
        "description": "",
        "settings": {"selected_tools": tools},
        "meta": {"mcp_options": {"available_by_mcp": available_by_mcp}},
    }


@pytest.fixture()
def mcp_service_module(monkeypatch):
    """Load mcp_service.py standalone with minimal stubs (module deps only;
    `toolkits_listing` is left as a real attribute on the loaded module so
    each test can monkeypatch it directly)."""
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.utils"):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        monkeypatch.setitem(sys.modules, name, mod)

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "pylon", sys.modules.get("pylon") or types.ModuleType("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", sys.modules.get("pylon.core") or types.ModuleType("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.auth = types.SimpleNamespace(current_user=lambda: {"id": 1})
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.openapi_registry = types.SimpleNamespace(get_mcp_api_tools=lambda *a, **k: [])
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.Application = type("Application", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.all", models_all)

    app_tools = types.ModuleType("plugins.elitea_core.utils.application_tools")
    app_tools.toolkits_listing = lambda **kwargs: {"rows": []}
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.application_tools", app_tools)

    app_utils = types.ModuleType("plugins.elitea_core.utils.application_utils")
    app_utils.list_applications_api = lambda *a, **k: {"applications": []}
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.application_utils", app_utils)

    tk_utils = types.ModuleType("plugins.elitea_core.utils.toolkits_utils")
    tk_utils.get_toolkit_schemas = lambda *a, **k: {}
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.toolkits_utils", tk_utils)

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.exceptions", exceptions)

    mcp_session = types.ModuleType("plugins.elitea_core.utils.mcp_session")
    mcp_session.SseSession = type("SseSession", (), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.mcp_session", mcp_session)

    mcp_versioning = types.ModuleType("plugins.elitea_core.utils.mcp_versioning")
    mcp_versioning.INTERNAL_MCP_ENVIRON_KEY = 'elitea.internal_mcp_request'
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.mcp_versioning", mcp_versioning)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.mcp_service",
        PLUGIN_ROOT / "utils" / "mcp_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _service(mcp_service_module, toolkits_rows):
    mcp_service_module.toolkits_listing = lambda **kwargs: {"rows": toolkits_rows}
    service = mcp_service_module.McpService.__new__(mcp_service_module.McpService)
    service.session = types.SimpleNamespace(project_id=1)
    return service


class TestGetToolkitByNameAvailableByMcpGate:
    def test_toolkit_not_opted_into_mcp_is_not_dispatchable(self, mcp_service_module):
        toolkits = [_toolkit(1, "figma", ["get_file"], available_by_mcp=False)]
        service = _service(mcp_service_module, toolkits)
        assert service._McpService__get_toolkit_by_name("figma_get_file") is None

    def test_toolkit_opted_into_mcp_is_dispatchable(self, mcp_service_module):
        toolkits = [_toolkit(1, "figma", ["get_file"], available_by_mcp=True)]
        service = _service(mcp_service_module, toolkits)
        found = service._McpService__get_toolkit_by_name("figma_get_file")
        assert found is not None and found["id"] == 1


class TestGetToolkitByNameCollisionSafety:
    def test_colliding_tool_names_across_toolkits_resolve_to_neither(self, mcp_service_module):
        """Two available-by-mcp toolkits exporting the same MCP tool name
        must not let the call route to whichever one happened to be seen
        first."""
        toolkits = [
            _toolkit(1, "shared_name", ["get_file"]),
            _toolkit(2, "shared_name", ["get_file"]),
        ]
        service = _service(mcp_service_module, toolkits)
        assert service._McpService__get_toolkit_by_name("shared_name_get_file") is None

    def test_non_colliding_names_still_resolve(self, mcp_service_module):
        toolkits = [
            _toolkit(1, "figma", ["get_file"]),
            _toolkit(2, "jira", ["get_issue"]),
        ]
        service = _service(mcp_service_module, toolkits)
        found = service._McpService__get_toolkit_by_name("jira_get_issue")
        assert found is not None and found["id"] == 2


def test_internal_api_wsgi_request_is_marked_as_mcp(mcp_service_module, monkeypatch):
    flask_stub = types.ModuleType('flask')
    flask_stub.g = types.SimpleNamespace()
    flask_stub.request = types.SimpleNamespace(headers={}, cookies={})
    monkeypatch.setitem(sys.modules, 'flask', flask_stub)

    environ = mcp_service_module.McpApiToolExecutor._build_wsgi_environ(
        'PUT', '/v2/version/prompt_lib/1/2/3', {}, {'name': 'base'},
    )

    assert environ['elitea.internal_mcp_request'] is True
