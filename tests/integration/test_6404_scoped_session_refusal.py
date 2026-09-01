"""Issue #6404 - a builder MCP session refuses tool arguments naming another project.

Drives the real `McpService.__handle_call_tool_request` path so the whole chain is covered:
the session's scope_project_id -> __session_scoped_path_params -> McpApiToolExecutor.execute ->
the CallToolResult the MCP client actually receives. A unit test on _parse_arguments alone
would not catch the scope being dropped between the session and the executor.
"""

import contextlib
import importlib.util
import json
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

SKILLS_TAG = 'elitea_core/skills'
APPLICATIONS_TAG = 'elitea_core/applications'
TOOLKITS_TAG = 'elitea_core/toolkits'
SCOPE_PROJECT = 2
OTHER_PROJECT = 3

API_TOOL = {
    'value': 'get_elitea_core_skills',
    'label': 'get_elitea_core_skills',
    'method': 'get',
    'path': '/api/v2/elitea_core/skills/prompt_lib/{project_id}',
    'parameters': [{'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}}],
}


@pytest.fixture()
def mcp_service_module(monkeypatch):
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
    tools_pkg.db = types.SimpleNamespace(
        get_session=lambda pid: contextlib.nullcontext(None),
        with_project_schema_session=lambda pid: contextlib.nullcontext(None),
    )
    tools_pkg.auth = types.SimpleNamespace(current_user=lambda: {"id": 1})
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.openapi_registry = types.SimpleNamespace(get_mcp_api_tools=lambda *a, **k: [API_TOOL])
    tools_pkg.sanitize_property_name = lambda name: name
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

    internal_tools = types.ModuleType("plugins.elitea_core.utils.internal_tools")
    internal_tools.MCP_CURRENT_PROJECT_SUFFIXES = {SKILLS_TAG, 'elitea_core/project_context'}
    internal_tools.MCP_PROJECT_SCOPED_SUFFIXES = (
        internal_tools.MCP_CURRENT_PROJECT_SUFFIXES | {APPLICATIONS_TAG}
    )
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.internal_tools", internal_tools)

    mcp_session = types.ModuleType("plugins.elitea_core.utils.mcp_session")
    mcp_session.SseSession = type("SseSession", (), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.mcp_session", mcp_session)

    mcp_versioning = types.ModuleType("plugins.elitea_core.utils.mcp_versioning")
    mcp_versioning.INTERNAL_MCP_ENVIRON_KEY = 'elitea.internal_mcp_request'
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.mcp_versioning", mcp_versioning)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.mcp_service", PLUGIN_ROOT / "utils" / "mcp_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _call_tool(mcp_service_module, entity_category, scope_project_id, arguments,
               reached=None):
    """Run a tools/call through the real service and return the parsed JSON-RPC response."""
    dispatched = []
    service = mcp_service_module.McpService.__new__(mcp_service_module.McpService)
    service.session = types.SimpleNamespace(
        project_id=SCOPE_PROJECT,
        tags=[],
        entity_category=entity_category,
        scope_project_id=scope_project_id,
        dispatch_message=dispatched.append,
    )

    def _fake_wsgi(environ):
        if reached is not None:
            reached.append(environ['PATH_INFO'])
        return 200, json.dumps({'rows': []}).encode()

    mcp_service_module.McpApiToolExecutor._execute_wsgi_request = staticmethod(_fake_wsgi)
    mcp_service_module.McpApiToolExecutor._build_wsgi_environ = staticmethod(
        lambda method, url_path, query_params, body_params: {'PATH_INFO': url_path}
    )

    request = types.SimpleNamespace(
        id='1',
        params=types.SimpleNamespace(
            name=mcp_service_module._build_agent_identifier(API_TOOL['value']),
            arguments=arguments,
        ),
    )
    service._McpService__handle_call_tool_request(request)
    return json.loads(dispatched[0])


def _content_text(response):
    return response['result']['content'][0]['text']


class TestScopedSessionRefusesOtherProjects:

    def test_a_foreign_project_is_refused_with_access_denied(self, mcp_service_module):
        reached = []
        response = _call_tool(
            mcp_service_module, SKILLS_TAG, SCOPE_PROJECT, {'project_id': OTHER_PROJECT}, reached
        )
        assert response['result']['isError'] is True
        assert _content_text(response).startswith('Access denied:')
        assert str(SCOPE_PROJECT) in _content_text(response)
        assert str(OTHER_PROJECT) in _content_text(response)
        assert reached == [], 'the refused call must never reach the endpoint'

    def test_the_scoped_project_is_served(self, mcp_service_module):
        reached = []
        response = _call_tool(
            mcp_service_module, SKILLS_TAG, SCOPE_PROJECT, {'project_id': SCOPE_PROJECT}, reached
        )
        assert response['result']['isError'] is False
        assert reached == [f'/api/v2/elitea_core/skills/prompt_lib/{SCOPE_PROJECT}']

    def test_an_omitted_project_is_filled_from_the_scope(self, mcp_service_module):
        reached = []
        response = _call_tool(mcp_service_module, SKILLS_TAG, SCOPE_PROJECT, {}, reached)
        assert response['result']['isError'] is False
        assert reached == [f'/api/v2/elitea_core/skills/prompt_lib/{SCOPE_PROJECT}']

    def test_agents_are_scoped_too(self, mcp_service_module):
        response = _call_tool(
            mcp_service_module, APPLICATIONS_TAG, SCOPE_PROJECT, {'project_id': OTHER_PROJECT}
        )
        assert response['result']['isError'] is True
        assert _content_text(response).startswith('Access denied:')


class TestUnscopedSessionsAreUnchanged:

    def test_a_session_without_a_scope_still_serves_any_project(self, mcp_service_module):
        reached = []
        response = _call_tool(
            mcp_service_module, SKILLS_TAG, None, {'project_id': OTHER_PROJECT}, reached
        )
        assert response['result']['isError'] is False
        assert reached == [f'/api/v2/elitea_core/skills/prompt_lib/{OTHER_PROJECT}']

    def test_a_group_outside_the_builder_set_is_not_clamped(self, mcp_service_module):
        reached = []
        response = _call_tool(
            mcp_service_module, TOOLKITS_TAG, SCOPE_PROJECT, {'project_id': OTHER_PROJECT}, reached
        )
        assert response['result']['isError'] is False
        assert reached == [f'/api/v2/elitea_core/skills/prompt_lib/{OTHER_PROJECT}']
