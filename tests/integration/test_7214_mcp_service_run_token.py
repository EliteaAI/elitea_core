"""#7214: the internal MCP service under a run token - every API tool call is classified and checked against the
token's scope before it runs, a refusal comes back as the readable tool error, and the in-process API call carries
the verified claims so it is not validated twice. Calls without a run token are not touched.

``mcp_service.py`` is loaded standalone (the fixture of ``test_6273_mcp_service_toolkit_dispatch``); the token module
is the real one, its audit and context reads are stubbed.
"""

import ast
import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
CLAIMS = {"jti": "j1", "sub": "idp:t:o", "uid": 42, "act": "binding-1", "ctx": "ctx-1", "prj": 1, "run": "r1",
          "scope": ["read", "build"]}


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
    exceptions.InputGuardrailBlockedError = type("InputGuardrailBlockedError", (Exception,), {})   # imported by utils/mcp_service.py (#6886)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.exceptions", exceptions)

    internal_tools = types.ModuleType("plugins.elitea_core.utils.internal_tools")
    internal_tools.MCP_CURRENT_PROJECT_SUFFIXES = {
        'elitea_core/project_context', 'elitea_core/skills',
    }
    internal_tools.MCP_PROJECT_SCOPED_SUFFIXES = (
        internal_tools.MCP_CURRENT_PROJECT_SUFFIXES | {'elitea_core/applications'}
    )
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.internal_tools", internal_tools)

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




@pytest.fixture()
def service(mcp_service_module, monkeypatch):
    flask = types.ModuleType("flask")
    flask.g = types.SimpleNamespace()
    flask.request = types.SimpleNamespace(headers={}, cookies={})
    flask.has_request_context = lambda: True
    monkeypatch.setitem(sys.modules, "flask", flask)
    checks, sent = [], []

    class Refused(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.code, self.message = code, "readable text"

    def check_operation(operation, target, tool_name):
        checks.append((operation, target, tool_name))
        if operation is None:
            raise Refused("INTERNAL_OPERATION_UNKNOWN")
        if operation not in flask.g.internal_obo["scope"]:
            raise Refused("INTERNAL_SCOPE_DENIED")

    obo = types.ModuleType("plugins.elitea_core.utils.internal_obo")
    obo.VERIFIED_ENVIRON_KEY = "elitea.internal_obo.claims"
    obo.OperationRefused = Refused
    obo.check_operation = check_operation
    obo.refusal_text = lambda exc: f"{exc.code}: {exc.message}"
    # the real classifier and its map, lifted from the module (no Pylon imports)
    tree = ast.parse((PLUGIN_ROOT / "utils" / "internal_obo.py").read_text())
    wanted = [node for node in tree.body if (
        isinstance(node, ast.Assign) and any(getattr(t, "id", None) in ("ENDPOINT_OPERATIONS", "READ_METHODS")
                                             for t in node.targets))
        or (isinstance(node, ast.FunctionDef) and node.name in ("endpoint_key", "operation_of"))]
    namespace = {"Optional": __import__("typing").Optional}
    exec(compile(ast.Module(wanted, []), "internal_obo.py", "exec"), namespace)  # pylint: disable=W0122
    obo.operation_of = namespace["operation_of"]
    monkeypatch.setitem(sys.modules, obo.__name__, obo)
    executor = mcp_service_module.McpApiToolExecutor

    def wsgi(environ):
        sent.append(environ)
        return 200, "{}"

    monkeypatch.setattr(executor, "_execute_wsgi_request", staticmethod(wsgi))
    return types.SimpleNamespace(executor=executor, flask=flask, checks=checks, sent=sent)


def tool(method, path):
    return {"method": method, "path": path, "label": f"{method.lower()}_tool", "parameters": [
        {"name": "mode", "in": "path", "schema": {"type": "string", "default": "prompt_lib"}},
        {"name": "project_id", "in": "path", "schema": {"type": "integer"}}]}


def test_a_write_out_of_scope_is_refused_before_the_api_is_called(service):
    service.flask.g.internal_obo = dict(CLAIMS)
    result = service.executor.execute(tool("POST", "/api/v2/configurations/configurations/{project_id}"),
                                      {"project_id": 1, "type": "github"})
    assert result == {"error": "INTERNAL_SCOPE_DENIED: readable text"}
    assert service.sent == [] and service.checks[0][0] == "credentials"
    assert service.checks[0][1] == "POST /api/v2/configurations/configurations/1"


def test_a_write_in_scope_runs_and_its_api_call_carries_the_verified_claims(service):
    service.flask.g.internal_obo = dict(CLAIMS)
    result = service.executor.execute(tool("POST", "/api/v2/elitea_core/applications/{mode}/{project_id}"),
                                      {"project_id": 1, "name": "a"})
    assert result == {"result": {}}
    [environ] = service.sent
    assert environ["elitea.internal_obo.claims"] == CLAIMS and environ["elitea.internal_mcp_request"] is True
    assert service.checks == [("build", "POST /api/v2/elitea_core/applications/prompt_lib/1", "post_tool")]


def test_a_write_the_map_does_not_know_is_refused_before_the_api_is_called(service):
    service.flask.g.internal_obo = dict(CLAIMS, scope=["read", "build", "credentials", "run"])
    result = service.executor.execute(tool("POST", "/api/v2/projects/project/{project_id}"), {"project_id": 1})
    assert result == {"error": "INTERNAL_OPERATION_UNKNOWN: readable text"} and service.sent == []
    assert service.checks[0][0] is None


def test_an_s3_credential_is_a_credentials_operation_not_build(service):
    service.flask.g.internal_obo = dict(CLAIMS)
    result = service.executor.execute(tool("POST", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}"),
                                      {"project_id": 1})
    assert result["error"].startswith("INTERNAL_SCOPE_DENIED") and service.sent == []
    assert service.checks[0][0] == "credentials"


def test_without_a_run_token_nothing_is_checked_or_marked(service):
    result = service.executor.execute(tool("POST", "/api/v2/configurations/configurations/{project_id}"),
                                      {"project_id": 1})
    assert result == {"result": {}} and service.checks == []
    assert "elitea.internal_obo.claims" not in service.sent[0]
