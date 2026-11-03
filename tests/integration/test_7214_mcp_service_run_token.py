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
          "scope": ["read", "build"], "tgt": [1, 5]}


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
    tools_pkg.openapi_registry = types.SimpleNamespace(get_mcp_api_tools=lambda *a, **k: [],
                                                       get_combined_spec=lambda full=True: {"paths": {}})
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
    exceptions.AdmissionDenied = type("AdmissionDenied", (Exception,), {})   # a refused governed run (#7336)
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

    pipeline_execution = types.ModuleType("plugins.elitea_core.utils.pipeline_execution")
    pipeline_execution.TriggerType = types.SimpleNamespace(MCP="mcp", API="api")   # imported by mcp_service (agent calls)
    monkeypatch.setitem(sys.modules, pipeline_execution.__name__, pipeline_execution)

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

    def check_operation(operation, target, tool_name, material=False):
        checks.append((operation, target, tool_name) + (("material",) if material else ()))
        if operation is None:
            raise Refused("INTERNAL_OPERATION_UNKNOWN")
        if operation not in flask.g.internal_obo["scope"]:
            raise Refused("INTERNAL_SCOPE_DENIED")
        if material:
            raise Refused("CREDENTIALS_VIA_FORM")

    obo = types.ModuleType("plugins.elitea_core.utils.internal_obo")
    obo.VERIFIED_ENVIRON_KEY = "elitea.internal_obo.claims"
    obo.OperationRefused = Refused
    obo.check_operation = check_operation
    obo.refusal_text = lambda exc: f"{exc.code}: {exc.message}"
    # the real classifier and its map, lifted from the module (no Pylon imports)
    tree = ast.parse((PLUGIN_ROOT / "utils" / "internal_obo.py").read_text())
    wanted = [node for node in tree.body if (
        isinstance(node, ast.Assign) and any(getattr(t, "id", None) in (
            "ENDPOINT_OPERATIONS", "READ_METHODS", "MAX_BODY_DEPTH", "MAX_BODY_NODES", "MAX_NESTED_JSON_CHARS",
            "MATERIAL_IN", "MATERIAL_OUT", "MATERIAL_NONE", "MATERIAL_PER_REQUEST", "DECLARED_MATERIAL",
            "MAX_SCHEMA_WALK", "_MATERIALS", "PER_REQUEST_FIELDS", "MATERIAL_UNREVIEWED", "MATERIAL_UNKNOWN",
            "TOOLKIT_WRITES", "VERSION_WRITES", "PIPELINE_SECRET_KEYS", "SECRET_REFERENCE", "SECRET_NAME",
            "REVIEWED_FIELD_NAMES", "MAX_REDACT_NODES", "HEADER_MAP", "MAX_SETTINGS_DEPTH")
            for t in node.targets))
        or (isinstance(node, ast.FunctionDef) and node.name in (
            "endpoint_key", "operation_of", "projects_named_in", "material_of", "material_of_call", "listed_for_run",
            "_registry_materials", "schema_material", "_secret_typed", "_operation_schemas", "_secret_named",
            "_plain_value", "_pipeline_holds_secret", "_version_holds_secret", "redact_for_run"))]
    namespace = {"Optional": __import__("typing").Optional, "json": __import__("json"), "re": __import__("re"),
                 "log": sys.modules["pylon.core.tools"].log}
    exec(compile(ast.Module(wanted, []), "internal_obo.py", "exec"), namespace)  # pylint: disable=W0122
    # (#7228) a configuration body's secret fields: the configurations plugin's answer, faked per test
    holds = {"github": {"access_token"}}
    namespace["_configuration_holds_secret"] = lambda config_type, data: config_type not in holds or bool(
        set(data) & holds[config_type])
    namespace["_stored_configuration_type"] = lambda project_id, config_id: "github"
    # a toolkit's typed secret: the real rule is tested on the real package (test_7228_credential_material); here a
    # token at ``token`` or an Authorization header stands for it
    namespace["toolkit_holds_secret"] = lambda tool_type, settings, project_id, user_id: bool(
        isinstance(settings, dict) and (settings.get("token") or (settings.get("headers") or {}).get("Authorization")))
    namespace["_stored_toolkit_type"] = lambda project_id, tool_id: "github"
    namespace["_redact_settings"] = lambda settings, tool_type, project_id, user_id: {
        **settings, **({"headers": {k: "governed-reference" for k in settings["headers"]}} if settings.get("headers")
                       else {})}
    obo.operation_of = namespace["operation_of"]
    obo.material_of_call = namespace["material_of_call"]
    obo.listed_for_run = namespace["listed_for_run"]
    obo.redact_for_run = namespace["redact_for_run"]

    def check_targets(arguments, target, tool_name):
        if any(p not in flask.g.internal_obo["tgt"] for p in namespace["projects_named_in"](arguments)):
            raise Refused("INTERNAL_TARGET_DENIED")

    obo.check_targets = check_targets
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


def test_a_participant_from_a_project_outside_the_targets_is_refused_before_the_api_is_called(service):
    service.flask.g.internal_obo = dict(CLAIMS, scope=["read", "build", "run"])
    participants = tool("POST", "/api/v2/elitea_core/participants/{mode}/{project_id}/{conversation_id}")
    participants["parameters"].append({"name": "conversation_id", "in": "path", "schema": {"type": "integer"}})
    result = service.executor.execute(participants, {
        "project_id": 1, "conversation_id": 12,
        "participants": [{"entity_name": "application", "entity_meta": {"id": 7, "project_id": 9}}]})
    assert result == {"error": "INTERNAL_TARGET_DENIED: readable text"} and service.sent == []
    ok = service.executor.execute(participants, {
        "project_id": 1, "conversation_id": 12,
        "participants": [{"entity_name": "application", "entity_meta": {"id": 7, "project_id": 5}}]})
    assert ok == {"result": {}} and len(service.sent) == 1


@pytest.mark.parametrize("value", ["../secrets", "a/b", "..", ".", "x\\y", "x?y", "x#y", "x\ny", "x\r\ny", ""])
def test_a_path_parameter_cannot_leave_its_segment(service, value):
    tool_ = tool("PUT", "/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}")
    tool_["parameters"].append({"name": "tool_id", "in": "path", "schema": {"type": "string"}})
    result = service.executor.execute(tool_, {"project_id": 1, "tool_id": value})
    assert "single path segment" in result["error"] or "plain path segment" in result["error"]
    assert service.sent == []


def test_a_filename_may_hold_folders_but_no_dot_segment(service):
    artifact = tool("GET", "/api/v2/artifacts/artifact/{mode}/{project_id}/{bucket}/{filename}")
    artifact["parameters"] += [{"name": "bucket", "in": "path", "schema": {"type": "string"}},
                               {"name": "filename", "in": "path", "schema": {"type": "string"}}]
    assert service.executor.execute(artifact, {"project_id": 1, "bucket": "b", "filename": "dir/report.md"}) == {
        "result": {}}
    assert service.sent[0]["PATH_INFO"] == "/api/v2/artifacts/artifact/prompt_lib/1/b/dir/report.md"
    refused = service.executor.execute(artifact, {"project_id": 1, "bucket": "b", "filename": "dir/../../x"})
    assert "plain path segment" in refused["error"] and len(service.sent) == 1


def test_without_a_run_token_nothing_is_checked_or_marked(service):
    result = service.executor.execute(tool("POST", "/api/v2/configurations/configurations/{project_id}"),
                                      {"project_id": 1})
    assert result == {"result": {}} and service.checks == []
    assert "elitea.internal_obo.claims" not in service.sent[0]


def test_an_api_tokens_api_tool_call_reaches_its_route_as_the_token_so_the_hook_holds_it_to_its_scope(service):
    """#7336: Elitea as an MCP server for a client with an API token. The MCP route itself is a ``run`` route of the
    token's project (``token_scope``); an API tool it exposes is the in-process request of that route, which carries the
    token (its bearer and its identity), so the auth hook asks the token scope checker again for the tool's own route
    (``build`` for this write) instead of the MCP route's ``run``. Nothing of the run token's checks applies."""
    service.flask.g.auth = types.SimpleNamespace(type="token", id=21, reference="-")
    service.flask.request = types.SimpleNamespace(headers={"Authorization": "Bearer pat-not-a-secret"}, cookies={})
    result = service.executor.execute(tool("POST", "/api/v2/elitea_core/applications/{mode}/{project_id}"),
                                      {"project_id": 5, "name": "a"})
    assert result == {"result": {}} and service.checks == []
    [environ] = service.sent
    assert (environ["HTTP_X_AUTH_TYPE"], environ["HTTP_X_AUTH_ID"]) == ("token", "21")
    assert environ["HTTP_AUTHORIZATION"] == "Bearer pat-not-a-secret"
    assert environ["PATH_INFO"] == "/api/v2/elitea_core/applications/prompt_lib/5"
    assert "elitea.internal_obo.claims" not in environ


def test_a_refused_agent_call_over_the_mcp_server_says_why(mcp_service_module, monkeypatch):
    """#7336: an agent call whose governed run is refused (an API token out of its scope, an unscoped token under
    enforce) answers the MCP client with the refusal's code and fixed text as a tool error, not a generic failure."""
    denied = sys.modules["plugins.elitea_core.utils.exceptions"].AdmissionDenied
    mcp_service_module.AdmissionDenied = denied

    class Refusal(denied):
        reason_code, message = "IDENTITY_KIND_NOT_ADMITTED", "Runs started with an unscoped API token are refused."

    def do_predict(**kwargs):
        raise Refusal()

    sent = []
    session = types.SimpleNamespace(project_id=5, dispatch_message=sent.append)
    mcp_service_module.this.module = types.SimpleNamespace(do_predict=do_predict)
    service = mcp_service_module.McpService(session)
    monkeypatch.setattr(service, "_McpService__get_ver_id_by_agent_name", lambda name: 6)
    request = mcp_service_module.types.CallToolRequest(
        method="tools/call", params={"name": "agent", "arguments": {"task": "hello"}})
    service._McpService__handle_call_tool_request(type("R", (), {"id": "1", "params": request.params})())  # pylint: disable=W0212

    reply = __import__("json").loads(sent[0])["result"]
    assert reply["isError"] is True
    assert reply["content"][0]["text"] == "IDENTITY_KIND_NOT_ADMITTED: Runs started with an unscoped API token are refused."


# --- #7228: credential material never on the model path ----------------------------------------------------------

def secret_tool(method, path, *extra):
    found = tool(method, path)
    found["parameters"] += [{"name": name, "in": "path", "schema": {"type": "string"}} for name in extra]
    return found


ALL_SCOPES = ["read", "build", "credentials", "run"]


@pytest.mark.parametrize("method, path, extra, arguments", [
    ("POST", "/api/v2/secrets/secrets/{mode}/{project_id}", (), {"name": "GH", "value": "made-up-not-a-secret"}),
    ("PUT", "/api/v2/secrets/secret/{mode}/{project_id}/{secret}", ("secret",),
     {"secret": "GH", "name": "GH", "value": "made-up-not-a-secret"}),
    ("POST", "/api/v2/configurations/configurations/{project_id}", (),
     {"type": "github", "elitea_title": "gh", "label": "gh", "data": {"access_token": "made-up-not-a-secret"}}),
    ("PUT", "/api/v2/configurations/configuration/{project_id}/{config_id}", ("config_id",),
     {"config_id": 4, "data": {"access_token": "made-up-not-a-secret"}}),
    ("POST", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}", (), {}),
    ("POST", "/api/v2/elitea_core/applications/{mode}/{project_id}", (), {"name": "a", "webhook_secret": "made-up"}),
    ("PUT", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}", ("access_key_id",),
     {"access_key_id": "AKIDEXAMPLE7228"}),
])
def test_a_credential_value_never_moves_on_a_credentials_scoped_run_token_and_nothing_is_written(
        service, method, path, extra, arguments):
    service.flask.g.internal_obo = dict(CLAIMS, scope=ALL_SCOPES)
    result = service.executor.execute(secret_tool(method, path, *extra), {"project_id": 1, **arguments})
    assert result["error"].startswith("CREDENTIALS_VIA_FORM") and service.sent == []
    assert service.checks[0][-1] == "material"


def test_reading_a_secret_value_is_a_credentials_operation_and_refused_as_material(service):
    service.flask.g.internal_obo = dict(CLAIMS, scope=ALL_SCOPES)
    result = service.executor.execute(secret_tool("GET", "/api/v2/secrets/secret/{mode}/{project_id}/{secret}",
                                                  "secret"), {"project_id": 1, "secret": "GH"})
    assert result["error"].startswith("CREDENTIALS_VIA_FORM") and service.sent == []


def test_a_read_only_binding_meets_its_scope_first(service):
    service.flask.g.internal_obo = dict(CLAIMS, scope=["read"])
    result = service.executor.execute(secret_tool("POST", "/api/v2/secrets/secrets/{mode}/{project_id}"),
                                      {"project_id": 1, "name": "GH", "value": "made-up-not-a-secret"})
    assert result["error"].startswith("INTERNAL_SCOPE_DENIED") and service.sent == []


@pytest.mark.parametrize("arguments", [
    {"type": "llm_model", "elitea_title": "m", "label": "m",
     "data": {"name": "gpt", "ai_credentials": {"elitea_title": "azure", "private": False}}},
    {"type": "github", "elitea_title": "gh", "label": "gh", "data": {"base_url": "https://api.github.com"}},
])
def test_a_model_definition_pointing_at_an_existing_credential_still_runs_under_the_credentials_scope(
        service, arguments):
    service.flask.g.internal_obo = dict(CLAIMS, scope=ALL_SCOPES)
    holds = {"llm_model": set(), "github": {"access_token"}}
    obo = sys.modules["plugins.elitea_core.utils.internal_obo"]
    obo.material_of_call.__globals__["_configuration_holds_secret"] = lambda config_type, data: bool(
        set(data) & holds[config_type])
    result = service.executor.execute(tool("POST", "/api/v2/configurations/configurations/{project_id}"),
                                      {"project_id": 1, **arguments})
    assert result == {"result": {}} and len(service.sent) == 1
    assert service.checks == [("credentials", "POST /api/v2/configurations/configurations/1", "post_tool")]


def test_without_a_run_token_a_credential_write_is_not_touched(service):
    result = service.executor.execute(secret_tool("POST", "/api/v2/secrets/secrets/{mode}/{project_id}"),
                                      {"project_id": 1, "name": "GH", "value": "made-up-not-a-secret"})
    assert result == {"result": {}} and service.checks == []


def test_the_run_token_is_not_shown_the_tools_it_may_never_call(service, mcp_service_module):
    obo = sys.modules["plugins.elitea_core.utils.internal_obo"]
    listed = {(m, p): obo.listed_for_run(m, p) for m, p in [
        ("POST", "/api/v2/secrets/secrets/{mode}/{project_id}"),
        ("PUT", "/api/v2/secrets/secret/{mode}/{project_id}/{secret}"),
        ("POST", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}"),
        ("PUT", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}"),
        ("POST", "/api/v2/configurations/configurations/{project_id}"),
        ("GET", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}"),
        ("POST", "/api/v2/elitea_core/applications/{mode}/{project_id}")]}
    assert [key for key, shown in listed.items() if not shown] == [
        ("POST", "/api/v2/secrets/secrets/{mode}/{project_id}"),
        ("PUT", "/api/v2/secrets/secret/{mode}/{project_id}/{secret}"),
        ("POST", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}"),
        ("PUT", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}")]
    tools = [{"method": m, "path": p, "label": f"{m}_{i}", "value": f"{m.lower()}_{i}", "args_schema": {},
              "description": ""} for i, (m, p) in enumerate(listed)]
    registry = sys.modules["tools"].openapi_registry
    registry.get_mcp_api_tools = lambda *a, **k: tools
    session = types.SimpleNamespace(project_id=1, tags=None, entity_category=None, resource_type=None,
                                    resource_id=None)
    mcp = mcp_service_module.McpService(session)
    service.flask.g.internal_obo = dict(CLAIMS, scope=ALL_SCOPES)
    with_token = [t.name for t in mcp._McpService__get_all_tools()]  # pylint: disable=W0212
    del service.flask.g.internal_obo
    without = [t.name for t in mcp._McpService__get_all_tools()]  # pylint: disable=W0212
    assert len(without) == 7 and with_token == ["post_4", "get_5", "post_6"]


def test_a_change_a_run_makes_is_filed_for_its_user_and_the_model_reads_what_happens_next(
        service, mcp_service_module, monkeypatch):
    """(#7228 item 8) a PUT/PATCH of a run is handed to ``file_for_run`` with its path parameters; the model's tool
    result carries the summary and its one sentence (kinds, titles, states only)."""
    asked = []
    summary = {"entity_kind": "toolkit", "entity_id": 3, "filed": [{"key": "grant:3:u", "kind": "grant", "id": "g"}],
               "items": [], "next": "Requested for approval: your use of the credential."}
    requests = types.ModuleType("plugins.elitea_core.utils.governance_requests")
    requests.file_for_run = lambda *args: asked.append(args) or summary
    monkeypatch.setitem(sys.modules, requests.__name__, requests)
    monkeypatch.setattr(mcp_service_module.this, "module", types.SimpleNamespace(), raising=False)
    service.flask.g.internal_obo = dict(CLAIMS, scope=ALL_SCOPES)
    put = secret_tool("PUT", "/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}", "tool_id")
    result = service.executor.execute(put, {"project_id": 1, "tool_id": "3", "settings": {}})
    assert result == {"result": {}, "governance_requests": summary}
    [(_, claims, method, path, project_id, answer, path_params)] = asked
    assert (method, path_params["tool_id"], project_id) == ("PUT", "3", 1)
    assert mcp_service_module._with_governance(result) == {"governance_requests": summary}   # pylint: disable=W0212
    assert mcp_service_module._with_governance({"result": [1]}) == [1]   # pylint: disable=W0212
    assert mcp_service_module._with_governance({"result": [1], "governance_requests": summary}) == {   # pylint: disable=W0212
        "result": [1], "governance_requests": summary}


# --- fix round (review M-1, M-2) ------------------------------------------------------------------------------

@pytest.mark.parametrize("method, path, extra, arguments", [
    ("POST", "/api/v2/elitea_core/tools/{mode}/{project_id}", (),
     {"type": "github", "name": "gh", "settings": {"token": "made-up-not-a-secret"}}),
    ("PUT", "/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}", ("tool_id",),
     {"tool_id": "3", "settings": {"headers": {"Authorization": "Bearer made-up"}}}),
    ("PUT", "/api/v2/elitea_core/version/{mode}/{project_id}/{application_id}/{version_id}",
     ("application_id", "version_id"),
     {"application_id": "2", "version_id": "2", "pipeline_settings": {"trigger": {"webhook_secret": "made-up"}}}),
    ("POST", "/api/v2/elitea_core/versions/{mode}/{project_id}/{application_id}", ("application_id",),
     {"application_id": "2", "pipeline_settings": {"trigger": {"webhook_signing_secret": "made-up"}}}),
])
def test_a_build_write_with_a_typed_secret_is_refused_and_nothing_is_written(service, method, path, extra, arguments):
    service.flask.g.internal_obo = dict(CLAIMS)   # read and build only: the build scope does not help
    result = service.executor.execute(secret_tool(method, path, *extra), {"project_id": 1, **arguments})
    assert result["error"].startswith("CREDENTIALS_VIA_FORM") and service.sent == []
    assert service.checks[0][0] == "build" and service.checks[0][-1] == "material"


def test_a_toolkit_selecting_a_stored_credential_is_built_as_before(service):
    service.flask.g.internal_obo = dict(CLAIMS)
    result = service.executor.execute(tool("POST", "/api/v2/elitea_core/tools/{mode}/{project_id}"), {
        "project_id": 1, "type": "github", "name": "gh",
        "settings": {"github_configuration": {"elitea_title": "gh", "private": False}}})
    assert result == {"result": {}} and len(service.sent) == 1


def test_a_run_tokens_read_of_a_version_is_served_with_its_toolkit_secrets_redacted(service, monkeypatch):
    stored = {"id": 73, "tools": [{"type": "mcp", "settings": {"url": "https://x",
                                                               "headers": {"Authorization": "Bearer made-up-26"}}}]}
    monkeypatch.setattr(service.executor, "_execute_wsgi_request",
                        staticmethod(lambda environ: (200, __import__("json").dumps(stored))))
    read = secret_tool("GET", "/api/v2/elitea_core/version/{mode}/{project_id}/{application_id}/{version_id}",
                       "application_id", "version_id")
    service.flask.g.internal_obo = dict(CLAIMS, scope=["read"])
    on_token = service.executor.execute(read, {"project_id": 1, "application_id": "58", "version_id": "73"})
    assert "made-up-26" not in __import__("json").dumps(on_token)
    assert on_token["result"]["tools"][0]["settings"]["headers"] == {"Authorization": "governed-reference"}
    del service.flask.g.internal_obo   # the same read in the person's own session: unchanged
    in_session = service.executor.execute(read, {"project_id": 1, "application_id": "58", "version_id": "73"})
    assert in_session == {"result": stored}
