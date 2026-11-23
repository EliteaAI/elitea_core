"""#1597: the Elitea MCP server lists every agent version tagged ``mcp`` as its own tool and a call runs exactly the
version its tool names; an untagged version is neither listed nor callable. The real ``McpService`` list and call paths
run here against the real rule (``utils/mcp_exposed_version.py``), with the project's agents and the predict stubbed."""
import importlib.util
import json
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "plugins.elitea_core"


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


def version(version_id, name, *tags):
    return types.SimpleNamespace(id=version_id, name=name, tags=[types.SimpleNamespace(name=tag) for tag in tags])


def agent(agent_id, name, versions, default=None):
    by_id = {item.id: item for item in versions}
    chosen = by_id.get(default) or next((item for item in versions if item.name == "base"), None)
    return types.SimpleNamespace(id=agent_id, name=name, description="does things", versions=versions,
                                 get_default_version=lambda: chosen)


class Server:
    def __init__(self, module, monkeypatch, applications, tags=(7,), governed=False, bound=None, toolkits=(),
                 resource=(None, None), category=None):
        self.module, self.sent, self.predicted, self.listed_with, self.tools_run = module, [], [], [], []
        self.toolkit_reads = 0
        models = types.ModuleType(f"{PKG}.models")
        models.__path__ = []
        identity = types.ModuleType(f"{PKG}.models.agent_identity")
        identity.AgentIdentityBinding = object
        governance = types.ModuleType(f"{PKG}.utils.governance")
        governance.is_governed = lambda plugin: governed
        for stub in (models, identity, governance):
            monkeypatch.setitem(sys.modules, stub.__name__, stub)
        for name in [name for name in sys.modules if name == "sqlalchemy" or name.startswith("sqlalchemy.")]:
            if getattr(sys.modules[name], "__file__", None) is None:
                monkeypatch.delitem(sys.modules, name)
        spec = importlib.util.spec_from_file_location(
            f"{PKG}.utils.mcp_exposed_version", PLUGIN_ROOT / "utils" / "mcp_exposed_version.py")
        rule = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, rule)
        spec.loader.exec_module(rule)
        monkeypatch.setattr(rule, "bound_version_ids", lambda session, project_id, ids: bound or {})

        def list_applications_api(**kwargs):
            self.listed_with.append((kwargs.get("tags"), kwargs.get("ids")))
            ids = kwargs.get("ids")
            return {"applications": [item for item in applications if ids is None or item.id in ids]}

        def do_predict(**kwargs):
            self.predicted.append(kwargs["version_id"])
            return {"chat_history": [{"content": f"ran {kwargs['version_id']}"}]}

        def do_runtool(**kwargs):
            self.tools_run.append((kwargs["toolkit_id"], kwargs["tool_name"]))
            return {"result": "toolkit ran"}

        class Session:
            def query(self, column):
                return self

            def filter(self, condition):
                return self

            def scalar(self):
                return next((item.id for item in applications
                             if any(found.id == resource[1] for found in item.versions)), None)

        @contextmanager
        def get_session(project_id):
            yield Session()

        monkeypatch.setattr(module, "ApplicationVersion", types.SimpleNamespace(id=object(), application_id=object()))
        monkeypatch.setattr(module, "list_applications_api", list_applications_api)
        def toolkits_listing(**kwargs):
            self.toolkit_reads += 1
            return {"rows": list(toolkits)}

        monkeypatch.setattr(module, "toolkits_listing", toolkits_listing)
        monkeypatch.setattr(module.db, "get_session", get_session, raising=False)
        monkeypatch.setattr(module.this, "module", types.SimpleNamespace(do_predict=do_predict, do_runtool=do_runtool), raising=False)
        monkeypatch.setattr(module, "_refused_by_run_token", lambda *args: None)
        self.service = module.McpService.__new__(module.McpService)
        self.service.session = types.SimpleNamespace(
            project_id=1, tags=list(tags), resource_type=resource[0], resource_id=resource[1], entity_category=category,
            dispatch_message=self.sent.append)

    def tools(self):
        request = types.SimpleNamespace(id=1, model_dump=lambda: {})
        self.service._McpService__handle_list_tools_request(request)
        return {tool["name"]: tool for tool in json.loads(self.sent[-1])["result"]["tools"]}

    def call(self, name):
        request = types.SimpleNamespace(id=2, params=types.SimpleNamespace(name=name, arguments={"task": "hi"}))
        self.service._McpService__handle_call_tool_request(request)
        result = json.loads(self.sent[-1])["result"]
        return result["isError"], result["content"][0]["text"]


@pytest.fixture()
def server(mcp_service_module, monkeypatch):
    return lambda applications, **kwargs: Server(mcp_service_module, monkeypatch, applications, **kwargs)


def test_the_ticket_case_the_tagged_version_runs_not_the_newer_untagged_one(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2")])])
    assert sorted(mcp.tools()) == ["Helper"]
    assert mcp.call("Helper") == (False, "ran 10") and mcp.predicted == [10]


def test_every_tagged_version_is_listed_and_each_tool_runs_its_own_version(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp"), version(12, "v3")])])
    tools = mcp.tools()
    assert sorted(tools) == ["Helper", "Helper_v2"]
    assert tools["Helper_v2"]["description"] == "Version 'v2' of agent 'Helper'. does things"
    assert tools["Helper"]["description"] == "does things"
    assert tools["Helper_v2"]["inputSchema"]["required"] == ["task"]
    assert mcp.call("Helper_v2") == (False, "ran 11") and mcp.call("Helper") == (False, "ran 10")


def test_with_only_a_non_default_version_tagged_the_agent_name_is_not_a_tool(server):
    mcp = server([agent(1, "Helper", [version(10, "base"), version(11, "v2", "mcp")])])
    assert sorted(mcp.tools()) == ["Helper_v2"]
    is_error, text = mcp.call("Helper")
    assert is_error and "Helper" in text and mcp.predicted == []
    assert mcp.call("Helper_v2") == (False, "ran 11")


def test_an_untagged_version_cannot_be_called_under_any_name(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2")])])
    for name in ("Helper_v2", "Helper_base", "v2"):
        assert mcp.call(name)[0] is True
    assert mcp.predicted == []


def test_a_project_without_the_mcp_tag_lists_and_runs_no_agent(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp")])], tags=())
    assert mcp.tools() == {}
    assert mcp.call("Helper")[0] is True
    assert mcp.predicted == [] and mcp.listed_with == []


def test_the_agents_are_asked_for_by_the_projects_mcp_tag(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp")])])
    mcp.tools()
    assert mcp.listed_with == [([7], None)]


def test_under_enforce_only_a_tagged_and_bound_version_is_listed_and_callable(server):
    applications = [agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])]
    mcp = server(applications, governed=True, bound={1: {11}})
    assert sorted(mcp.tools()) == ["Helper_v2"]
    assert mcp.call("Helper")[0] is True and mcp.predicted == []
    assert mcp.call("Helper_v2") == (False, "ran 11")


def test_a_toolkit_tool_keeps_a_name_an_agent_version_would_also_take_in_the_list_and_on_a_call(server):
    toolkit = {"id": 5, "name": "Helper", "type": "t", "description": "", "settings": {"selected_tools": ["v2"]},
               "meta": {"mcp_options": {"available_by_mcp": True}}}
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])], toolkits=[toolkit])
    tools = mcp.tools()
    assert sorted(tools) == ["Helper", "Helper_v2"]
    assert tools["Helper_v2"]["description"].startswith("Tool 'v2' from toolkit")
    assert mcp.call("Helper_v2") == (False, '"toolkit ran"')
    assert mcp.tools_run == [(5, "v2")] and mcp.predicted == []
    assert mcp.call("Helper") == (False, "ran 10")


def test_the_version_scoped_endpoint_lists_and_runs_the_version_it_names(server):
    applications = [agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp"), version(12, "v3")])]
    mcp = server(applications, resource=("application", 11))
    tools = mcp.tools()
    assert sorted(tools) == ["Helper"]
    assert tools["Helper"]["description"] == "Version 'v2' of agent 'Helper'. does things"
    assert mcp.call("Helper") == (False, "ran 11")
    assert mcp.call("Helper_v2")[0] is True and mcp.predicted == [11]


def test_the_version_scoped_endpoint_serves_no_untagged_version(server):
    applications = [agent(1, "Helper", [version(10, "base", "mcp"), version(12, "v3")])]
    mcp = server(applications, resource=("application", 12))
    assert mcp.tools() == {}
    assert mcp.call("Helper")[0] is True and mcp.predicted == []


def test_the_version_scoped_endpoint_keeps_its_tool_when_another_agent_holds_the_version_name(server):
    applications = [agent(1, "Helper_v2", [version(5, "base", "mcp")]),
                    agent(2, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])]
    assert sorted(server(applications).tools()) == ["Helper", "Helper_v2"]
    mcp = server(applications, resource=("application", 11))
    assert sorted(mcp.tools()) == ["Helper"]
    assert mcp.call("Helper") == (False, "ran 11")


def test_the_version_scoped_endpoint_reads_its_own_agent_only(server):
    applications = [agent(1, "Other", [version(5, "base", "mcp")]),
                    agent(2, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])]
    mcp = server(applications, resource=("application", 11))
    mcp.tools()
    assert mcp.listed_with == [([7], [2])]


def test_the_version_scoped_endpoint_of_an_unknown_version_serves_nothing(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp")])], resource=("application", 999))
    assert mcp.tools() == {} and mcp.listed_with == []


def test_a_call_to_a_tool_named_after_its_agent_reads_no_toolkits(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])])
    assert mcp.call("Helper") == (False, "ran 10") and mcp.toolkit_reads == 0
    assert mcp.call("Helper_v2") == (False, "ran 11") and mcp.toolkit_reads == 1


def test_a_version_scoped_session_runs_its_agent_not_a_toolkit_tool_it_never_listed(server):
    toolkit = {"id": 5, "name": "Jira", "type": "t", "description": "", "settings": {"selected_tools": ["search"]},
               "meta": {"mcp_options": {"available_by_mcp": True}}}
    applications = [agent(1, "Jira_search", [version(10, "base", "mcp"), version(11, "v2", "mcp")])]
    mcp = server(applications, toolkits=[toolkit], resource=("application", 11))
    assert sorted(mcp.tools()) == ["Jira_search"]
    assert mcp.call("Jira_search") == (False, "ran 11")
    assert mcp.tools_run == [] and mcp.toolkit_reads == 0


def test_a_toolkit_tool_no_agent_is_named_like_still_runs(server):
    toolkit = {"id": 5, "name": "Jira", "type": "t", "description": "", "settings": {"selected_tools": ["search"]},
               "meta": {"mcp_options": {"available_by_mcp": True}}}
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp")])], toolkits=[toolkit])
    assert mcp.call("Jira_search") == (False, '"toolkit ran"')
    assert mcp.tools_run == [(5, "search")] and mcp.predicted == []


def test_a_name_two_toolkits_share_is_refused_not_given_to_the_agent_version_named_alike(server):
    toolkits = [{"id": tid, "name": name, "type": "t", "description": "", "settings": {"selected_tools": ["x"]},
                 "meta": {"mcp_options": {"available_by_mcp": True}}} for tid, name in ((5, "Jira"), (6, "Jira"))]
    mcp = server([agent(1, "Jira", [version(10, "base", "mcp"), version(11, "x", "mcp")])], toolkits=toolkits)
    assert mcp.tools()["Jira_x"]["description"].startswith("Tool 'x' from toolkit")
    assert mcp.call("Jira_x")[0] is True
    assert mcp.predicted == [] and mcp.tools_run == []


def test_a_toolkit_scoped_session_runs_no_agent(server):
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])],
                 resource=("toolkit", 5))
    assert mcp.call("Helper")[0] is True
    assert mcp.call("Helper_v2")[0] is True
    assert mcp.predicted == [] and mcp.listed_with == []


def test_a_category_session_runs_its_listed_api_tool_not_an_agent_version_named_alike(server, monkeypatch):
    api_tool = {"value": "Helper_v2", "description": "", "args_schema": {}}
    executed = []
    mcp = server([agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])],
                 category="elitea_core/applications")
    monkeypatch.setattr(mcp.module.openapi_registry, "get_mcp_api_tools", lambda *args, **kwargs: [api_tool])
    monkeypatch.setattr(mcp.module.McpApiToolExecutor, "execute",
                        staticmethod(lambda tool, *args: executed.append(tool["value"]) or {"result": "api ran"}))
    assert mcp.call("Helper_v2") == (False, '"api ran"')
    assert mcp.call("Helper")[0] is True
    assert executed == ["Helper_v2"] and mcp.predicted == [] and mcp.listed_with == []
