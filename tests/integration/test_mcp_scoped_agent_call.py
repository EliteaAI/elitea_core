"""An agent-scoped MCP session (``/mcp/agent/<version_id>``) can call the agent it lists, whatever its tags.

``tools/list`` on that endpoint lists the session's agent by version id; ``tools/call`` resolves the name through
``McpService.__get_ver_id_by_agent_name``. The ``mcp`` tag filter must only pick agents for the project-wide endpoint.
This suite runs the real resolver with its module dependencies faked.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "plugins.elitea_core"


class _Column:
    def __eq__(self, other):
        return other


class Application:
    versions = None


class ApplicationVersion:
    id = _Column()
    application = None
    tags = None


def _install(monkeypatch, name, **attrs):
    module = sys.modules.get(name) if name in ("plugins", "pylon", "pylon.core") else None
    module = module or types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    monkeypatch.setitem(sys.modules, name, module)


class _Query:
    def __init__(self, world, model):
        self.world, self.model = world, model

    def options(self, *_):
        return self

    def filter(self, version_id):
        self.version_id = version_id
        return self

    def first(self):
        return self.world.versions.get(self.version_id)

    def get(self, application_id):
        return self.world.applications.get(application_id)


@pytest.fixture
def world(monkeypatch):
    state = types.SimpleNamespace(versions={}, applications={}, tagged=[], listed_with=[], governed=False)

    class Session:
        def query(self, model):
            return _Query(state, model)

    @contextmanager
    def session_for(_project_id):
        yield Session()

    def list_applications_api(**kwargs):
        state.listed_with.append(kwargs["tags"])
        return {"applications": state.tagged}

    log = types.SimpleNamespace(**{level: (lambda *a, **k: None)
                                   for level in ("info", "warning", "error", "debug", "exception")})
    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.utils", "pylon", "pylon.core"):
        _install(monkeypatch, name)
    _install(monkeypatch, "pylon.core.tools", log=log)
    _install(monkeypatch, "sqlalchemy.orm", joinedload=lambda *_: types.SimpleNamespace(joinedload=lambda *_: None))
    _install(monkeypatch, "tools", db=types.SimpleNamespace(get_session=session_for,
                                                             with_project_schema_session=session_for),
             auth=types.SimpleNamespace(current_user=lambda: {"id": 1}), this=types.SimpleNamespace(module=None),
             openapi_registry=types.SimpleNamespace(get_mcp_api_tools=lambda *a, **k: []),
             sanitize_property_name=lambda name: name)
    _install(monkeypatch, f"{PKG}.models.all", Application=Application, ApplicationVersion=ApplicationVersion)
    _install(monkeypatch, f"{PKG}.utils.application_tools", toolkits_listing=lambda **_: {"rows": []})
    _install(monkeypatch, f"{PKG}.utils.application_utils", list_applications_api=list_applications_api)
    _install(monkeypatch, f"{PKG}.utils.toolkits_utils", get_toolkit_schemas=lambda *a, **k: {})
    _install(monkeypatch, f"{PKG}.utils.exceptions", AdmissionDenied=type("AdmissionDenied", (Exception,), {}),
             InputGuardrailBlockedError=type("InputGuardrailBlockedError", (Exception,), {}),
             PoolSaturationError=type("PoolSaturationError", (Exception,), {}))
    _install(monkeypatch, f"{PKG}.utils.internal_tools", MCP_CURRENT_PROJECT_SUFFIXES=set(),
             MCP_PROJECT_SCOPED_SUFFIXES=set())
    _install(monkeypatch, f"{PKG}.utils.mcp_session", SseSession=type("SseSession", (), {}))
    _install(monkeypatch, f"{PKG}.utils.mcp_versioning", INTERNAL_MCP_ENVIRON_KEY="elitea.internal_mcp_request")
    _install(monkeypatch, f"{PKG}.utils.pipeline_execution", TriggerType=types.SimpleNamespace(MCP="mcp"))
    _install(monkeypatch, f"{PKG}.utils.governance", is_governed=lambda _module: state.governed)
    _install(monkeypatch, f"{PKG}.utils.mcp_exposed_version",
             bound_version_ids=lambda *_: {},
             exposed_version=lambda versions, _bound, _governed: (versions[0], "stored") if versions else (None, None))

    spec = importlib.util.spec_from_file_location(f"{PKG}.utils.mcp_service", PLUGIN_ROOT / "utils" / "mcp_service.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)

    def add_agent(application_id, name, version_ids, tagged=False):
        app = types.SimpleNamespace(id=application_id, name=name)
        versions = [types.SimpleNamespace(id=version_id, application_id=application_id, application=app)
                    for version_id in version_ids]
        state.applications[application_id] = types.SimpleNamespace(id=application_id, versions=versions)
        state.versions.update({version.id: version for version in versions})
        if tagged:
            state.tagged.append(app)

    def resolve(name, resource_type=None, resource_id=None):
        service = module.McpService.__new__(module.McpService)
        service.session = types.SimpleNamespace(project_id=2, tags=[6], resource_type=resource_type,
                                                resource_id=resource_id)
        return service._McpService__get_ver_id_by_agent_name(name)

    state.add_agent, state.resolve = add_agent, resolve
    return state


def test_the_scoped_agent_resolves_without_the_mcp_tag(world):
    world.add_agent(85, "fd-7128 Echo", [127])

    assert world.resolve("fd-7128_Echo", "application", 127) == 127
    assert world.listed_with == []   # never went through the tag-filtered listing


def test_the_scoped_agent_runs_its_exposed_version(world):
    world.add_agent(85, "Echo", [130, 127])

    assert world.resolve("Echo", "application", 127) == 130


def test_another_agents_name_on_a_scoped_session_still_needs_the_tag(world):
    world.add_agent(85, "Echo", [127])
    world.add_agent(86, "Other", [140])

    assert world.resolve("Other", "application", 127) is None
    assert world.listed_with == [[6]]


def test_a_tagged_agent_still_resolves_on_a_scoped_session(world):
    world.add_agent(85, "Echo", [127])
    world.add_agent(86, "Other", [140], tagged=True)

    assert world.resolve("Other", "application", 127) == 140


def test_a_missing_scoped_version_falls_back_to_the_tagged_listing(world):
    assert world.resolve("Echo", "application", 999) is None
    assert world.listed_with == [[6]]


def test_the_project_wide_session_still_needs_the_mcp_tag(world):
    world.add_agent(85, "Echo", [127])

    assert world.resolve("Echo") is None
    world.add_agent(85, "Echo", [127], tagged=True)
    assert world.resolve("Echo") == 127


def test_a_toolkit_scoped_session_does_not_resolve_agents_by_version(world):
    world.add_agent(85, "Echo", [127])

    assert world.resolve("Echo", "toolkit", 127) is None
