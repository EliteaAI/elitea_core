"""GET tool/<pid>/<id>?expand=true needs tool.update and full folder access."""

import functools
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = "plugins.elitea_core"

TOOL_DETAILS = "models.applications.tool.details"
TOOL_UPDATE = "models.applications.tool.update"


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


def _load(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Harness:
    def __init__(self):
        self.user_permissions = set()
        self.folder_level = "full"
        self.args = {}
        self.expand_calls = []

    def check_api(self, descriptor, project_id=None):
        required = set(descriptor["permissions"])

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.user_permissions:
                    return {"ok": False, "error": "access_denied"}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def call(self, **args):
        self.args.clear()
        self.args.update(args)
        return self.handler.get(project_id=1, tool_id=7)


class _Row:
    settings = {"connection_string": "{{secret.x}}"}
    type = "artifact"


class _ToolDetails:
    @classmethod
    def from_orm(cls, _row):
        return cls()

    def fix_name(self, *_): pass
    def set_online(self, *_): pass
    def set_agent_meta_and_fields(self, *_): pass
    def check_is_pinned(self, *_): pass

    def model_dump(self, mode=None):
        return {"type": "artifact", "settings": dict(_Row.settings)}


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ("plugins", PACKAGE, f"{PACKAGE}.api", f"{PACKAGE}.api.v2",
                 f"{PACKAGE}.utils", f"{PACKAGE}.models", f"{PACKAGE}.models.enums",
                 f"{PACKAGE}.models.pd", "pylon", "pylon.core"):
        _package(name)

    _module("flask", request=types.SimpleNamespace(args=harness.args))
    _module("pydantic", ValidationError=type("ValidationError", (Exception,), {}))
    _module("yaml", safe_load=lambda s: s, YAMLError=type("YAMLError", (Exception,), {}))
    _module("pylon.core.tools", log=types.SimpleNamespace(
        debug=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None,
    ))

    class _Query:
        def filter(self, *_): return self
        def first(self): return _Row()

    @contextmanager
    def with_project_schema_session(_project_id):
        yield types.SimpleNamespace(query=lambda *_: _Query())

    rpc = types.SimpleNamespace(
        timeout=lambda _t: types.SimpleNamespace(
            social_resolve_entity_access=lambda **kw: harness.folder_level
        )
    )
    _module(
        "tools",
        api_tools=types.SimpleNamespace(
            APIModeHandler=type("APIModeHandler", (), {}),
            APIBase=type("APIBase", (), {}),
            endpoint_metrics=lambda func: func,
            with_modes=lambda params: params,
        ),
        auth=types.SimpleNamespace(
            decorators=types.SimpleNamespace(check_api=harness.check_api),
            current_user=lambda: {"id": 42},
        ),
        config=types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default"),
        db=types.SimpleNamespace(with_project_schema_session=with_project_schema_session),
        rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        register_openapi=lambda **kw: (lambda func: func),
    )

    def expand_toolkit_settings(type_, settings, project_id, user_id):
        harness.expand_calls.append(settings)
        return {"connection_string": "resolved-secret"}

    _module(f"{PACKAGE}.models.all", ApplicationVersion=object)
    _module(f"{PACKAGE}.models.enums.events", ApplicationEvents=types.SimpleNamespace())
    _module(f"{PACKAGE}.models.elitea_tools", EliteATool=types.SimpleNamespace(id=1), EntityToolMapping=object)
    _module(
        f"{PACKAGE}.models.pd.tool",
        ToolDetails=_ToolDetails, ToolAPIUpdateModel=object, ToolUpdateRelationModel=object,
    )
    _module(f"{PACKAGE}.models.enums.all", ToolEntityTypes=types.SimpleNamespace(), AgentTypes=types.SimpleNamespace())
    _module(f"{PACKAGE}.utils.constants", PROMPT_LIB_MODE="prompt_lib")
    _module(f"{PACKAGE}.utils.pipeline_utils", from_str_to_yaml=lambda s: s)
    _module(
        f"{PACKAGE}.utils.application_tools",
        delete_toolkit=None,
        toolkit_change_relation=None,
        ToolkitChangeRelationError=type("ToolkitChangeRelationError", (Exception,), {}),
        expand_toolkit_settings=expand_toolkit_settings,
        ValidatorNotSupportedError=type("ValidatorNotSupportedError", (Exception,), {}),
        ConfigurationExpandError=type("ConfigurationExpandError", (Exception,), {}),
        wrap_provider_hub_secret_fields=None,
    )
    _load(f"{PACKAGE}.utils.folder_access", "utils/folder_access.py")
    _load(f"{PACKAGE}.utils.permissions", "utils/permissions.py")
    module = _load(f"{PACKAGE}.api.v2.tool", "api/v2/tool.py")

    harness.handler = module.PromptLibAPI()
    return harness


def test_viewer_with_expand_is_forbidden_and_nothing_is_expanded(harness):
    harness.user_permissions = {TOOL_DETAILS}

    response, status = harness.call(expand="true")

    assert status == 403
    assert harness.expand_calls == []


def test_viewer_without_expand_gets_the_unexpanded_row(harness):
    harness.user_permissions = {TOOL_DETAILS}

    response, status = harness.call()

    assert status == 200
    assert response["settings"] == {"connection_string": "{{secret.x}}"}
    assert harness.expand_calls == []


def test_editor_with_expand_gets_resolved_settings(harness):
    harness.user_permissions = {TOOL_DETAILS, TOOL_UPDATE}

    response, status = harness.call(expand="true")

    assert status == 200
    assert response["settings"] == {"connection_string": "resolved-secret"}


def test_editor_with_read_only_folder_is_forbidden_with_expand(harness):
    harness.user_permissions = {TOOL_DETAILS, TOOL_UPDATE}
    harness.folder_level = "read_only"

    response, status = harness.call(expand="true")

    assert status == 403
    assert harness.expand_calls == []


def test_editor_with_read_only_folder_still_reads_without_expand(harness):
    harness.user_permissions = {TOOL_DETAILS, TOOL_UPDATE}
    harness.folder_level = "read_only"

    response, status = harness.call()

    assert status == 200
    assert harness.expand_calls == []
