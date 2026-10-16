"""Issue #7048 - the REST test-tool endpoint resolves the project's default model itself.

A failure of that lookup must come back as the endpoint's JSON error, never as an unhandled 500.
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = "plugins.elitea_core"
REQUEST_BODY = {
    "toolkit_config": {"type": "artifact", "toolkit_name": "files", "settings": {}},
    "tool_name": "list_files",
}


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


def _load(name, relpath):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relpath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _Harness:
    def __init__(self):
        self.resolve_error = None
        self.dispatched = []

    def resolve(self, project_id, llm_model, llm_settings):
        if self.resolve_error:
            raise self.resolve_error
        return "project-default", {"model_name": "project-default"}

    def test_toolkit_tool_sio(self, sid, data, event, await_task_timeout):
        self.dispatched.append(data)
        return {"task_id": "task-1", "result": {"success": True}}


@pytest.fixture
def endpoint(isolated_sys_modules):
    harness = _Harness()
    for name in ("plugins", PACKAGE, f"{PACKAGE}.api", f"{PACKAGE}.api.v2",
                 f"{PACKAGE}.utils", f"{PACKAGE}.models", f"{PACKAGE}.models.pd"):
        _package(name)
    _module("flask", request=types.SimpleNamespace(json=REQUEST_BODY, args={}))
    _module(
        "tools",
        api_tools=types.SimpleNamespace(
            APIModeHandler=type("APIModeHandler", (), {}),
            APIBase=type("APIBase", (), {}),
            endpoint_metrics=lambda func: func,
            with_modes=lambda params: params,
        ),
        auth=types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=lambda descriptor: (lambda f: f))),
        config=types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default"),
        serialize=lambda value: value,
    )
    _module(f"{PACKAGE}.utils.constants", PROMPT_LIB_MODE="prompt_lib")
    _module(f"{PACKAGE}.utils.sio_utils", SioValidationError=type("SioValidationError", (Exception,), {}))
    _module(f"{PACKAGE}.utils.exceptions", PoolSaturationError=type("PoolSaturationError", (Exception,), {}))
    _module(f"{PACKAGE}.utils.predict_utils", resolve_test_tool_llm=harness.resolve)
    _module("pylon", __path__=[])
    _module("pylon.core", __path__=[])
    _module("pylon.core.tools", log=types.SimpleNamespace(
        debug=lambda *a, **k: None, error=lambda *a, **k: None, info=lambda *a, **k: None))
    _load(f"{PACKAGE}.models.pd.test_toolkit_tool", "models/pd/test_toolkit_tool.py")
    api = _load(f"{PACKAGE}.api.v2.test_toolkit_tool", "api/v2/test_toolkit_tool.py")
    handler = types.SimpleNamespace(module=harness)

    def post():
        return api.PromptLibAPI.post(handler, project_id=3)
    return post, harness


def test_a_failed_model_lookup_returns_the_json_error(endpoint):
    post, harness = endpoint
    harness.resolve_error = RuntimeError("configurations unavailable")

    body, status = post()

    assert status == 500
    assert body == {"error": "configurations unavailable"}
    assert harness.dispatched == []


def test_the_resolved_model_is_dispatched(endpoint):
    post, harness = endpoint

    body, status = post()

    assert status == 200
    assert harness.dispatched[0]["llm_model"] == "project-default"
    assert harness.dispatched[0]["llm_settings"] == {"model_name": "project-default"}
