"""toolkit_discover_tools requires unsecret in addition to tool.details.

The two check_api decorators stack as AND: both must pass.
"""

import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = "plugins.elitea_core"

TOOL_DETAILS = "models.applications.tool.details"
UNSECRET = "configuration.secrets.secret.unsecret"
TARGET_URL = "http://example.invalid/"


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


class Harness:
    def __init__(self):
        self.user_permissions = set()
        self.descriptors = []
        self.expand_calls = []
        self.discover_calls = []

    def check_api(self, descriptor):
        required = set(descriptor["permissions"])
        self.descriptors.append(descriptor)

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.user_permissions:
                    return {"ok": False, "error": "access_denied"}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ("plugins", PACKAGE, f"{PACKAGE}.api", f"{PACKAGE}.api.v2", f"{PACKAGE}.utils"):
        _package(name)

    body = {"settings": {"url": TARGET_URL, "credentials": {"elitea_title": "stored", "private": False}}}
    _module("flask", request=types.SimpleNamespace(json=body))

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
        db=types.SimpleNamespace(),
    )

    def expand_toolkit_settings(type_, settings, project_id, user_id):
        # Real code resolves stored credentials here; record that it was reached
        harness.expand_calls.append(settings)
        return {**settings, "credentials": {"token": "resolved-secret"}}

    _module(f"{PACKAGE}.utils.constants", PROMPT_LIB_MODE="prompt_lib")
    _module(
        f"{PACKAGE}.utils.application_tools",
        expand_toolkit_settings=expand_toolkit_settings,
        ValidatorNotSupportedError=type("ValidatorNotSupportedError", (Exception,), {}),
        ConfigurationExpandError=type("ConfigurationExpandError", (Exception,), {}),
    )
    _module(
        f"{PACKAGE}.utils.internal_tools",
        resolve_internal_mcp_settings=lambda settings, *a, **k: settings,
    )

    module_name = f"{PACKAGE}.api.v2.toolkit_discover_tools"
    spec = importlib.util.spec_from_file_location(
        module_name, PLUGIN_ROOT / "api" / "v2" / "toolkit_discover_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    handler = module.PromptLibAPI()

    def discover_mcp_tools(toolkit_type, settings):
        harness.discover_calls.append(settings)
        return {"tools": []}

    handler.module = types.SimpleNamespace(
        discover_mcp_tools=discover_mcp_tools,
        get_mcp_prebuilt_config=lambda toolkit_type: {},
    )
    harness.handler = handler
    return harness


def test_both_permissions_are_declared(harness):
    assert [d["permissions"] for d in harness.descriptors] == [[TOOL_DETAILS], [UNSECRET]]


def test_unsecret_recommended_roles_exclude_viewer(harness):
    unsecret = next(d for d in harness.descriptors if d["permissions"] == [UNSECRET])
    expected = {"admin": True, "editor": True, "viewer": False}
    assert unsecret["recommended_roles"] == {"administration": expected, "default": expected}


def test_viewer_with_only_tool_details_is_denied_before_expansion(harness):
    # Viewer-like caller: tool.details passes, unsecret fails -> AND must deny
    harness.user_permissions = {TOOL_DETAILS}

    response, status = harness.handler.post(project_id=1, toolkit_type="mcp_test")

    assert status == 403
    assert response["error"] == "access_denied"
    assert harness.expand_calls == [], "stored credentials must not be expanded for a denied caller"
    assert harness.discover_calls == [], "no connection may be made for a denied caller"


def test_unsecret_alone_is_not_enough(harness):
    harness.user_permissions = {UNSECRET}

    _, status = harness.handler.post(project_id=1, toolkit_type="mcp_test")

    assert status == 403
    assert harness.discover_calls == []


def test_caller_with_both_permissions_discovers_tools(harness):
    harness.user_permissions = {TOOL_DETAILS, UNSECRET}

    response, status = harness.handler.post(project_id=1, toolkit_type="mcp_test")

    assert status == 200
    assert response == {"success": True, "tools": []}
    assert harness.discover_calls == [
        {"url": TARGET_URL, "credentials": {"token": "resolved-secret"}}
    ]


@pytest.mark.parametrize("toolkit_type", ["github", "openapi", "mcp", "McpFoo", "xmcp_test"])
def test_non_mcp_type_is_rejected_before_expansion_and_discovery(harness, toolkit_type):
    # Non-MCP types must never reach credential expansion or the indexer connection check
    harness.user_permissions = {TOOL_DETAILS, UNSECRET}

    response, status = harness.handler.post(project_id=1, toolkit_type=toolkit_type)

    assert status == 400
    assert response == {"success": False, "error": "toolkit_type must be an MCP type (mcp_*)"}
    assert harness.expand_calls == []
    assert harness.discover_calls == []


def test_non_mcp_type_without_permissions_is_still_denied(harness):
    # Permission check runs first, so the type error does not leak to unauthorized callers
    harness.user_permissions = {TOOL_DETAILS}

    _, status = harness.handler.post(project_id=1, toolkit_type="github")

    assert status == 403
