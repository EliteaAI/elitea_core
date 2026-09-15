"""Scope resolution for admin-defined MCP servers, across both key spellings.

Admin MCP server definitions in pylon.yml declare `scope` (singular) while toolkit settings
and the OAuth token proxy read `scopes` (plural). Without alias handling the backend cannot
resolve scope on its own and depends on the browser sending it.

Run via:
    python tests/run_tests.py integration/test_mcp_prebuilt_scope_resolution.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

SCOPE = "https://mcp.example.test/mcp/user_impersonation offline_access"


@pytest.fixture(scope="module")
def method_class():
    # Sibling tests swap sys.modules['pylon.core.tools'] for stubs of their own, so install a
    # self-contained one here rather than depending on collection order.
    previous = {name: sys.modules.get(name) for name in ("pylon", "pylon.core", "pylon.core.tools")}
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(**{
        level: lambda *a, **k: None
        for level in ("debug", "info", "warning", "error", "exception")
    })
    pylon_tools.web = types.SimpleNamespace(method=lambda *a, **k: (lambda func: func))
    sys.modules["pylon"] = types.ModuleType("pylon")
    sys.modules["pylon.core"] = types.ModuleType("pylon.core")
    sys.modules["pylon.core.tools"] = pylon_tools

    spec = importlib.util.spec_from_file_location(
        "mcp_prebuilt_config_under_test",
        PLUGIN_ROOT / "methods" / "mcp_prebuilt_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        yield module.Method
    finally:
        for name, original in previous.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def _resolver(method_class, server_config):
    instance = method_class()
    instance.mcp_prebuilt_configs = {"example_crm": server_config}
    return instance.resolve_mcp_prebuilt_settings


@pytest.mark.parametrize("definition_key", ["scope", "scopes"])
def test_scope_resolved_under_either_definition_spelling(method_class, definition_key):
    resolve = _resolver(method_class, {"url": "https://mcp.example.test/mcp", definition_key: SCOPE})

    assert resolve({"toolkit_type": "mcp_example_crm"})["scopes"] == SCOPE


@pytest.mark.parametrize("existing_key", ["scope", "scopes"])
def test_incoming_scope_wins_under_either_spelling(method_class, existing_key):
    resolve = _resolver(method_class, {"scope": SCOPE})

    result = resolve({"toolkit_type": "mcp_example_crm", existing_key: "from-settings"})

    assert result[existing_key] == "from-settings"
    assert "scopes" not in result or result["scopes"] == "from-settings"


def test_list_scope_is_passed_through_untouched(method_class):
    """The token proxy joins lists; resolution must not silently reshape the value."""
    resolve = _resolver(method_class, {"scope": ["a", "b"]})

    assert resolve({"toolkit_type": "mcp_example_crm"})["scopes"] == ["a", "b"]


def test_other_fields_still_injected(method_class):
    resolve = _resolver(
        method_class,
        {"url": "https://mcp.example.test/mcp", "client_id": "cid", "timeout": 30},
    )

    result = resolve({"toolkit_type": "mcp_example_crm"})

    assert result["url"] == "https://mcp.example.test/mcp"
    assert result["client_id"] == "cid"
    assert result["timeout"] == 30
    assert "scopes" not in result


def test_definition_without_scope_injects_nothing(method_class):
    resolve = _resolver(method_class, {"url": "https://mcp.example.test/mcp"})

    assert "scopes" not in resolve({"toolkit_type": "mcp_example_crm"})
    assert "scope" not in resolve({"toolkit_type": "mcp_example_crm"})
