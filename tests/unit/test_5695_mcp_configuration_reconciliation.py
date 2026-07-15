"""EL-5695 dynamic MCP configuration registry reconciliation."""

import importlib.util
from pathlib import Path
import sys
import types
from unittest.mock import Mock


PLUGIN_ROOT = Path(__file__).parents[2]


class _RpcManager:
    def __init__(self, entries):
        self.entries = entries
        self.registered = []
        self.unregistered = []

    def timeout(self, _seconds):
        return self

    def configurations_list_types(self):
        return list(self.entries)

    def configurations_register(self, **kwargs):
        self.registered.append(kwargs)

    def configurations_unregister(self, **kwargs):
        self.unregistered.append(kwargs)


def _entry(
    name,
    *,
    validator="applications_configuration_validator",
    section="toolkits",
    config_schema=None,
):
    if config_schema is None and name.startswith("mcp_"):
        config_schema = {
            "metadata": {
                "mcp_server_name": name.removeprefix("mcp_"),
            },
        }
    return types.SimpleNamespace(
        type=name,
        section=section,
        validation_func=validator,
        config_schema=config_schema,
    )


def _schema(title):
    return {
        "title": title,
        "metadata": {
            "section": "toolkits",
            "check_connection_supported": True,
            "mcp_server_name": title,
        },
    }


def _load_method(monkeypatch, rpc_manager):
    pylon = types.ModuleType("pylon")
    pylon_core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=Mock(), warning=Mock(), error=Mock(), debug=Mock(),
    )

    class _Web:
        @staticmethod
        def method(*_args, **_kwargs):
            return lambda function: function

    pylon_tools.web = _Web()
    monkeypatch.setitem(sys.modules, "pylon", pylon)
    monkeypatch.setitem(sys.modules, "pylon.core", pylon_core)
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)

    tools = types.ModuleType("tools")
    tools.context = types.SimpleNamespace(rpc_manager=rpc_manager)
    monkeypatch.setitem(sys.modules, "tools", tools)

    spec = importlib.util.spec_from_file_location(
        "elitea_core_toolkits_el5695",
        PLUGIN_ROOT / "methods" / "toolkits.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _method_instance(module, previous=None):
    instance = module.Method()
    instance.configuration_schemas = previous or {}
    instance.toolkit_configurations_ready_event = types.SimpleNamespace(set=Mock())
    return instance


def test_add_registers_new_mcp_configuration(monkeypatch):
    rpc = _RpcManager([])
    module = _load_method(monkeypatch, rpc)
    instance = _method_instance(module)

    instance.toolkit_configurations_collected(None, {"mcp_new": _schema("New")})

    assert rpc.registered == [{
        "type_name": "mcp_new",
        "section": "toolkits",
        "config_schema": _schema("New"),
        "validation_func": "applications_configuration_validator",
        "check_connection_func": "applications_configuration_check_connection",
    }]


def test_edit_replaces_owned_mcp_configuration(monkeypatch):
    rpc = _RpcManager([_entry("mcp_remote", config_schema=_schema("Old"))])
    module = _load_method(monkeypatch, rpc)
    instance = _method_instance(module, {"mcp_remote": _schema("Old")})

    instance.toolkit_configurations_collected(None, {"mcp_remote": _schema("New")})

    assert rpc.registered[0]["type_name"] == "mcp_remote"
    assert rpc.registered[0]["replace"] is True
    assert rpc.registered[0]["config_schema"] == _schema("New")


def test_delete_unregisters_owned_mcp_configuration(monkeypatch):
    rpc = _RpcManager([_entry("mcp_removed"), _entry("github")])
    module = _load_method(monkeypatch, rpc)
    instance = _method_instance(module, {"mcp_removed": _schema("Removed")})

    instance.toolkit_configurations_collected(None, {"github": _schema("GitHub")})

    assert rpc.unregistered == [{"type_name": "mcp_removed"}]


def test_foreign_mcp_prefix_registration_is_not_replaced_or_removed(monkeypatch):
    rpc = _RpcManager([
        _entry("mcp_foreign", validator="other_validator"),
        _entry("mcp_unmarked", config_schema={"metadata": {}}),
    ])
    module = _load_method(monkeypatch, rpc)
    instance = _method_instance(module)

    instance.toolkit_configurations_collected(None, {})

    assert rpc.unregistered == []
    assert rpc.registered == []
