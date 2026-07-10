"""Issue #5694 - keep provider-hub reference fields typed for the Inventory UI.

Run from the elitea_core plugin root:

    python3 -m pytest --rootdir=utils/tests --import-mode=importlib \
        utils/tests/test_5694_provider_hub_schemas.py -v
"""

import importlib.util
import pathlib
import sys
import types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _Log:
    @staticmethod
    def exception(*_args, **_kwargs):
        pass


class _Web:
    def __getattr__(self, _name):
        def decorator_factory(*_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

        return decorator_factory


def _install_stubs():
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = _Log()
    tools_mod.web = _Web()
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules["pylon.core.tools"] = tools_mod

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    sys.modules["tools"] = tools_pkg
    sys.modules["tools.auth"] = tools_pkg.auth


def _load_module():
    _install_stubs()
    spec = importlib.util.spec_from_file_location(
        "provider_hub_schemas",
        PLUGIN_ROOT / "methods" / "provider_hub_schemas.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeParam:
    def __init__(self, type_name, *, required=False, default=None, description=None, json_schema_extra=None):
        self.type = types.SimpleNamespace(value=type_name)
        self.required = required
        self.default = default
        self.description = description
        self.json_schema_extra = json_schema_extra


def _make_provider(parameters):
    toolkit = types.SimpleNamespace(
        name="inventory",
        description="Inventory toolkit",
        toolkit_config=types.SimpleNamespace(parameters=parameters, fields_order=list(parameters.keys())),
        toolkit_metadata={},
        provided_tools=[],
    )
    return types.SimpleNamespace(provided_toolkits=[toolkit])


def test_reference_picker_fields_keep_array_type():
    module = _load_module()
    provider = _make_provider({
        "sources": FakeParam(
            "JSON",
            default=[],
            json_schema_extra={"toolkit_types": ["github", "ado_repos"], "compact_label": True},
        ),
    })

    result = {}
    module.Method().prepare_provider_toolkits(result, "provider", provider)

    sources_schema = result["provider_inventory"]["properties"]["toolkit_configuration_sources"]
    assert sources_schema["type"] == "array"
    assert sources_schema["default"] == []
    assert sources_schema["toolkit_types"] == ["github", "ado_repos"]
    assert sources_schema["compact_label"] is True


def test_configuration_fields_still_use_configuration_type():
    module = _load_module()
    provider = _make_provider({
        "llm_model": FakeParam(
            "String",
            required=True,
            json_schema_extra={"configuration_model": "llm", "compact_label": True},
        ),
    })

    result = {}
    module.Method().prepare_provider_toolkits(result, "provider", provider)

    model_schema = result["provider_inventory"]["properties"]["toolkit_configuration_llm_model"]
    assert model_schema["type"] == "configuration"
    assert model_schema["configuration_model"] == "llm"
    assert model_schema["compact_label"] is True
