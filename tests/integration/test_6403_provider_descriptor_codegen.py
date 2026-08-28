"""Issue #6403 - datamodel-code-generator usage contract for the provider descriptor.

Exercises load_provider_descriptor_model against the REAL, repo-bundled
data/ExternalServiceProviderDescriptor.json schema, running the actual
JsonSchemaParser().parse() + compile()/exec() pipeline (not mocked), so a
version bump of datamodel-code-generator that changes generated-code shape
(defaults, enum handling, open-dict/additionalProperties passthrough) shows up
here before it reaches production.

Run via:
    python tests/run_tests.py integration/test_6403_provider_descriptor_codegen.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pydantic
import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
GENERATED_MODULE_BASE = "plugins.elitea_core.generated"


@pytest.fixture(scope="module")
def descriptor_model_module():
    """Load descriptor_model.py with minimal stubs, keeping its relative import intact."""
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.methods"):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")

    class _Log:
        def __getattr__(self, _name):
            return lambda *_a, **_k: None

    class _Web:
        def __getattr__(self, _name):
            def decorator_factory(*_args, **_kwargs):
                def decorator(func):
                    return func
                return decorator
            return decorator_factory

    tools_mod.log = _Log()
    tools_mod.web = _Web()
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.methods.descriptor_model",
        PLUGIN_ROOT / "methods" / "descriptor_model.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def descriptor_model_cls(descriptor_model_module):
    """Run the real codegen pipeline once and return the generated pydantic class."""
    fake_self = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(
            loader=types.SimpleNamespace(
                get_data=lambda path: (PLUGIN_ROOT / path).read_bytes()
            )
        ),
        generated_module_base=GENERATED_MODULE_BASE,
    )
    descriptor_model_module.Method.load_provider_descriptor_model(fake_self)
    return fake_self.descriptor_model


def _sample_payload(extra_param=None):
    parameters = {"api_key": {"_type": "Secret", "_required": True}}
    if extra_param:
        parameters.update(extra_param)
    return {
        "name": "Example Provider",
        "service_location_url": "https://example.com/spi",
        "configuration": {
            "auth_type": "bearer",
            "connection": {"timeout": 30, "retries": [1, 2, 3]},
        },
        "provided_toolkits": [
            {
                "name": "search_toolkit",
                "_description": "Search toolkit",
                "toolkit_config": {
                    "_type": "rest",
                    "parameters": parameters,
                },
                "provided_tools": [
                    {
                        "name": "find",
                        "args_schema": {"type": "object", "properties": {}},
                    }
                ],
            }
        ],
    }


class TestProviderDescriptorCodegen:
    def test_realistic_payload_validates(self, descriptor_model_cls):
        instance = descriptor_model_cls.model_validate(_sample_payload())
        dumped = instance.model_dump(by_alias=True)
        assert dumped["name"] == "Example Provider"
        assert dumped["provided_toolkits"][0]["provided_tools"][0]["name"] == "find"

    def test_tool_defaults_apply(self, descriptor_model_cls):
        instance = descriptor_model_cls.model_validate(_sample_payload())
        dumped = instance.model_dump(by_alias=True)
        tool = dumped["provided_toolkits"][0]["provided_tools"][0]
        assert tool["tool_result_type"] == "Any"
        assert tool["sync_invocation_supported"] is True
        assert tool["async_invocation_supported"] is True

    def test_parameter_required_default_is_false(self, descriptor_model_cls):
        instance = descriptor_model_cls.model_validate(
            _sample_payload({"timeout": {"_type": "Integer"}})
        )
        dumped = instance.model_dump(by_alias=True)
        params = dumped["provided_toolkits"][0]["toolkit_config"]["parameters"]
        assert params["timeout"]["_required"] is False
        assert params["api_key"]["_required"] is True

    def test_invalid_parameter_type_is_rejected(self, descriptor_model_cls):
        payload = _sample_payload({"timeout": {"_type": "NotARealType"}})
        with pytest.raises(pydantic.ValidationError):
            descriptor_model_cls.model_validate(payload)

    def test_open_configuration_dict_round_trips_nested_extras(self, descriptor_model_cls):
        instance = descriptor_model_cls.model_validate(_sample_payload())
        dumped = instance.model_dump(by_alias=True)
        assert dumped["configuration"]["connection"] == {
            "timeout": 30,
            "retries": [1, 2, 3],
        }
