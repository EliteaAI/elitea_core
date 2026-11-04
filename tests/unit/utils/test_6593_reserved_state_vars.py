"""Validate that tool_outcomes and last_tool_outcome are allowed as pipeline state variables (#6593).

These variables were previously reserved for system use, but are now exposed as default
pipeline state variables that users can reference in their pipelines.
"""
import importlib.util
import pathlib
import sys
import types

import yaml

_PLUGIN_ROOT = pathlib.Path(__file__).parents[3]
_PKG = "elitea_core_6593"


def _load(rel_path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, _PLUGIN_ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Register a package stub so the relative import inside pipeline_utils
# (`from .pipeline_validator import ...`) resolves correctly.
_pkg = types.ModuleType(_PKG)
_pkg.__path__ = [str(_PLUGIN_ROOT / "utils")]
sys.modules[_PKG] = _pkg

_load("utils/pipeline_validator.py", f"{_PKG}.pipeline_validator")
_utils_mod = _load("utils/pipeline_utils.py", f"{_PKG}.pipeline_utils")

validate_yaml_from_str = _utils_mod.validate_yaml_from_str


_MINIMAL_NODE = [{'id': 'step1', 'type': 'llm', 'transition': 'END'}]


def _make_yaml(state: dict) -> str:
    return yaml.safe_dump(
        {'entry_point': 'step1', 'nodes': _MINIMAL_NODE, 'state': state},
        sort_keys=False,
    )


def test_tool_outcomes_is_allowed():
    result = validate_yaml_from_str(_make_yaml({'tool_outcomes': {'type': 'dict'}}))
    assert 'state' in result


def test_last_tool_outcome_is_allowed():
    result = validate_yaml_from_str(_make_yaml({'last_tool_outcome': {'type': 'dict'}}))
    assert 'state' in result


def test_tool_outcomes_mixed_with_user_vars_passes():
    result = validate_yaml_from_str(_make_yaml({'my_var': {'type': 'str'}, 'tool_outcomes': {'type': 'dict'}}))
    assert 'state' in result


def test_non_default_name_passes():
    result = validate_yaml_from_str(_make_yaml({'my_var': {'type': 'str'}}))
    assert 'state' in result


def test_no_state_key_passes():
    result = validate_yaml_from_str(
        yaml.safe_dump({'entry_point': 'step1', 'nodes': _MINIMAL_NODE}, sort_keys=False)
    )
    assert isinstance(result, dict)


def test_empty_state_passes():
    result = validate_yaml_from_str(_make_yaml({}))
    assert isinstance(result, dict)
