"""Validate that tool_outcomes and last_tool_outcome are allowed as pipeline state variables (#6593).

These variables were previously reserved for system use, but are now exposed as default
pipeline state variables that users can reference in their pipelines.
"""
import importlib.util
import pathlib
import sys

import yaml

# Load pipeline_utils directly to avoid package-install requirement
_utils_path = pathlib.Path(__file__).parents[3] / 'utils' / 'pipeline_utils.py'
_spec = importlib.util.spec_from_file_location('pipeline_utils', _utils_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules.setdefault('pipeline_utils', _mod)
_spec.loader.exec_module(_mod)

validate_yaml_from_str = _mod.validate_yaml_from_str


def _make_yaml(state: dict) -> str:
    return yaml.safe_dump({'nodes': [], 'state': state}, sort_keys=False)


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
    result = validate_yaml_from_str(yaml.safe_dump({'nodes': []}, sort_keys=False))
    assert isinstance(result, dict)


def test_empty_state_passes():
    result = validate_yaml_from_str(_make_yaml({}))
    assert isinstance(result, dict)
