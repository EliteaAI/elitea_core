"""Validate that reserved system state variable names are rejected at pipeline save time (#6593)."""
import importlib.util
import pathlib
import sys

import pytest
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


@pytest.mark.parametrize('name', ['tool_outcomes', 'last_tool_outcome'])
def test_reserved_name_raises(name):
    with pytest.raises(ValueError, match='reserved for system use'):
        validate_yaml_from_str(_make_yaml({name: {'type': 'dict'}}))


def test_non_reserved_name_passes():
    result = validate_yaml_from_str(_make_yaml({'my_var': {'type': 'str'}}))
    assert 'state' in result


def test_no_state_key_passes():
    result = validate_yaml_from_str(yaml.safe_dump({'nodes': []}, sort_keys=False))
    assert isinstance(result, dict)


def test_empty_state_passes():
    result = validate_yaml_from_str(_make_yaml({}))
    assert isinstance(result, dict)


def test_mixed_state_raises_on_reserved():
    with pytest.raises(ValueError, match='reserved for system use'):
        validate_yaml_from_str(_make_yaml({'my_var': {'type': 'str'}, 'tool_outcomes': {'type': 'dict'}}))
