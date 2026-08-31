"""Default output-token normalization for EliteaAI/elitea_issues#3562."""

import importlib.util
import pathlib


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'llm_settings.py'
SPEC = importlib.util.spec_from_file_location('issue_3562_llm_settings', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
get_default_max_tokens = MODULE.get_default_max_tokens
normalize_runtime_max_tokens = MODULE.normalize_runtime_max_tokens


def test_missing_and_legacy_default_have_no_runtime_custom_cap():
    assert normalize_runtime_max_tokens(None) is None
    assert normalize_runtime_max_tokens(-1) is None


def test_positive_custom_cap_is_preserved():
    assert normalize_runtime_max_tokens(1234) == 1234


def test_legacy_default_accessor_no_longer_returns_magic_numbers():
    assert get_default_max_tokens(False) is None
    assert get_default_max_tokens(True) is None
