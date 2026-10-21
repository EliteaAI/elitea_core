"""#6886: the input guardrail admin fields.

Checkpoint C runs in the indexer, so every field must sync to indexer_worker at the same
path; a field that does not would leave the SDK middleware on a different policy than
checkpoints A and B. The defaults must match the detector's, so a fresh install with the
switch on behaves as the plan says (credentials block, PII off, fail open).
"""
import importlib.util
import json
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA = json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())
FIELDS = {k: v for k, v in SCHEMA['properties'].items() if k.startswith('input_guardrail_')}


@pytest.fixture(scope='module')
def detector():
    path = PLUGIN_ROOT / 'utils' / 'input_guardrail' / 'input_detector.py'
    spec = importlib.util.spec_from_file_location('input_detector_schema_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_field_lives_under_the_config_block_and_syncs_to_the_indexer():
    assert len(FIELDS) == 8
    for key, field in FIELDS.items():
        assert field['path'].startswith('input_guardrail.'), key
        assert field['section'] == 'guardrails', key
        assert field['requires_restart'] is False, key
        assert field['sync_targets'] == [
            {'pylon': 'pylon-indexer', 'plugin': 'indexer_worker', 'path': field['path']},
        ], key


def test_switch_is_off_by_default_and_gates_the_rest():
    assert FIELDS['input_guardrail_enabled']['default'] is False
    for key, field in FIELDS.items():
        if key != 'input_guardrail_enabled':
            assert 'input_guardrail_enabled' in json.dumps(field.get('visible_when')), key


def test_defaults_match_the_detector(detector):
    assert FIELDS['input_guardrail_credentials_action']['default'] == detector.DEFAULT_CATEGORY_ACTIONS['credentials']
    assert FIELDS['input_guardrail_personal_data_action']['default'] == detector.DEFAULT_CATEGORY_ACTIONS['personal_data']
    assert FIELDS['input_guardrail_on_error']['default'] == detector.Policy().on_error
    assert FIELDS['input_guardrail_timeout_ms']['default'] == detector.DEFAULT_TIMEOUT_MS
    assert FIELDS['input_guardrail_max_inspect_chars']['default'] == detector.DEFAULT_MAX_INSPECT_CHARS


def test_action_choices_are_the_detector_actions(detector):
    for key in ('input_guardrail_credentials_action', 'input_guardrail_personal_data_action'):
        assert tuple(FIELDS[key]['value_schema']['enum']) == detector.ACTIONS, key


def test_allow_patterns_are_validated_as_regex_on_save():
    assert FIELDS['input_guardrail_allow_patterns']['format'] == 'regex_list'
