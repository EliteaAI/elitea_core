import importlib.util
from datetime import datetime, timezone
import pathlib

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'mcp_versioning.py'


@pytest.fixture(scope='module')
def versioning():
    spec = importlib.util.spec_from_file_location('mcp_versioning_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_patch_changes_one_server_side_excerpt(versioning):
    current = 'Keep this. Change this sentence. Keep that.'

    updated = versioning.apply_instructions_patch(
        current,
        expected_sha256=versioning.instructions_sha256(current),
        old_text='Change this sentence.',
        replacement='Use the safer sentence.',
    )

    assert updated == 'Keep this. Use the safer sentence. Keep that.'


def test_patch_rejects_stale_read_without_changing_content(versioning):
    with pytest.raises(versioning.InstructionsPatchConflictError, match='changed'):
        versioning.apply_instructions_patch(
            'new server value',
            expected_sha256=versioning.instructions_sha256('old read value'),
            old_text='value',
            replacement='content',
        )


def test_patch_rejects_ambiguous_match(versioning):
    current = 'same and same'

    with pytest.raises(versioning.InstructionsPatchConflictError, match='found 2'):
        versioning.apply_instructions_patch(
            current,
            expected_sha256=versioning.instructions_sha256(current),
            old_text='same',
            replacement='different',
        )


def test_patch_rejects_empty_result(versioning):
    current = 'do not wipe me'

    with pytest.raises(versioning.InstructionsPatchConflictError, match='empty'):
        versioning.apply_instructions_patch(
            current,
            expected_sha256=versioning.instructions_sha256(current),
            replacement='',
            replace_all=True,
        )


def test_replace_all_is_hash_guarded(versioning):
    current = 'old prompt'

    updated = versioning.apply_instructions_patch(
        current,
        expected_sha256=versioning.instructions_sha256(current),
        replacement='complete new prompt',
        replace_all=True,
    )

    assert updated == 'complete new prompt'


@pytest.mark.parametrize('legacy_value', [None, '', 'current prompt'])
def test_settings_update_strips_non_changes_to_instructions(versioning, legacy_value):
    payload = {'welcome_message': 'Hello!', 'instructions': legacy_value}

    sanitized = versioning.sanitize_mcp_settings_update(payload, 'current prompt')

    assert sanitized == {'welcome_message': 'Hello!'}
    assert payload['instructions'] == legacy_value


def test_settings_update_without_instructions_is_unchanged(versioning):
    payload = {'conversation_starters': ['Start here']}

    assert versioning.sanitize_mcp_settings_update(payload, 'current prompt') == payload


def test_settings_update_rejects_a_real_instruction_change(versioning):
    with pytest.raises(versioning.InstructionsPatchConflictError, match='safe instructions'):
        versioning.sanitize_mcp_settings_update(
            {'welcome_message': 'Hello!', 'instructions': 'different prompt'},
            'current prompt',
        )


def test_backup_name_is_compact_and_stable(versioning):
    now = datetime(2026, 8, 28, 12, 34, 56, 123456, tzinfo=timezone.utc)

    name = versioning.build_mcp_backup_version_name(101, now)

    assert name == 'mcp-backup-101-20260828T123456123456Z'
    assert len(name) <= 128
