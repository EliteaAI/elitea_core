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


def test_batch_chains_each_patch_onto_the_previous_result(versioning):
    """Only the first patch is hash-checked; the rest see text the earlier ones produced. Checking
    every patch against the original hash would reject any batch longer than one."""
    current = 'Answer briefly. Never cite sources.'

    updated = versioning.apply_instructions_patch_batch(
        current,
        expected_sha256=versioning.instructions_sha256(current),
        patches=[
            {'old_text': 'Answer briefly.', 'replacement': 'Answer thoroughly.'},
            {'old_text': 'Never cite sources.', 'replacement': 'Always cite sources.'},
        ],
    )

    assert updated == 'Answer thoroughly. Always cite sources.'


def test_batch_failure_names_the_offending_patch(versioning):
    """With several edits in flight, "old_text must match exactly once" is untraceable without an
    index — and the usual cause is an earlier patch having consumed the text this one anchors to."""
    current = 'Answer briefly.'

    with pytest.raises(versioning.InstructionsPatchConflictError, match='Patch 1'):
        versioning.apply_instructions_patch_batch(
            current,
            expected_sha256=versioning.instructions_sha256(current),
            patches=[
                {'old_text': 'briefly', 'replacement': 'thoroughly'},
                {'old_text': 'briefly', 'replacement': 'concisely'},
            ],
        )


def test_batch_rejects_a_stale_hash_on_the_first_patch(versioning):
    with pytest.raises(versioning.InstructionsPatchConflictError, match='Patch 0'):
        versioning.apply_instructions_patch_batch(
            'new server value',
            expected_sha256=versioning.instructions_sha256('old read value'),
            patches=[{'old_text': 'value', 'replacement': 'content'}],
        )


def test_batch_rejects_an_empty_list(versioning):
    with pytest.raises(versioning.InstructionsPatchConflictError, match='No patches'):
        versioning.apply_instructions_patch_batch(
            'anything', expected_sha256=versioning.instructions_sha256('anything'), patches=[],
        )


def test_backup_name_is_compact_and_stable(versioning):
    now = datetime(2026, 8, 28, 12, 34, 56, 123456, tzinfo=timezone.utc)

    name = versioning.build_mcp_backup_version_name(101, now)

    assert name == 'mcp-backup-101-20260828T123456123456Z'
    assert len(name) <= 128


def test_enhance_version_names_are_distinguishable_from_mcp_backups(versioning):
    """Someone auditing why instructions changed needs to tell "an agent edited this via a tool
    call" from "a human accepted an AI enhancement proposal"."""
    now = datetime(2026, 8, 28, 12, 34, 56, 123456, tzinfo=timezone.utc)

    backup = versioning.build_enhance_backup_version_name(101, now)
    fork = versioning.build_enhance_fork_version_name(101, now)

    assert backup == 'enhance-backup-101-20260828T123456123456Z'
    assert fork == 'enhanced-101-20260828T123456123456Z'
    assert len({backup, fork, versioning.build_mcp_backup_version_name(101, now)}) == 3
    assert len(backup) <= 128 and len(fork) <= 128
