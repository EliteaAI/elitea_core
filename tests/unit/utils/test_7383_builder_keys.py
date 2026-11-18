"""Issue #7383 - one Elitea Builder capability key.

The three old builder modules (internal_mcp, skill_builder, project_context_builder) are removed.
Only `elitea_builder` turns the builder on; the one-off migration rewrites stored rows.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


@pytest.fixture(scope='module')
def builder_keys():
    return load_utils_module(TESTS_DIR.parent / 'utils', 'builder_keys')


class TestBuilderEnabled:

    def test_new_key_enables(self, builder_keys):
        assert builder_keys.is_builder_enabled(['ask_user', 'elitea_builder']) is True

    @pytest.mark.parametrize('given', [
        None, [], ['ask_user'], ['internal_mcp'], ['skill_builder'], ['project_context_builder'],
    ])
    def test_old_keys_no_longer_enable(self, builder_keys, given):
        assert builder_keys.is_builder_enabled(given) is False


class TestMigrateBuilderKeys:

    @pytest.mark.parametrize('given, expected', [
        (['skill_builder', 'ask_user'], ['elitea_builder', 'ask_user']),
        (['project_context_builder', 'internal_mcp', 'skill_builder'], ['elitea_builder']),
        (['ask_user', 'internal_mcp', 'planner'], ['ask_user', 'elitea_builder', 'planner']),
        (['ask_user', 'project_context_builder', 'elitea_builder'], ['ask_user', 'elitea_builder']),
        (['elitea_builder'], ['elitea_builder']),
        (['ask_user', 'swarm'], ['ask_user', 'swarm']),
        ([], []),
        (None, []),
    ])
    def test_maps_old_keys_in_place(self, builder_keys, given, expected):
        assert builder_keys.migrate_builder_keys(given) == expected

    def test_does_not_mutate_input(self, builder_keys):
        given = ['skill_builder', 'ask_user']
        builder_keys.migrate_builder_keys(given)
        assert given == ['skill_builder', 'ask_user']


class TestMigrateMeta:

    def test_old_meta_is_rewritten(self, builder_keys):
        meta = {'internal_tools': ['skill_builder', 'ask_user'], 'lazy_tools_mode': True}
        new_meta, changed = builder_keys.migrate_meta(meta)
        assert changed is True
        assert new_meta == {'internal_tools': ['elitea_builder', 'ask_user'], 'lazy_tools_mode': True}
        assert meta['internal_tools'] == ['skill_builder', 'ask_user']

    @pytest.mark.parametrize('meta', [
        {'internal_tools': ['elitea_builder', 'ask_user']},
        {'internal_tools': []},
        {'other': 1},
        {},
        None,
    ])
    def test_already_migrated_is_unchanged(self, builder_keys, meta):
        new_meta, changed = builder_keys.migrate_meta(meta)
        assert changed is False
        assert new_meta == meta

    def test_second_pass_is_idempotent(self, builder_keys):
        once, _ = builder_keys.migrate_meta({'internal_tools': ['internal_mcp', 'skill_builder']})
        twice, changed = builder_keys.migrate_meta(once)
        assert changed is False
        assert twice == {'internal_tools': ['elitea_builder']}


class _Row:
    def __init__(self, meta):
        self.meta = meta


class TestMigrateRows:

    def test_counts_and_rewrites(self, builder_keys):
        rows = [_Row({'internal_tools': ['skill_builder']}), _Row({'internal_tools': ['elitea_builder']}), _Row({})]
        flagged = []
        assert builder_keys.migrate_rows(rows, False, lambda row, key: flagged.append(key)) == 1
        assert rows[0].meta == {'internal_tools': ['elitea_builder']}
        assert flagged == ['meta']

    def test_dry_run_writes_nothing(self, builder_keys):
        rows = [_Row({'internal_tools': ['internal_mcp']})]
        assert builder_keys.migrate_rows(rows, True) == 1
        assert rows[0].meta == {'internal_tools': ['internal_mcp']}


class TestRunBuilderKeysMigration:

    def test_sums_counts_and_lists_failed_projects(self, builder_keys):
        def migrate_project(pid, dry_run):
            if pid == 2:
                raise RuntimeError('schema broken')
            return 3, 1

        result = builder_keys.run_builder_keys_migration(
            [1, 2, 3], migrate_project, lambda pid, dry_run: 2, dry_run=False,
        )
        assert result == {
            'updated_versions': 6, 'updated_conversations': 2, 'updated_settings': 4,
            'failed_projects': [2], 'dry_run': False,
        }

    def test_passes_dry_run_through(self, builder_keys):
        seen = []
        builder_keys.run_builder_keys_migration(
            [1], lambda pid, dry_run: seen.append(dry_run) or (0, 0),
            lambda pid, dry_run: seen.append(dry_run) or 0, dry_run=True,
        )
        assert seen == [True, True]
