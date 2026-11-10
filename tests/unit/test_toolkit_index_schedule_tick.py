"""The schedule tick and Toolkit Index schedules: the type flag, the legacy index step with its idle bound, the restore RPC.

The tick module is the real file; its steps and the migrations are stand-ins that record their calls
(the `world` fixture of the legacy pipeline step test).
"""
import sys
import types

import pytest
from unit.rpc import test_7011_tick_legacy_step as legacy_step
from unit.rpc.test_7011_tick_legacy_step import _module

MIGRATION = 'plugins.elitea_core.utils.legacy_index_schedule_migration'
world = legacy_step.world  # the fixture of the pipeline step test, bound here so pytest finds it
RESTORE_REFUSED = 'the legacy index step is on'


class IndexWorld:
    """The index migration stand-in, a clock for the idle bound and the types the tick asked to claim."""

    def __init__(self, world, monkeypatch):
        self.world, self.migrated, self.fail, self.found, self.held = world, [], set(), {}, {}
        self.claimed, self.restored, self.now = [], [], [1000.0]
        tick = world.module

        def migrate_index_project(project_id, dry_run, enforce_limits=False):
            self.migrated.append((project_id, dry_run, enforce_limits))
            if project_id in self.fail:
                raise RuntimeError('lock wait timed out')
            return {'found': self.found.get(project_id, 0), 'errors': [], 'held': self.held.get(project_id, [])}

        def index_restore_refusal(settings):
            return RESTORE_REFUSED if (settings or {}).get('legacy_index_step_enabled') else None

        def restore_index_schedules(project_id):
            self.restored.append(project_id)
            return {'restored': [1], 'errors': [], 'without_original_trigger': 0}

        monkeypatch.setitem(sys.modules, MIGRATION, _module(
            MIGRATION, migrate_index_project=migrate_index_project, index_restore_refusal=index_restore_refusal,
            restore_index_schedules=restore_index_schedules))

        def select_due(session, entity_types, limit):
            self.claimed.append(list(entity_types))
            return []

        monkeypatch.setattr(tick, 'select_due_schedules', select_due)
        monkeypatch.setattr(tick, 'RUN_STARTERS', {'agent': print, 'pipeline': print, 'toolkit_index': print})
        monkeypatch.setattr(tick, 'time', types.SimpleNamespace(monotonic=lambda: self.now[0]))
        monkeypatch.setitem(tick._legacy_index_idle, 'until', 0.0)

    def wait(self, seconds):
        self.now[0] += seconds


@pytest.fixture
def index(world, monkeypatch):  # noqa: F811 -- the imported fixture
    return IndexWorld(world, monkeypatch)


def test_the_tick_claims_toolkit_index_rows_unless_their_flag_is_off(world, index):
    world.tick()
    world.settings['toolkit_index_scheduling_enabled'] = False
    world.tick()
    assert index.claimed[0] == ['agent', 'pipeline', 'toolkit_index']
    assert index.claimed[-1] == ['agent', 'pipeline']


def test_the_toolkit_index_flag_leaves_the_agent_flag_alone(world, index):
    world.settings['agent_scheduling_enabled'] = False
    world.tick()
    assert index.claimed[0] == ['pipeline', 'toolkit_index']


def test_the_index_step_migrates_each_project_with_the_limits_enforced(world, index):
    world.tick()
    assert index.migrated == [(1, False, True), (2, False, True)]


def test_the_index_step_does_not_touch_the_pipeline_migration(world, index):
    world.tick()
    assert world.migrated == [(1, False), (2, False)]


def test_an_empty_pass_puts_the_index_step_to_sleep_for_an_hour(world, index):
    world.tick()
    assert world.module._legacy_index_idle['until'] == index.now[0] + world.module.LEGACY_RECHECK_SECONDS
    index.migrated.clear()
    index.wait(world.module.LEGACY_RECHECK_SECONDS - 1)
    world.tick()
    assert index.migrated == []
    index.wait(2)
    world.tick()
    assert [project for project, _, _ in index.migrated] == [1, 2]


def test_a_pass_that_found_schedules_does_not_sleep(world, index):
    index.found = {2: 3}
    world.tick()
    index.migrated.clear()
    world.tick()
    assert sorted(project for project, _, _ in index.migrated) == [1, 2]


def test_the_switch_off_stops_the_index_step_only(world, index):
    world.settings['legacy_index_step_enabled'] = False
    world.tick()
    assert index.migrated == []
    assert world.migrated == [(1, False), (2, False)]


def test_a_project_that_held_index_schedules_waits_for_its_retry(world, index):
    index.held = {1: [7]}
    index.found = {2: 1}  # a pass that found schedules does not sleep, so the retry map is what skips project 1
    world.tick()
    index.migrated.clear()
    index.wait(world.module.MIGRATION_RETRY_SECONDS - 1)
    world.tick()
    assert sorted(project for project, _, _ in index.migrated) == [2]
    index.wait(2)
    world.tick()
    assert sorted(project for project, _, _ in index.migrated) == [1, 2, 2]


def test_a_held_index_project_does_not_hold_back_its_pipeline_migration(world, index):
    index.held = {1: [7]}
    world.tick()
    world.migrated.clear()
    assert world.module._migrate_legacy_triggers(1) is True
    assert world.migrated == [(1, False)]


def test_a_failed_index_migration_is_logged_and_the_other_projects_go_on(world, index):
    index.fail = {1}
    world.tick()
    assert [project for project, _, _ in index.migrated] == [1, 2]
    assert any('legacy migration failed' in warning[0] for warning in world.warnings)


def test_a_failed_index_step_does_not_stop_the_pipeline_step(world, monkeypatch):
    monkeypatch.setitem(sys.modules, MIGRATION, None)  # the import fails
    world.tick()
    assert [e for e in world.events if e[0] == 'legacy'] == [('legacy', 1), ('legacy', 2)]


def test_the_pipeline_migration_keeps_its_call_and_its_result(world, index):
    assert world.module._migrate_legacy_triggers(1) is True
    assert world.migrated == [(1, False)]
    assert world.enforced is True
    world.found = {3: 2}
    assert world.module._migrate_legacy_triggers(3) is False


def test_a_given_migration_and_retry_map_replace_the_pipeline_ones(world, index):
    retry = {}
    calls = []

    def migrate(project_id, dry_run, enforce_limits=False):
        calls.append((project_id, dry_run, enforce_limits))
        return {'found': 0, 'errors': ['bad'], 'held': []}

    world.module._migrate_legacy_triggers(4, migrate, retry)
    assert world.module._migrate_legacy_triggers(4, migrate, retry) is False  # waits for its retry
    assert calls == [(4, False, True)]
    assert 4 in retry
    assert 4 not in world.module._migration_retry_at
    assert world.migrated == []


def test_the_index_restore_rpc_refuses_while_the_index_step_is_on(world, index):
    rpc = world.module.RPC()
    assert RESTORE_REFUSED in rpc.schedules_restore_legacy_index_schedules(5)['error']
    world.settings['legacy_index_step_enabled'] = True
    assert RESTORE_REFUSED in rpc.schedules_restore_legacy_index_schedules(5)['error']
    assert index.restored == []


def test_the_index_restore_rpc_restores_once_the_index_step_is_off(world, index):
    world.settings['legacy_index_step_enabled'] = False
    assert world.module.RPC().schedules_restore_legacy_index_schedules('5')['restored'] == [1]
    assert index.restored == [5]


def test_the_pipeline_switch_does_not_open_the_index_restore(world, index):
    world.settings['legacy_step_enabled'] = False
    assert 'error' in world.module.RPC().schedules_restore_legacy_index_schedules(5)
    assert index.restored == []
