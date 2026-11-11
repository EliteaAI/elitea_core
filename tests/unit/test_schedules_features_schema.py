"""The scheduling settings live in the Features -> Schedules section, not in Configuration -> Runtime."""
import json
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

# card title -> keys, in the order the page shows them
CARDS = {
    'Schedule Settings': [
        'schedules_min_interval_minutes',
        'schedules_max_enabled_per_project',
        'schedules_dispatch_cap',
        'schedules_stale_run_minutes',
        'schedules_max_parallel_runs',
        'schedules_approval_wait_hours',
    ],
    'Agent Schedules': ['agent_scheduling_enabled'],
    'Pipeline Schedules': [
        'pipeline_scheduling_enabled',
        'pipeline_scheduling_cron',
        'schedules_legacy_step_enabled',
    ],
    'Index Schedules': ['index_scheduling_enabled', 'index_scheduling_cron'],
    'Webhooks': [
        'schedules_webhook_max_concurrent_runs',
        'schedules_webhook_max_concurrent_runs_limit',
        'schedules_webhook_project_max_concurrent_runs',
        'schedules_webhook_max_waiting_runs',
    ],
}

# key -> path, type, default, minimum, maximum, requires_restart
MOVED = {
    'schedules_min_interval_minutes': ('scheduler.schedules.min_interval_minutes', 'integer', 5, 1, 1440, False),
    'schedules_max_enabled_per_project': ('scheduler.schedules.max_enabled_per_project', 'integer', 50, 1, 1000, False),
    'schedules_dispatch_cap': ('scheduler.schedules.dispatch_cap', 'integer', 100, 1, 10000, False),
    'schedules_stale_run_minutes': ('scheduler.schedules.stale_run_minutes', 'integer', 120, 1, 10080, False),
    'schedules_max_parallel_runs': ('scheduler.schedules.max_parallel_runs', 'integer', 100, 1, 1000, True),
    'schedules_approval_wait_hours': ('scheduler.schedules.approval_wait_hours', 'integer', 24, 1, 720, False),
    'agent_scheduling_enabled': ('scheduler.schedules.agent_scheduling_enabled', 'boolean', True, None, None, None),
    'pipeline_scheduling_enabled': ('scheduler.pipeline_scheduling.enabled', 'boolean', True, None, None, None),
    'pipeline_scheduling_cron': ('scheduler.pipeline_scheduling.cron', 'string', '* * * * *', None, None, None),
    'schedules_legacy_step_enabled': ('scheduler.schedules.legacy_step_enabled', 'boolean', True, None, None, None),
    'index_scheduling_enabled': ('scheduler.index_scheduling.enabled', 'boolean', True, None, None, None),
    'index_scheduling_cron': ('scheduler.index_scheduling.cron', 'string', '* * * * *', None, None, None),
    'schedules_webhook_max_concurrent_runs': ('scheduler.schedules.webhook_max_concurrent_runs', 'integer', 3, 1, 1000, False),
    'schedules_webhook_max_concurrent_runs_limit': ('scheduler.schedules.webhook_max_concurrent_runs_limit', 'integer', 20, 1, 1000, False),
    'schedules_webhook_project_max_concurrent_runs': ('scheduler.schedules.webhook_project_max_concurrent_runs', 'integer', 10, 1, 1000, False),
    'schedules_webhook_max_waiting_runs': ('scheduler.schedules.webhook_max_waiting_runs', 'integer', 10, 1, 1000, False),
}

CRON_SWITCHES = {
    'pipeline_scheduling_cron': 'pipeline_scheduling_enabled',
    'index_scheduling_cron': 'index_scheduling_enabled',
}


@pytest.fixture(scope='module')
def properties():
    return json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())['properties']


def _section_keys(properties, section):
    return [key for key, field in properties.items() if field.get('section') == section]


def test_the_schedules_section_holds_the_sixteen_scheduler_settings(properties):
    assert sorted(_section_keys(properties, 'schedules')) == sorted(MOVED)
    assert len(MOVED) == 16


def test_runtime_no_longer_offers_a_moved_scheduler_setting(properties):
    assert [key for key in _section_keys(properties, 'runtime') if key in MOVED] == []


def test_each_card_lists_its_keys_in_schema_order(properties):
    schema_order = _section_keys(properties, 'schedules')
    card_keys = [key for keys in CARDS.values() for key in keys]

    assert sorted(card_keys) == sorted(schema_order)
    for title, keys in CARDS.items():
        positions = [schema_order.index(key) for key in keys]
        assert positions == sorted(positions), title
    assert {title: len(keys) for title, keys in CARDS.items()} == {
        'Schedule Settings': 6, 'Agent Schedules': 1, 'Pipeline Schedules': 3,
        'Index Schedules': 2, 'Webhooks': 4,
    }


@pytest.mark.parametrize('key', MOVED)
def test_a_moved_setting_keeps_its_path_default_range_and_restart_flag(properties, key):
    path, kind, default, minimum, maximum, restart = MOVED[key]
    field = properties[key]

    assert (field['path'], field['type'], field['default']) == (path, kind, default)
    assert (field.get('minimum'), field.get('maximum'), field.get('requires_restart')) == (minimum, maximum, restart)


def test_only_the_parallel_runs_limit_asks_for_a_reload(properties):
    assert [
        key for key in _section_keys(properties, 'schedules') if properties[key].get('requires_restart')
    ] == ['schedules_max_parallel_runs']


@pytest.mark.parametrize('cron, switch', CRON_SWITCHES.items())
def test_each_cron_stays_visible_only_when_its_switch_is_on(properties, cron, switch):
    assert properties[cron]['visible_when'] == {'field': switch, 'value': True}
    assert properties[switch]['section'] == properties[cron]['section'] == 'schedules'
