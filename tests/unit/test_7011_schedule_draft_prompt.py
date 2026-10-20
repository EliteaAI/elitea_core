"""EL-7011 US-21 R1.2 - build_schedule_draft_prompt sends the current values and the roster to the model."""
import sys
import types

import pytest
from unit._tool_spend_loader import load

TEMPLATE = ('Today {today} in {timezone}.\nAgents:\n{agents}\nPipelines:\n{pipelines}\n'
            'Current: {current_values}\nLocked: {locked_entity}\nJSON: {{"name": "..."}} {unknown}')
ROSTER = [
    {'application_id': 42, 'name': 'Digest agent', 'agent_type': 'react', 'version_id': 420, 'version_name': 'base'},
    {'application_id': 51, 'name': 'Report flow', 'agent_type': 'pipeline', 'version_id': 510, 'version_name': 'base'},
    {'application_id': 51, 'name': 'Report flow', 'agent_type': 'pipeline', 'version_id': 511, 'version_name': 'v2'},
]


@pytest.fixture
def build(monkeypatch):
    stub = types.ModuleType('tools')
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    monkeypatch.setitem(sys.modules, 'tools', stub)
    for dotted in ('utils.schedule_timing', 'models.pd.entity_schedule', 'models.pd.generate_schedule_draft',
                   'utils.schedule_draft_utils'):
        monkeypatch.setitem(sys.modules, 'plugins.elitea_core.' + dotted, None)
    load('models/pd/index.py')
    load('utils/schedule_timing.py', fresh=True)
    load('models/pd/entity_schedule.py', fresh=True)
    load('models/pd/generate_schedule_draft.py', fresh=True)
    return load('utils/schedule_draft_utils.py', fresh=True).build_schedule_draft_prompt


def _expected(today, timezone, current, locked):
    return (f'Today {today} in {timezone}.\nAgents:\n- Digest agent (versions: base)\n'
            f'Pipelines:\n- Report flow (versions: base, v2)\nCurrent: {current}\nLocked: {locked}\n'
            'JSON: {"name": "..."} {unknown}')


def test_el_7011_r1_2_prompt_fills_every_placeholder_of_the_service_prompt(build):
    from datetime import date
    prompt = build(TEMPLATE, {'name': 'Daily digest', 'cron': '0 9 * * *'}, ROSTER, 'Europe/Kyiv')
    assert prompt == _expected(date.today().isoformat(), 'Europe/Kyiv',
                               '{"name": "Daily digest", "cron": "0 9 * * *"}', 'none')


def test_el_7011_r1_2_new_schedule_without_a_time_zone_uses_utc_and_empty_values(build):
    from datetime import date
    assert build(TEMPLATE, {}, ROSTER, None) == _expected(date.today().isoformat(), 'UTC', '{}', 'none')


def test_el_7011_r1_2_the_locked_entity_is_named_from_the_roster(build):
    prompt = build(TEMPLATE, {}, ROSTER, 'UTC', {'entity_type': 'pipeline', 'application_id': 51})
    assert 'Locked: pipeline Report flow\n' in prompt


def test_el_7011_r1_2_an_empty_roster_says_none(build):
    prompt = build('{agents}|{pipelines}', {}, [], 'UTC')
    assert prompt == '(none)|(none)'
