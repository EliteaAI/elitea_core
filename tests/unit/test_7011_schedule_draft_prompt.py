"""EL-7011 US-21 R1.2 - build_schedule_draft_prompt sends the current values and the roster to the model."""
import pytest
from unit.test_7011_schedule_draft_validation import ROSTER, load_draft_utils

TEMPLATE = ('Today {today} in {timezone}.\nAgents:\n{agents}\nPipelines:\n{pipelines}\n'
            'Current: {current_values}\nLocked: {locked_entity}\nJSON: {{"name": "..."}} {unknown}')


@pytest.fixture
def build(monkeypatch):
    return load_draft_utils(monkeypatch).build_schedule_draft_prompt


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


def test_el_7011_r1_2_a_prompt_with_single_braces_still_gets_its_placeholders(build):
    prompt = build('Answer as {"name": "..."} for {timezone} with {agents}', None, ROSTER, 'UTC')
    assert prompt == 'Answer as {"name": "..."} for UTC with - Digest agent (versions: base)'
