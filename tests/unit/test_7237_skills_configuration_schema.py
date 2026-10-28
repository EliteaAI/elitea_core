"""#7237 - the skill limits moved from Configuration -> Runtime to Features -> Skills Configuration."""
import json
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
SKILL_LIMITS = {
    'max_skills_per_agent': ('skills.max_skills_per_agent', 50, 200),
    'max_invoked_skills_per_turn': ('skills.max_invoked_skills_per_turn', 5, 50),
    'max_instructions_length': ('skills.max_instructions_length', 50000, 200000),
}


@pytest.fixture(scope='module')
def properties():
    return json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())['properties']


def test_the_skills_section_holds_the_three_limits_and_the_project_skills_switch(properties):
    assert [key for key, field in properties.items() if field.get('section') == 'skills'] == [
        *SKILL_LIMITS, 'project_skills_in_chat_available',
    ]


def test_runtime_no_longer_offers_a_skills_setting(properties):
    assert [key for key, field in properties.items() if field.get('section') == 'runtime' and field.get('path', '').startswith('skills.')] == []


@pytest.mark.parametrize('key', SKILL_LIMITS)
def test_a_moved_limit_keeps_its_path_default_and_range(properties, key):
    path, default, maximum = SKILL_LIMITS[key]
    field = properties[key]

    assert (field['path'], field['default'], field['minimum'], field['maximum']) == (path, default, 1, maximum)
    assert field['value_schema'] == {'type': 'integer', 'minimum': 1, 'maximum': maximum}
    assert field['description'].endswith(f'Allowed range: 1–{maximum}.')
    assert field['requires_restart'] is False


def test_the_project_skills_switch_has_the_ticket_wording_and_defaults_off(properties):
    field = properties['project_skills_in_chat_available']

    assert field['title'] == 'Allow project skills without chat participation'
    assert field['description'] == (
        'Allow chats to load and invoke accessible skills from their project without adding those skills as chat participants.'
    )
    assert (field['default'], field['strict_type']) == (False, True)
