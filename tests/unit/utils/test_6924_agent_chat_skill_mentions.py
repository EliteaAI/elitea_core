import types

import pytest

from fixtures.helpers import load_module_with_stubs

PACKAGE = 'plugins.elitea_core'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


@pytest.fixture
def mentions(isolated_sys_modules, plugin_root):
    packages = {name: _package(name) for name in (
        'plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
    )}
    load_module_with_stubs(plugin_root / 'models/enums/all.py', f'{PACKAGE}.models.enums.all', packages)
    return load_module_with_stubs(plugin_root / 'utils/skill_mentions.py', f'{PACKAGE}.utils.skill_mentions')


ATTACHED = {'skill_id': 9, 'name': 'Tester'}
CHAT_SKILL = {'skill_id': 4, 'name': 'Tester'}
OTHER_CHAT_SKILL = {'skill_id': 5, 'name': 'Writer'}


class TestMentionCandidates:
    def test_attached_skills_come_first_so_they_win_a_name_clash(self, mentions):
        assert mentions.merge_mention_candidates([ATTACHED], [CHAT_SKILL, OTHER_CHAT_SKILL]) == [
            ATTACHED, CHAT_SKILL, OTHER_CHAT_SKILL,
        ]

    def test_agent_without_attached_skills_offers_the_chat_skills(self, mentions):
        assert mentions.merge_mention_candidates([], [CHAT_SKILL]) == [CHAT_SKILL]


class TestWhoResolvesMentionsInPredict:
    @pytest.mark.parametrize('entity_name, version_details, expected', [
        ('application', {'agent_type': 'openai'}, True),
        ('application', {'agent_type': 'pipeline'}, False),
        ('application', None, False),
        ('skill', {'agent_type': 'openai'}, True),
        ('toolkit', {'agent_type': 'openai'}, False),
        ('dummy', None, False),
    ])
    def test_agents_and_skills_resolve_pipelines_do_not(self, mentions, entity_name, version_details, expected):
        assert mentions.resolves_mentions_in_predict(entity_name, version_details) is expected
