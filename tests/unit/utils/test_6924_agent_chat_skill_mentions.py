import pytest

from fixtures.helpers import load_module_with_stubs, load_utils_module

PACKAGE = 'plugins.elitea_core'


@pytest.fixture
def mentions(isolated_sys_modules, models_path, utils_path):
    enums = load_module_with_stubs(models_path / 'enums/all.py', 'test_6924_enums')
    return load_utils_module(utils_path, 'skill_mentions', extra_stubs={f'{PACKAGE}.models.enums.all': enums})


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
