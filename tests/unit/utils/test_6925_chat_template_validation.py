import types

import pytest

from fixtures.helpers import load_module_with_stubs, load_utils_module

PACKAGE = 'plugins.elitea_core'
TEMPLATE_PROJECT_ID = 2
PUBLIC_PROJECT_ID = 1


class SkillParticipantError(ValueError):
    pass


@pytest.fixture
def harness(isolated_sys_modules, models_path, utils_path):
    loaded = []
    unavailable = {}

    def load_skill_participant_target(entity_meta, chat_project_id, version_id):
        loaded.append((entity_meta, chat_project_id, version_id))
        if entity_meta['id'] in unavailable:
            raise SkillParticipantError(unavailable[entity_meta['id']])

    skill_participants = types.ModuleType(f'{PACKAGE}.utils.skill_participant_utils')
    skill_participants.load_skill_participant_target = load_skill_participant_target
    enums = load_module_with_stubs(models_path / 'enums/all.py', 'test_6925_enums')
    pd = load_module_with_stubs(models_path / 'pd/chat_template.py', 'test_6925_chat_template_pd')
    validation = load_utils_module(utils_path, 'chat_template_validation', extra_stubs={
        f'{PACKAGE}.utils.skill_participant_utils': skill_participants,
        f'{PACKAGE}.models.enums.all': enums,
        f'{PACKAGE}.models.pd.chat_template': pd,
    })

    def validate(participants, stored=()):
        entries = [pd.ChatTemplateParticipant.model_validate(p) for p in participants]
        validation.validate_template_participants(entries, list(stored), TEMPLATE_PROJECT_ID)

    return types.SimpleNamespace(validate=validate, loaded=loaded, unavailable=unavailable,
                                 Error=validation.ChatTemplateParticipantError)


AGENT = {'id': 3, 'entity_name': 'application', 'project_id': TEMPLATE_PROJECT_ID, 'name': 'Agent'}
PIPELINE = {'id': 4, 'entity_name': 'pipeline', 'project_id': TEMPLATE_PROJECT_ID, 'agent_type': 'pipeline'}
TOOLKIT = {'id': 5, 'entity_name': 'toolkit', 'project_id': TEMPLATE_PROJECT_ID, 'toolkit_type': 'github'}
MCP = {'id': 6, 'entity_name': 'mcp', 'project_id': TEMPLATE_PROJECT_ID}
USER = {'id': 7, 'entity_name': 'user'}
OWN_SKILL = {'id': 8, 'entity_name': 'skill', 'project_id': TEMPLATE_PROJECT_ID, 'name': 'Reviewer'}
CATALOG_SKILL = {'id': 8, 'entity_name': 'skill', 'project_id': PUBLIC_PROJECT_ID, 'name': 'Reviewer'}
LEGACY = {'id': 9, 'entity_name': 'prompt', 'project_id': TEMPLATE_PROJECT_ID, 'name': 'Old prompt'}


class TestAllowList:
    def test_a_template_can_mix_every_supported_type(self, harness):
        harness.validate([AGENT, PIPELINE, TOOLKIT, MCP, USER, OWN_SKILL, CATALOG_SKILL])

    def test_an_unknown_type_is_rejected_on_create(self, harness):
        with pytest.raises(harness.Error, match='Unsupported participant type: prompt'):
            harness.validate([AGENT, LEGACY])

    def test_an_unchanged_legacy_entry_is_kept_on_update(self, harness):
        harness.validate([LEGACY, AGENT], stored=[AGENT, LEGACY])

    def test_a_changed_legacy_entry_is_rejected_on_update(self, harness):
        with pytest.raises(harness.Error, match='Unsupported participant type: prompt'):
            harness.validate([{**LEGACY, 'name': 'Renamed'}], stored=[LEGACY])

    def test_a_new_legacy_entry_next_to_a_stored_one_is_rejected(self, harness):
        with pytest.raises(harness.Error):
            harness.validate([LEGACY, {**LEGACY, 'id': 10}], stored=[LEGACY])


class TestDuplicates:
    def test_the_same_skill_twice_is_rejected(self, harness):
        with pytest.raises(harness.Error, match='skill #8 is listed more than once'):
            harness.validate([OWN_SKILL, {**OWN_SKILL, 'name': 'Other name'}])

    def test_an_own_and_a_catalog_skill_with_the_same_id_are_different_participants(self, harness):
        harness.validate([OWN_SKILL, CATALOG_SKILL])

    def test_duplicates_already_stored_are_tolerated_on_update(self, harness):
        harness.validate([AGENT, AGENT], stored=[AGENT, AGENT])

    def test_a_new_copy_of_a_stored_entry_is_rejected(self, harness):
        with pytest.raises(harness.Error, match='application #3 is listed more than once'):
            harness.validate([AGENT, AGENT], stored=[AGENT])


class TestSkillEntries:
    def test_a_new_skill_is_checked_against_the_template_project_with_its_default_version(self, harness):
        harness.validate([CATALOG_SKILL])
        assert harness.loaded == [({'id': 8, 'project_id': PUBLIC_PROJECT_ID}, TEMPLATE_PROJECT_ID, None)]

    def test_a_skill_without_project_is_rejected(self, harness):
        with pytest.raises(harness.Error, match='requires project_id'):
            harness.validate([{**OWN_SKILL, 'project_id': None}])
        assert harness.loaded == []

    def test_an_unavailable_skill_is_rejected_with_the_reason(self, harness):
        harness.unavailable[8] = 'Skill has no runnable version'
        with pytest.raises(SkillParticipantError, match='no runnable version'):
            harness.validate([CATALOG_SKILL])

    def test_an_unchanged_skill_is_not_rechecked_on_update(self, harness):
        harness.unavailable[8] = 'Skill not found'
        harness.validate([OWN_SKILL, AGENT], stored=[OWN_SKILL])
        assert harness.loaded == []

    def test_other_types_are_not_checked_as_skills(self, harness):
        harness.validate([AGENT, PIPELINE, TOOLKIT, MCP, USER])
        assert harness.loaded == []
