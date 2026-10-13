"""#6896 - chat skill participants are offered to agent and model replies for on-demand loading."""
import importlib.util
import pathlib
import types

import pytest

from fixtures.helpers import load_module_with_stubs, load_utils_module

PACKAGE = 'plugins.elitea_core'


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_backend = _load_sibling('test_6923_skill_participant_backend.py', 'skill_backend_harness_6896')
_predict = _load_sibling('test_auto_routing_predict_payload.py', 'predict_payload_harness_6896')
CHAT_PROJECT_ID = _backend.CHAT_PROJECT_ID
PUBLIC_PROJECT_ID = _backend.PUBLIC_PROJECT_ID
ParticipantRowsSession = _backend.ParticipantRowsSession
participant_row = _backend._row
env = _backend.env
builder = _predict.builder


@pytest.fixture
def mentions(isolated_sys_modules, models_path, utils_path):
    enums = load_module_with_stubs(models_path / 'enums/all.py', 'test_6896_enums')
    return load_utils_module(utils_path, 'skill_mentions', extra_stubs={f'{PACKAGE}.models.enums.all': enums})


def _skill(name, skill_id=1, **extra):
    return {'skill_id': skill_id, 'name': name, 'description': f'Use for {name}.', 'instructions': 'Body.', **extra}


class TestMergeDisclosableSkills:
    def test_chat_skills_follow_the_agents_own_skills(self, mentions):
        own = [_skill('reviewer', instruction_referenced=True)]
        merged = mentions.merge_disclosable_skills(own, [_skill('ui-review', 4)])
        assert [s['name'] for s in merged] == ['reviewer', 'ui-review']
        assert merged[0]['instruction_referenced'] is True
        assert merged[1]['instruction_referenced'] is False

    def test_the_agents_own_skill_wins_a_name_clash(self, mentions):
        own = [_skill('Reviewer', 9)]
        merged = mentions.merge_disclosable_skills(own, [_skill(' reviewer ', 4)])
        assert [s['skill_id'] for s in merged] == [9]

    def test_two_chat_skills_with_one_name_are_listed_once(self, mentions):
        merged = mentions.merge_disclosable_skills([], [_skill('writer', 4), _skill('Writer', 5)])
        assert [s['skill_id'] for s in merged] == [4]

    def test_a_nameless_chat_skill_is_not_listed(self, mentions):
        assert mentions.merge_disclosable_skills([], [_skill('', 4)]) == []


def _recipient(entity_name):
    return types.SimpleNamespace(id=55, entity_name=entity_name)


def _apply(env, entity_name, user_input='analyze this screenshot', version_details=None):
    session = ParticipantRowsSession([participant_row(56, CHAT_PROJECT_ID, 101), participant_row(57, PUBLIC_PROJECT_ID, 200)])
    msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55, sent_to=_recipient(entity_name))
    result = {'user_input': user_input}
    if version_details is not None:
        result['version_details'] = version_details
    env.mod.apply_participant_skill_mentions(session, msg_group, result, CHAT_PROJECT_ID)
    return result


class TestChatSkillsReachTheResponder:
    def test_an_agent_gets_the_chat_skills_without_a_tilde(self, env):
        result = _apply(env, 'application', version_details={'agent_type': 'openai'})
        chat_skills = result['version_details']['chat_skills']
        assert [(s['skill_project_id'], s['skill_id'], s['skill_version_id']) for s in chat_skills] == [
            (CHAT_PROJECT_ID, 10, 101), (PUBLIC_PROJECT_ID, 10, 200),
        ]
        assert 'mention_skills' not in result['version_details']

    def test_chat_skills_carry_their_description_for_the_registry(self, env):
        env.schemas[CHAT_PROJECT_ID][0].description = 'Use when reviewing UI screenshots.'
        result = _apply(env, 'application', version_details={'agent_type': 'openai'})
        assert result['version_details']['chat_skills'][0]['description'] == 'Use when reviewing UI screenshots.'

    def test_a_model_chat_gets_the_chat_skills_as_its_registry(self, env):
        result = _apply(env, 'dummy')
        assert [s['skill_version_id'] for s in result['attached_skills']] == [101, 200]
        assert {s['instruction_referenced'] for s in result['attached_skills']} == {False}
        assert result['user_input'] == 'analyze this screenshot'
        assert 'invoked_skills' not in result

    def test_a_model_chat_still_resolves_tilde_mentions(self, env):
        result = _apply(env, 'dummy', user_input='use ~Reviewer')
        assert [s['skill_version_id'] for s in result['invoked_skills']] == [101, 200]
        assert result['attached_skills']

    def test_a_skill_recipient_never_gets_other_chat_skills_listed(self, env):
        version_details = {'agent_type': 'openai'}
        result = _apply(env, 'skill', version_details=version_details)
        assert version_details == {'agent_type': 'openai'}
        assert 'attached_skills' not in result

    def test_a_skill_recipient_keeps_tilde_mentions(self, env):
        result = _apply(env, 'skill', user_input='use ~Reviewer', version_details={'agent_type': 'openai'})
        assert [s['skill_version_id'] for s in result['version_details']['mention_skills']] == [101, 200]
        assert 'chat_skills' not in result['version_details']

    def test_a_pipeline_gets_no_chat_skills(self, env):
        result = _apply(env, 'application', version_details={'agent_type': 'pipeline'})
        assert result['version_details'] == {'agent_type': 'pipeline'}

    def test_a_chat_without_usable_skill_participants_changes_nothing(self, env):
        session = ParticipantRowsSession([participant_row(56, PUBLIC_PROJECT_ID, 201)])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55, sent_to=_recipient('dummy'))
        result = {'user_input': 'hi'}
        env.mod.apply_participant_skill_mentions(session, msg_group, result, CHAT_PROJECT_ID)
        assert result == {'user_input': 'hi'}


class TestPredictPayloadListsChatSkills:
    @pytest.fixture
    def build(self, builder, mentions):
        build, llm, rpc = builder
        build.__globals__['merge_disclosable_skills'] = mentions.merge_disclosable_skills
        build.__globals__['log'] = types.SimpleNamespace(debug=lambda *a, **k: None)
        return build, llm

    def test_agent_registry_has_its_own_skills_then_the_chat_skills(self, build):
        run, llm = build
        own = _skill('reviewer', 9)
        run.__globals__['resolve_runtime_skills'] = lambda version: [own]
        request = _predict.parsed(llm, agent=True, auto=False)
        request.version_details.update(skills=[own], chat_skills=[_skill('reviewer', 4), _skill('ui-review', 5)])
        result = run(request, 2, skip_expansion=True)
        assert [s['skill_id'] for s in result['attached_skills']] == [9, 5]
        assert 'chat_skills' not in result['application']['version_details']

    def test_an_agent_without_own_skills_lists_the_chat_skills(self, build):
        run, llm = build
        request = _predict.parsed(llm, agent=True, auto=False)
        request.version_details['chat_skills'] = [_skill('ui-review', 5)]
        result = run(request, 2, skip_expansion=True)
        assert [s['skill_id'] for s in result['attached_skills']] == [5]

    def test_chat_skills_are_tilde_candidates_for_the_agent(self, build):
        run, llm = build
        consumed = []
        run.__globals__['consume_message_skills'] = lambda content, candidates: consumed.append(candidates) or (content, [])
        request = _predict.parsed(llm, agent=True, auto=False)
        chat_skill = _skill('ui-review', 5)
        request.version_details['chat_skills'] = [chat_skill]
        run(request, 2, skip_expansion=True)
        assert consumed == [[chat_skill]]

    def test_a_model_chat_passes_its_chat_skills_to_the_runtime(self, build):
        run, llm = build
        request = _predict.parsed(llm, agent=False, auto=False)
        request.attached_skills = [_skill('ui-review', 5)]
        result = run(request, 2, skip_expansion=True)
        assert [s['skill_id'] for s in result['attached_skills']] == [5]

    def test_a_model_chat_without_chat_skills_sends_no_registry(self, build):
        run, llm = build
        result = run(_predict.parsed(llm, agent=False, auto=False), 2, skip_expansion=True)
        assert 'attached_skills' not in result
