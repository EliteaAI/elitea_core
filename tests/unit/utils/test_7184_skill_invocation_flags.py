"""#7184: the Claude skill keys ``disable-model-invocation`` and ``user-invocable`` govern who may start a skill.

The keys stay in ``SkillVersion.meta.frontmatter``. Core copies them onto every skill entry it builds as
``disable_model_invocation`` / ``user_invocable`` (only when set) and keeps a model-only skill out of ``~name``
matching; the SDK hides and refuses a user-only skill on the model path.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

from fixtures.helpers import load_utils_module

PACKAGE = 'plugins.elitea_core'


def _load_sibling(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).parent / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_participants = _load_sibling('test_7126_project_skills_in_chat.py', 'project_skills_harness_7184')
_registry = _load_sibling('../test_6926_runtime_skill_registry.py', 'runtime_registry_harness_7184')
_spec = _load_sibling('../test_7183_skill_spec_frontmatter.py', 'spec_frontmatter_harness_7184')
CHAT_PROJECT_ID = _participants.CHAT_PROJECT_ID
FakeSkill = _participants._harness.FakeSkill
FakeVersion = _participants._harness.FakeVersion
_entry = _participants._entry
env = _participants.env
project_skills = _participants.project_skills
platform = _participants.platform
skill_utils = _registry.skill_utils
md = _spec.md
builder = _participants.builder

USER_ONLY = {'frontmatter': {'disable-model-invocation': True}}
MODEL_ONLY = {'frontmatter': {'user-invocable': False}}
BOTH_KEYS_SKILL = _spec.CLAUDE_CODE_SKILL.replace('disable-model-invocation: true\n',
                                                  'disable-model-invocation: true\nuser-invocable: false\n')


@pytest.fixture
def frontmatter(isolated_sys_modules, utils_path):
    return load_utils_module(utils_path, 'skill_frontmatter')


def _candidate(skill_id, name, instructions='Body.', **flags):
    return {'skill_id': skill_id, 'skill_version_id': 100 + skill_id, 'name': name, 'instructions': instructions, **flags}


class TestReader:
    @pytest.mark.parametrize('meta, expected', [
        (None, {}),
        ({}, {}),
        ({'frontmatter': {'license': 'MIT'}}, {}),
        ({'frontmatter': {'disable-model-invocation': False, 'user-invocable': True}}, {}),
        ({'frontmatter': {'disable-model-invocation': None, 'user-invocable': None}}, {}),
        (USER_ONLY, {'disable_model_invocation': True}),
        (MODEL_ONLY, {'user_invocable': False}),
        ({'frontmatter': {'disable-model-invocation': True, 'user-invocable': False}},
         {'disable_model_invocation': True, 'user_invocable': False}),
    ])
    def test_only_set_keys_become_payload_flags(self, frontmatter, meta, expected):
        assert frontmatter.invocation_flags(meta) == expected

    def test_a_rebuilt_entry_keeps_only_the_flags_it_carried(self, frontmatter):
        assert frontmatter.carried_invocation_flags({'name': 'a', 'user_invocable': False}) == {'user_invocable': False}
        assert frontmatter.carried_invocation_flags({'name': 'a'}) == {}


class TestValidation:
    @pytest.mark.parametrize('key, value', [
        ('disable-model-invocation', 'yes'), ('disable-model-invocation', 1), ('user-invocable', 0), ('user-invocable', 'false'),
    ])
    def test_a_non_boolean_value_is_refused_by_name(self, frontmatter, key, value):
        with pytest.raises(ValueError, match=f'Field "{key}" must be true or false'):
            frontmatter.extract_non_elitea_frontmatter({key: value})

    def test_booleans_and_null_pass_and_are_kept(self, frontmatter):
        assert frontmatter.extract_non_elitea_frontmatter({'disable-model-invocation': True, 'user-invocable': None}) == {
            'disable-model-invocation': True, 'user-invocable': None,
        }

    def test_the_two_keys_are_no_longer_reported_as_unused(self, frontmatter):
        assert frontmatter.list_unused_frontmatter_keys(
            {'disable-model-invocation': True, 'user-invocable': False, 'argument-hint': '[v]'}
        ) == ['argument-hint']

    def test_a_stored_non_boolean_is_a_400_on_save(self, skill_utils):
        with pytest.raises(skill_utils.SkillSpecViolationError, match='user-invocable'):
            skill_utils.clean_stored_frontmatter({'frontmatter': {'user-invocable': 'no'}})


class TestImport:
    def test_both_keys_are_stored_as_booleans_and_not_listed_as_unused(self, md):
        result, _, version = _spec.imported_version(md, BOTH_KEYS_SKILL)
        assert version['meta']['frontmatter']['disable-model-invocation'] is True
        assert version['meta']['frontmatter']['user-invocable'] is False
        assert 'disable-model-invocation' not in result['unused_frontmatter_keys']
        assert 'user-invocable' not in result['unused_frontmatter_keys']

    def test_a_quoted_value_is_refused(self, md):
        with pytest.raises(ValueError, match='Field "user-invocable" must be true or false'):
            _spec.imported_version(md, _spec.CLAUDE_CODE_SKILL.replace('argument-hint', "user-invocable: 'false'\nargument-hint"))


class TestRuntimeRegistry:
    def test_the_disclosable_entries_carry_the_flags_the_mappings_carry(self, skill_utils):
        version_details = {'instructions': 'Help.', 'skills': [
            _candidate(1, 'deploy', disable_model_invocation=True),
            _candidate(2, 'style-guide', user_invocable=False),
            _candidate(3, 'plain'),
        ]}
        registry = skill_utils.resolve_runtime_skills(version_details)
        assert [(s['name'], s.get('disable_model_invocation'), s.get('user_invocable')) for s in registry] == [
            ('deploy', True, None), ('style-guide', None, False), ('plain', None, None),
        ]
        assert 'user_invocable' not in registry[2] and 'disable_model_invocation' not in registry[2]

    def test_an_authors_reference_to_a_model_only_skill_is_still_de_sigiled(self, skill_utils):
        version_details = {'instructions': 'Always use ~style-guide.', 'skills': [_candidate(2, 'style-guide', user_invocable=False)]}
        [entry] = skill_utils.resolve_runtime_skills(version_details)
        assert version_details['instructions'] == 'Always use style-guide.'
        assert entry['instruction_referenced'] is True

    def test_an_authors_reference_to_a_user_only_skill_is_de_sigiled_and_the_flag_kept(self, skill_utils):
        version_details = {'instructions': 'Run ~deploy when asked.', 'skills': [_candidate(1, 'deploy', disable_model_invocation=True)]}
        [entry] = skill_utils.resolve_runtime_skills(version_details)
        assert version_details['instructions'] == 'Run deploy when asked.'
        assert entry['instruction_referenced'] is True and entry['disable_model_invocation'] is True


class TestUserMentions:
    @pytest.fixture(autouse=True)
    def limit(self, skill_utils, monkeypatch):
        monkeypatch.setattr(skill_utils, 'get_max_invoked_skills_per_turn', lambda: 5)

    CANDIDATES = [_candidate(1, 'deploy', disable_model_invocation=True), _candidate(2, 'style-guide', user_invocable=False),
                  _candidate(3, 'plain')]

    def test_a_model_only_name_stays_as_typed_and_is_not_injected(self, skill_utils):
        text, invoked = skill_utils.consume_message_skills('~style-guide ~plain ~deploy', self.CANDIDATES)
        assert text == '~style-guide plain deploy'
        assert [s['name'] for s in invoked] == ['plain', 'deploy']

    def test_block_content_is_gated_the_same_way(self, skill_utils):
        blocks, invoked = skill_utils.consume_message_skills(
            [{'type': 'image_url'}, {'type': 'text', 'text': 'use ~style-guide and ~plain'}], self.CANDIDATES,
        )
        assert blocks[1]['text'] == 'use ~style-guide and plain'
        assert [s['name'] for s in invoked] == ['plain']

    def test_a_model_only_name_does_not_count_toward_the_cap(self, skill_utils):
        candidates = [_candidate(9, 'hidden', user_invocable=False)] + [_candidate(i, f's{i}') for i in range(1, 6)]
        _text, invoked = skill_utils.consume_message_skills('~hidden ~s1 ~s2 ~s3 ~s4 ~s5', candidates)
        assert len(invoked) == 5

    def test_precedence_is_resolved_before_the_gate_so_a_same_named_participant_does_not_match(self, skill_utils):
        attached = _candidate(2, 'style-guide', user_invocable=False)
        participant = _candidate(7, 'style-guide', instructions='Participant body.')
        text, invoked = skill_utils.consume_message_skills('~style-guide', [attached, participant])
        assert (text, invoked) == ('~style-guide', [])

    def test_a_model_only_participant_behind_a_mentionable_attached_skill_does_not_hide_the_name(self, skill_utils):
        attached = _candidate(2, 'style-guide')
        participant = _candidate(7, 'style-guide', user_invocable=False)
        text, invoked = skill_utils.consume_message_skills('~style-guide', [attached, participant])
        assert text == 'style-guide' and invoked[0]['skill_id'] == 2

    def test_model_only_names_are_resolved_by_precedence(self, skill_utils):
        assert skill_utils.model_only_names(self.CANDIDATES) == {'style-guide'}
        assert skill_utils.model_only_names([_candidate(2, 'X'), _candidate(7, 'x', user_invocable=False)]) == set()


class TestProjectSkillMentions:
    def test_a_hidden_catalog_name_is_skipped_without_a_body_query(self, project_skills, platform):
        catalog = [_entry(1, 'style-guide', user_invocable=False), _entry(2, 'plain')]
        mentioned = project_skills.resolve_mentioned_project_skills(
            '~style-guide ~plain', set(), catalog, CHAT_PROJECT_ID, hidden_names={'style-guide'},
        )
        assert [m['name'] for m in mentioned] == ['plain']
        assert platform.body_requests == [[102]]

    def test_strict_mode_never_steers_a_typo_to_a_hidden_skill(self, project_skills, platform):
        catalog = [_entry(1, 'style-guide', user_invocable=False)]
        assert project_skills.resolve_mentioned_project_skills(
            '~style-gude please', set(), catalog, CHAT_PROJECT_ID, strict=True, hidden_names={'style-guide'},
        ) == []
        assert project_skills.resolve_mentioned_project_skills(
            '~style-gude please', {'style-guide'}, catalog, CHAT_PROJECT_ID, strict=True, hidden_names={'style-guide'},
        ) == []

    def test_a_hidden_name_is_not_counted_in_the_over_limit_refusal(self, project_skills, platform):
        catalog = [_entry(i, f's{i}') for i in range(1, 8)]
        text = '~hidden ' + ' '.join(f'~s{i}' for i in range(1, 7))
        with pytest.raises(project_skills.SkillInvocationLimitError) as error:
            project_skills.resolve_mentioned_project_skills(text, {'hidden'}, catalog, CHAT_PROJECT_ID, hidden_names={'hidden'})
        assert error.value.count == 6

    def test_the_catalog_carries_the_flags_of_the_default_version(self, project_skills, platform):
        platform.rows = [
            (1, 'deploy', 'd', {}, 10, 'base', USER_ONLY, True),
            (2, 'style-guide', 'd', {}, 20, 'base', MODEL_ONLY, True),
            (3, 'plain', 'd', {}, 30, 'base', {}, True),
        ]
        entries = _participants._private_catalog(project_skills, platform, platform.rows)
        assert [(e['name'], e.get('disable_model_invocation'), e.get('user_invocable')) for e in entries] == [
            ('deploy', True, None), ('style-guide', None, False), ('plain', None, None),
        ]
        assert 'user_invocable' not in entries[2]

    def test_collecting_project_candidates_hides_model_only_names_from_every_source(self, env, monkeypatch):
        catalog = [_entry(1, 'style-guide', user_invocable=False), _entry(2, 'reviewer')]
        monkeypatch.setattr(env.mod, 'is_project_skills_in_chat_enabled', lambda project_id: True)
        monkeypatch.setattr(env.mod, 'build_project_skill_catalog', lambda *args: list(catalog))
        seen = {}

        def resolve(user_input, known_names, catalog_, project_id, strict=False, hidden_names=frozenset()):
            seen.update(known=set(known_names), hidden=set(hidden_names))
            return []

        monkeypatch.setattr(env.mod, 'resolve_mentioned_project_skills', resolve)
        recipient = types.SimpleNamespace(entity_name='application')
        env.mod.collect_project_skill_candidates(
            recipient, 5, None, '~style-guide ~reviewer', [_candidate(9, 'tester', user_invocable=False)],
            [_candidate(8, 'reviewer', user_invocable=False)], CHAT_PROJECT_ID,
        )
        assert seen == {'known': {'tester', 'reviewer'}, 'hidden': {'tester', 'reviewer', 'style-guide'}}


class TestResumeKeepsUserStartedSkills:
    """A HITL approve/reject/edit resumes with the decision as user_input; the ~skills of the resumed message stay active."""

    DEPLOY = {'skill_id': 1, 'skill_version_id': 101, 'name': 'deploy', 'instructions': 'Deploy body.',
              'disable_model_invocation': True}

    @pytest.fixture(autouse=True)
    def real_mentions(self, env, monkeypatch):
        def consume(text, candidates):
            flat = text if isinstance(text, str) else ' '.join(b.get('text') or '' for b in text or [])
            by_name = {c['name']: c for c in candidates}
            found = [by_name[n] for n in by_name if f'~{n}' in flat]
            cleaned = flat.replace('~', '') if isinstance(text, str) else text
            return cleaned, [{'skill_id': s['skill_id'], 'name': s['name'], 'instructions': s['instructions']}
                             for s in found]

        def merge(message, resumed):
            seen = {s['skill_id'] for s in message}
            return [*message, *[s for s in resumed or [] if s['skill_id'] not in seen]]

        monkeypatch.setattr(env.mod, 'consume_message_skills', consume)
        monkeypatch.setattr(env.mod, 'merge_invoked_skills', merge)

    @staticmethod
    def _blocks(text):
        """The shape generate_user_input returns: a list of content blocks, never a string."""
        return [{'type': 'text', 'text': text}]

    def _apply(self, env, user_input, mention_source, skills, entity_name='application', rows=()):
        session = _participants.ParticipantRowsSession(list(rows))
        result = {'user_input': user_input, 'version_details': {'agent_type': 'openai', 'skills': skills}}
        env.mod.apply_participant_skill_mentions(
            session, _participants._msg_group(entity_name), result, CHAT_PROJECT_ID, strict_mentions=True,
            mention_source=mention_source,
        )
        return result

    def test_an_agent_resume_re_resolves_the_skills_of_the_resumed_message(self, env):
        result = self._apply(env, 'approve', self._blocks('~deploy ship it'), [self.DEPLOY])
        assert result['user_input'] == 'approve'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['deploy']
        assert result[env.mod.INVOKED_SKILLS_KEY][0]['instructions'] == 'Deploy body.'

    def test_a_resume_of_a_message_without_mentions_adds_nothing(self, env):
        result = self._apply(env, 'approve', self._blocks('ship it'), [self.DEPLOY])
        assert env.mod.INVOKED_SKILLS_KEY not in result and result['user_input'] == 'approve'

    def test_without_a_mention_source_nothing_changes(self, env):
        result = self._apply(env, 'approve', None, [self.DEPLOY])
        assert env.mod.INVOKED_SKILLS_KEY not in result

    def test_an_llm_chat_resume_still_consumes_the_decision_text(self, env):
        rows = [_participants.participant_row(56, CHAT_PROJECT_ID, 101)]
        result = self._apply(env, 'edited: use ~Reviewer', self._blocks('ship it'), [], entity_name='dummy', rows=rows)
        assert result['user_input'] == 'edited: use Reviewer'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['Reviewer']

    def test_an_llm_chat_resume_merges_the_resumed_and_the_decision_skills(self, env):
        rows = [_participants.participant_row(56, CHAT_PROJECT_ID, 101)]
        result = self._apply(env, 'also ~Reviewer', self._blocks('~Reviewer go'), [], entity_name='dummy', rows=rows)
        assert result['user_input'] == 'also Reviewer'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['Reviewer']

    def test_an_agent_edit_resume_keeps_the_resumed_skill_and_leaves_the_edit_to_predict(self, env):
        plain = {'skill_id': 3, 'skill_version_id': 103, 'name': 'plain', 'instructions': 'Plain body.'}
        result = self._apply(env, 'edit: also ~plain', self._blocks('~deploy ship it'), [self.DEPLOY, plain])
        assert result['user_input'] == 'edit: also ~plain'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['deploy']

    def test_an_llm_chat_edit_resume_keeps_the_resumed_skill_next_to_the_edits_own(self, env):
        env.schemas[CHAT_PROJECT_ID].append(FakeSkill(11, 'Writer', [FakeVersion(110, 'base', instructions='Writer body.')]))
        writer_row = (types.SimpleNamespace(id=57, entity_meta={'id': 11, 'project_id': CHAT_PROJECT_ID}), {'version_id': 110})
        rows = [_participants.participant_row(56, CHAT_PROJECT_ID, 101), writer_row]
        result = self._apply(env, 'edit: ~Writer too', self._blocks('~Reviewer go'), [], entity_name='dummy', rows=rows)
        assert result['user_input'] == 'edit: Writer too'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['Writer', 'Reviewer']

    def test_project_skills_are_resolved_for_both_the_resumed_and_the_decision_text(self, env, monkeypatch):
        seen = {}

        def collect(recipient, sender_id, conversation, user_input, *args):
            seen['text'] = user_input
            return []

        monkeypatch.setattr(env.mod, 'collect_project_skill_candidates', collect)
        self._apply(env, 'edit: ~b', self._blocks('~a go'), [self.DEPLOY])
        assert [block['text'] for block in seen['text']] == ['~a go', 'edit: ~b']

    def test_a_continue_that_resends_the_same_text_lists_each_skill_once(self, env):
        rows = [_participants.participant_row(56, CHAT_PROJECT_ID, 101)]
        result = self._apply(env, '~Reviewer go', self._blocks('~Reviewer go'), [], entity_name='dummy', rows=rows)
        assert result['user_input'] == 'Reviewer go'
        assert [s['name'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == ['Reviewer']


class TestMergeInvokedSkills:
    @pytest.fixture(autouse=True)
    def limit(self, skill_utils, monkeypatch):
        monkeypatch.setattr(skill_utils, 'get_max_invoked_skills_per_turn', lambda: 2)

    def test_message_skills_come_first_and_resumed_duplicates_are_dropped(self, skill_utils):
        message = [{'skill_id': 1, 'skill_project_id': 2, 'name': 'a'}]
        resumed = [{'skill_id': 1, 'skill_project_id': 2, 'name': 'a'}, {'skill_id': 1, 'skill_project_id': 9, 'name': 'twin'}]
        assert [s['name'] for s in skill_utils.merge_invoked_skills(message, resumed)] == ['a', 'twin']

    def test_the_merged_list_is_held_to_the_per_turn_cap(self, skill_utils):
        message = [{'skill_id': 1, 'name': 'a'}, {'skill_id': 2, 'name': 'b'}]
        with pytest.raises(skill_utils.SkillInvocationLimitError):
            skill_utils.merge_invoked_skills(message, [{'skill_id': 3, 'name': 'c'}])
        assert len(skill_utils.merge_invoked_skills(message, None)) == 2


class TestPredictPayloadKeepsResumedSkills:
    def test_the_resumed_skills_are_kept_and_counted_as_applied(self, builder):
        build, llm, _rpc = builder
        value = _participants.parsed(llm, agent=True)
        value.user_input = 'approve'
        resumed = [{'skill_id': 9, 'name': 'deploy', 'icon_meta': None, 'instructions': 'Deploy body.'}]
        payload = build(value, 2, skip_expansion=True, invoked_skills=resumed)
        assert payload['invoked_skills'] == resumed
        assert [s['name'] for s in payload['applied_skills']] == ['deploy']

    def test_without_resumed_skills_the_payload_is_as_before(self, builder):
        build, llm, _rpc = builder
        payload = build(_participants.parsed(llm, agent=True), 2, skip_expansion=True)
        assert payload['invoked_skills'] == [] and payload['applied_skills'] == []


class TestParticipantsAndMenu:
    def test_a_pinned_participant_version_reports_its_flags_to_the_menu(self, env):
        skill = FakeSkill(10, 'Reviewer', [FakeVersion(100, 'base', meta=MODEL_ONLY), FakeVersion(101, 'v2')])
        assert env.mod.describe_pinned_version(skill, 100, False)['user_invocable'] is False
        assert 'user_invocable' not in env.mod.describe_pinned_version(skill, 101, False)

    def test_a_participant_candidate_carries_the_flags_of_its_version(self, env):
        skill_run_utils = sys.modules[f'{PACKAGE}.utils.skill_run_utils']
        skill = FakeSkill(10, 'deploy', [FakeVersion(100, 'base', meta=USER_ONLY)], description='Deploys.')
        target = skill_run_utils.build_skill_run_target(skill, 100, False)
        assert target.invocation_flags == {'disable_model_invocation': True}
        assert env.mod.mention_candidate(target, CHAT_PROJECT_ID)['disable_model_invocation'] is True
        plain = skill_run_utils.build_skill_run_target(FakeSkill(11, 'plain', [FakeVersion(110)]), 110, False)
        assert 'disable_model_invocation' not in env.mod.mention_candidate(plain, CHAT_PROJECT_ID)

    def test_the_menu_endpoint_reports_the_flags_of_the_pinned_version(self, skill_utils):
        mapping = types.SimpleNamespace(
            skill_version_id=100,
            skill=types.SimpleNamespace(id=10, name='style-guide', description='d'),
            skill_version=types.SimpleNamespace(name='base', meta=MODEL_ONLY),
        )
        plain = types.SimpleNamespace(
            skill_version_id=110, skill=types.SimpleNamespace(id=11, name='plain', description='d'),
            skill_version=types.SimpleNamespace(name='base', meta={}),
        )

        class Query:
            def __init__(self, rows):
                self.rows = rows

            def filter(self, *args):
                return self

            def first(self):
                return self.rows[0]

            def all(self):
                return self.rows

        class Session:
            def query(self, model):
                return Query([object()]) if 'ApplicationVersion' in repr(model) else Query([mapping, plain])

            def flush(self):
                pass

        items = skill_utils.get_available_skills_for_agent(1, 300, session=Session())
        assert [(item['name'], item.get('user_invocable')) for item in items] == [('style-guide', False), ('plain', None)]
        assert 'user_invocable' not in items[1]
