"""Issue #6930 - an agent version can carry up to the admin-configured number of skills (default 50).

Server-side copies of an accepted skill set (publish, "Save as version") skip the
cap, so lowering it never truncates them. Import and fork take their skill list
from the client and stay capped. The per-message ``~skill`` cap is configured separately
(default 5); instruction references are not capped.

Run via:
    python tests/run_tests.py integration/test_6930_skill_attach_limit.py -v
"""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_5955_published_mapping_guards import skill_utils_module  # noqa: E402,F401
from test_6414_agent_version_validation import (  # noqa: E402,F401
    _Session,
    _draft_agent_version,
    bind_session,
)

@pytest.fixture(autouse=True)
def runtime_models(skill_utils_module, monkeypatch):  # noqa: F811
    """The shared stub tree has a mapping class without a constructor and no pipeline type."""
    models_skill = sys.modules['plugins.elitea_core.models.skill']

    class _Mapping(models_skill.EntitySkillMapping):
        def __init__(self, **columns):
            self.__dict__.update(columns)

    monkeypatch.setattr(skill_utils_module, 'EntitySkillMapping', _Mapping)
    monkeypatch.setattr(
        skill_utils_module, 'AgentTypes',
        types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
    )
    return _Mapping


def _attach_session(skill_utils_module, attached_count):  # noqa: F811
    models_all = sys.modules['plugins.elitea_core.models.all']
    models_skill = sys.modules['plugins.elitea_core.models.skill']
    mapping = skill_utils_module.EntitySkillMapping
    return _Session(
        results={
            models_all.ApplicationVersion: _draft_agent_version(),
            models_skill.Skill: types.SimpleNamespace(id=7, name='sk'),
            models_skill.SkillVersion: types.SimpleNamespace(id=70, name='base'),
            mapping: None,
        },
        counts={mapping: attached_count},
    )


@pytest.fixture
def admin_config(skill_utils_module, monkeypatch):  # noqa: F811
    def _set(config):
        monkeypatch.setattr(
            skill_utils_module, 'this', types.SimpleNamespace(descriptor=types.SimpleNamespace(config=config)),
        )

    _set({})
    return _set


def _attach(skill_utils_module, **kwargs):  # noqa: F811
    return skill_utils_module.attach_skill_to_agent(
        project_id=2, entity_version_id=5, skill_id=7, skill_version_id=70, **kwargs,
    )


class TestAttachLimit:
    @pytest.mark.parametrize('attached_count', [0, 48, 49])
    def test_attaches_up_to_the_default_of_50(
        self, skill_utils_module, bind_session, admin_config, attached_count,  # noqa: F811
    ):
        session = bind_session(_attach_session(skill_utils_module, attached_count))

        _attach(skill_utils_module)

        assert len(session.added) == 1

    def test_the_51st_attach_is_a_400_naming_the_maximum(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        session = bind_session(_attach_session(skill_utils_module, 50))

        with pytest.raises(skill_utils_module.SkillLimitExceededError) as exc:
            _attach(skill_utils_module)

        assert exc.value.http_status == 400
        assert 'Maximum allowed is 50.' in str(exc.value)
        assert session.added == []

    def test_admin_config_sets_the_maximum(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_skills_per_agent': 3}})
        bind_session(_attach_session(skill_utils_module, 3))

        with pytest.raises(skill_utils_module.SkillLimitExceededError) as exc:
            _attach(skill_utils_module)

        assert 'Maximum allowed is 3.' in str(exc.value)
        assert skill_utils_module.get_max_skills_per_agent() == 3

    @pytest.mark.parametrize('bad_value', [0, -4, 'many', None])
    def test_a_bad_admin_value_falls_back_to_50(
        self, skill_utils_module, admin_config, bad_value,  # noqa: F811
    ):
        admin_config({'skills': {'max_skills_per_agent': bad_value}})

        assert skill_utils_module.get_max_skills_per_agent() == 50


class TestCopiesKeepEverySkillAboveALoweredLimit:
    def test_publish_copy_skips_the_cap(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_skills_per_agent': 2}})
        session = bind_session(_attach_session(skill_utils_module, 10))

        skill_utils_module.attach_skill_to_public_copy(
            project_id=1, entity_version_id=5, skill_id=7, skill_version_id=70,
        )

        assert len(session.added) == 1

    def test_import_and_fork_still_enforce_the_cap(
        self, skill_utils_module, bind_session, admin_config,  # noqa: F811
    ):
        """Import and fork attach through attach_skill_to_agent with a skill list
        the client supplies, so only server-side copies may skip the cap."""
        admin_config({'skills': {'max_skills_per_agent': 2}})
        session = bind_session(_attach_session(skill_utils_module, 2))

        with pytest.raises(skill_utils_module.SkillLimitExceededError):
            _attach(skill_utils_module)

        assert session.added == []


def _skills(count):
    return [
        {
            'skill_id': i,
            'skill_version_id': 100 + i,
            'name': f'skill-{i}',
            'description': f'Use when case {i}.',
            'instructions': f'Body {i}.',
        }
        for i in range(1, count + 1)
    ]


class TestInvocationCaps:
    def test_a_message_over_the_default_cap_is_rejected(self, skill_utils_module, admin_config):  # noqa: F811
        message = ' '.join(f'~skill-{i}' for i in range(1, 8))

        with pytest.raises(skill_utils_module.SkillInvocationLimitError) as exc:
            skill_utils_module.consume_message_skills(message, _skills(50))

        assert (exc.value.count, exc.value.max_allowed) == (7, 5)
        assert 'Maximum allowed is 5' in str(exc.value)

    def test_the_early_chat_check_rejects_a_message_over_the_cap(self, skill_utils_module, admin_config):  # noqa: F811
        message = ' '.join(f'~skill-{i}' for i in range(1, 7))

        with pytest.raises(skill_utils_module.SkillInvocationLimitError):
            skill_utils_module.validate_message_skill_limit(message, _skills(50))

    def test_the_early_chat_check_accepts_a_message_at_the_cap(self, skill_utils_module, admin_config):  # noqa: F811
        message = ' '.join(f'~skill-{i}' for i in range(1, 6))

        assert skill_utils_module.validate_message_skill_limit(message, _skills(50)) is None

    def test_a_message_at_the_cap_invokes_every_mention(self, skill_utils_module, admin_config):  # noqa: F811
        message = ' '.join(f'~skill-{i}' for i in range(1, 6))

        _cleaned, invoked = skill_utils_module.consume_message_skills(message, _skills(50))

        assert [s['name'] for s in invoked] == [f'skill-{i}' for i in range(1, 6)]

    def test_the_per_message_cap_is_configured_apart_from_the_attach_limit(
        self, skill_utils_module, admin_config,  # noqa: F811
    ):
        admin_config({'skills': {'max_skills_per_agent': 50, 'max_invoked_skills_per_turn': 7}})

        _cleaned, invoked = skill_utils_module.consume_message_skills(
            ' '.join(f'~skill-{i}' for i in range(1, 8)), _skills(50),
        )
        assert len(invoked) == 7
        with pytest.raises(skill_utils_module.SkillInvocationLimitError):
            skill_utils_module.consume_message_skills(' '.join(f'~skill-{i}' for i in range(1, 9)), _skills(50))

    def test_the_cap_counts_mentions_across_text_blocks(self, skill_utils_module, admin_config):  # noqa: F811
        blocks = [
            {'type': 'text', 'text': '~skill-1 ~skill-2 ~skill-3'},
            {'type': 'text', 'text': '~skill-4 ~skill-5 ~skill-6'},
        ]

        with pytest.raises(skill_utils_module.SkillInvocationLimitError):
            skill_utils_module.consume_message_skills(blocks, _skills(50))

    def test_a_skill_mentioned_in_two_text_blocks_counts_once(self, skill_utils_module, admin_config):  # noqa: F811
        blocks = [
            {'type': 'text', 'text': ' '.join(f'~skill-{i}' for i in range(1, 6))},
            {'type': 'text', 'text': 'and again ~skill-1'},
        ]

        _rebuilt, invoked = skill_utils_module.consume_message_skills(blocks, _skills(50))

        assert [s['name'] for s in invoked] == [f'skill-{i}' for i in range(1, 6)]

    def test_a_repeated_mention_counts_once(self, skill_utils_module, admin_config):  # noqa: F811
        message = '~skill-1 ~skill-1 ~skill-2 ~skill-3 ~skill-4 ~skill-5 ~skill-2'

        _cleaned, invoked = skill_utils_module.consume_message_skills(message, _skills(50))

        assert [s['name'] for s in invoked] == [f'skill-{i}' for i in range(1, 6)]

    def test_an_unknown_name_does_not_count(self, skill_utils_module, admin_config):  # noqa: F811
        message = ' '.join(f'~skill-{i}' for i in range(1, 6)) + ' ~nobody'

        _cleaned, invoked = skill_utils_module.consume_message_skills(message, _skills(50))

        assert len(invoked) == 5

    def test_instructions_may_reference_every_attached_skill(self, skill_utils_module, admin_config):  # noqa: F811
        skills = _skills(50)
        version_details = {
            'instructions': ' '.join(f'Use ~skill-{i} for case {i}.' for i in range(1, 51)),
            'skills': skills,
        }

        registry = skill_utils_module.resolve_runtime_skills(version_details)

        assert len(registry) == 50
        assert all(entry['instruction_referenced'] for entry in registry)
        assert '~' not in version_details['instructions']

    def test_the_reference_hint_is_a_flag_not_description_text(self, skill_utils_module):  # noqa: F811
        skills = _skills(2)
        version_details = {'instructions': 'Use ~skill-1 when asked.', 'skills': skills}

        registry = {s['name']: s for s in skill_utils_module.resolve_runtime_skills(version_details)}

        assert registry['skill-1']['instruction_referenced'] is True
        assert registry['skill-1']['description'] == 'Use when case 1.'
        assert registry['skill-2']['instruction_referenced'] is False
