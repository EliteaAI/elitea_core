"""Issue #5978 - shaping version_details for the runtime.

`apply_runtime_skills` is what lets a sub-agent run with the skills attached to
its own version. Every invariant below fails silently when broken: the child
simply runs without its skills, which is indistinguishable from the original bug.

Run via:
    python tests/run_tests.py integration/test_5978_runtime_skills.py -v
"""

import pytest


UPPERCASE = {
    'skill_id': 1,
    'skill_version_id': 10,
    'name': 'shout',
    'description': 'Use when the reply should be loud',
    'instructions': 'Write every letter in upper case.',
}
TERSE = {
    'skill_id': 2,
    'skill_version_id': 20,
    'name': 'terse',
    'description': 'Use when the reply should be short',
    'instructions': 'Answer in one sentence.',
}


class TestEmptyPayloadUntouched:
    """The webhook/API/MCP predict shape sends no version_details.

    The SDK refetches only while the dict stays falsy, so inventing a key here
    strands it with a truthy payload that has no llm_settings.
    """

    def test_empty_dict_gains_no_keys(self, skill_utils_module):
        version_details = {}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details == {}

    def test_none_instructions_are_left_alone(self, skill_utils_module):
        version_details = {'instructions': None, 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] is None


class TestPipelineInstructionsPreserved:
    """A pipeline's instructions are its YAML graph, not a prompt."""

    def test_yaml_survives_byte_identical(self, skill_utils_module):
        yaml = 'nodes:\n  - id: start\n    type: llm\n'
        version_details = {
            'instructions': yaml,
            'agent_type': 'pipeline',
            'skills': [UPPERCASE],
        }

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == yaml
        assert version_details['attached_skills'] == []


class TestChannelsAreDisjoint:
    """A skill is either baked into the instructions or loadable, never both."""

    def test_referenced_skill_is_baked_and_not_disclosable(self, skill_utils_module):
        version_details = {
            'instructions': 'Answer briefly. Follow ~shout in every reply.',
            'skills': [UPPERCASE, TERSE],
        }

        skill_utils_module.apply_runtime_skills(version_details)

        assert 'Write every letter in upper case.' in version_details['instructions']
        assert '~' not in version_details['instructions']
        assert [s['name'] for s in version_details['attached_skills']] == ['terse']

    def test_unreferenced_skills_stay_loadable(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE, TERSE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == 'Answer briefly.'
        assert [s['name'] for s in version_details['attached_skills']] == ['shout', 'terse']

    def test_skill_only_agent_still_gets_load_skill(self, skill_utils_module):
        """Empty instructions must not suppress the disclosable channel."""
        version_details = {'instructions': '', 'skills': [UPPERCASE, TERSE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == ''
        assert len(version_details['attached_skills']) == 2

    def test_blank_bodied_skill_is_consumed_but_not_offered(self, skill_utils_module):
        blank = {**UPPERCASE, 'instructions': '   '}
        version_details = {'instructions': 'Do ~shout now.', 'skills': [blank]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert '~' not in version_details['instructions']
        assert version_details['attached_skills'] == []


class TestRawMappingsDropped:
    """`skills` carries every body a second time and no runtime reads it."""

    def test_skills_key_is_removed(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert 'skills' not in version_details

    def test_disclosable_entries_are_fresh_objects(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['attached_skills'][0] is not UPPERCASE


class TestRepeatedApplication:
    """Shaping an already-shaped payload must not strip what it produced."""

    def test_second_call_keeps_attached_skills(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)
        first = list(version_details['attached_skills'])
        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['attached_skills'] == first

    def test_second_call_does_not_bake_twice(self, skill_utils_module):
        version_details = {
            'instructions': 'Follow ~shout in every reply.',
            'skills': [UPPERCASE],
        }

        skill_utils_module.apply_runtime_skills(version_details)
        baked = version_details['instructions']
        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == baked


class TestDeterminism:
    """The baked prompt is cached, so identical input must render identically."""

    def test_identical_inputs_render_identically(self, skill_utils_module):
        def shape():
            version_details = {
                'instructions': 'Use ~shout and ~terse.',
                'skills': [UPPERCASE, TERSE],
            }
            skill_utils_module.apply_runtime_skills(version_details)
            return version_details['instructions']

        assert shape() == shape()
