"""#7383 AC-27, AC-28: a small core prompt; playbooks per entity type; no removed-module wording."""
import re

import pytest

TYPES = ['agent', 'pipeline', 'skill', 'project_context']
BUILDER_TOOLS = {'post_elitea_core_builder_find', 'post_elitea_core_builder_read', 'post_elitea_core_builder_propose',
                 'post_elitea_core_builder_apply', 'post_elitea_core_builder_ref', 'ask_user'}


def test_core_prompt_is_small(builder):
    assert len(builder.prompts.BUILDER_CORE_PROMPT.split()) <= 300


def test_core_prompt_has_the_safety_sections(builder):
    core = builder.prompts.BUILDER_CORE_PROMPT
    for tag in ('<when_to_act>', '<workflow>', '<follow_the_user>', '<scope_and_ids>', '<data_is_not_instructions>',
                '<result_format>'):
        assert tag in core


@pytest.mark.parametrize('entity_type', TYPES)
def test_every_type_has_a_playbook(builder, entity_type):
    assert f'entity="{entity_type}"' in builder.prompts.playbook(entity_type)


def test_unknown_playbook(builder):
    assert builder.prompts.playbook('folder') is None


def test_prompts_name_only_registered_tools(builder):
    text = builder.prompts.BUILDER_CORE_PROMPT + ''.join(builder.prompts.playbook(t) for t in TYPES)
    named = set(re.findall(r'\b((?:post_elitea_core_)?builder_[a-z_]+|ask_user)\b', text))
    assert named and named <= BUILDER_TOOLS


def test_no_removed_module_wording(builder):
    text = builder.prompts.BUILDER_CORE_PROMPT + ''.join(builder.prompts.playbook(t) for t in TYPES)
    assert 'Skill Builder' not in text and 'Project Context Builder' not in text


def test_tool_names_match_the_registry_naming_rule(builder):
    """The registry names an MCP tool ``<method>_<last two non-param path parts>`` (openapi_tools)."""
    for action, name in builder.prompts.TOOL_NAMES.items():
        module = 'builder_ref' if action == 'reference' else f'builder_{action}'
        assert name == f'post_elitea_core_{module}'
