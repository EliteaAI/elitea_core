"""Guards which builder toggles earn a hidden <runtime_context> block.

Re-broken by gating on a single tool key instead of the whole builder set.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


@pytest.fixture(scope='module')
def internal_tools():
    import tools

    for attr, value in (
        ('config', types.SimpleNamespace()),
        ('VaultClient', object),
        ('rpc_tools', types.SimpleNamespace(RpcMixin=object)),
        ('this', types.SimpleNamespace()),
        ('auth', types.SimpleNamespace()),
    ):
        if not hasattr(tools, attr):
            setattr(tools, attr, value)
    tools.config.APP_HOST = 'http://localhost'

    mcp_config_mod = types.ModuleType('mcp_config')
    mcp_config_mod.is_mcp_exposure_enabled = lambda: True

    support_utils_mod = types.ModuleType('support_utils')
    support_utils_mod.get_support_config = lambda: {'enabled': False, 'project_id': None}

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'internal_tools',
        extra_stubs={
            'plugins.elitea_core.utils.mcp_config': mcp_config_mod,
            'plugins.elitea_core.utils.support_utils': support_utils_mod,
        },
    )


class TestShouldInjectRuntimeContext:

    def test_skill_builder_alone_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['skill_builder'], False) is True

    def test_project_context_builder_alone_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['project_context_builder'], False) is True

    def test_internal_mcp_still_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['internal_mcp'], False) is True

    def test_pipeline_target_never_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['skill_builder'], True) is False
        assert internal_tools.should_inject_runtime_context(['internal_mcp'], True) is False

    def test_non_builder_tools_do_not_qualify(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['attachments', 'swarm'], False) is False

    def test_empty_and_none(self, internal_tools):
        assert internal_tools.should_inject_runtime_context([], False) is False
        assert internal_tools.should_inject_runtime_context(None, False) is False


class TestMcpEntityLinkInstructions:

    def test_addon_points_the_model_at_runtime_context(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['skill_builder'])
        assert '<runtime_context>' in addon
        assert '<project_id>' in addon
        assert '<user_id>' in addon

    def test_addon_present_for_project_context_builder(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['project_context_builder'])
        assert '<runtime_context>' in addon

    def test_no_addon_without_a_builder_tool(self, internal_tools):
        assert internal_tools.get_mcp_entity_link_instructions([]) == ''
        assert internal_tools.get_mcp_entity_link_instructions(['attachments']) == ''


class TestSkillRequestWithoutSkillBuilder:

    def test_agent_builder_alone_tells_the_model_skills_need_skill_builder(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'managing skills needs the Skill Builder tool' in addon
        assert 'is not enabled in this conversation' in addon
        assert 'do not create or change an agent, a pipeline or any other entity in its place' in addon

    def test_the_refusal_covers_managing_skills_not_any_mention_of_one(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'publish, import, export, attach or detach an ELITEA skill' in addon
        assert 'otherwise work with' not in addon

    def test_the_refusal_does_not_stop_an_agent_using_its_own_skills(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'This does not restrict using skills' in addon
        assert '<available_skills>' in addon

    def test_a_named_but_unattached_skill_is_not_presented_as_loadable(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'referenced by name' not in addon
        assert 'not listed there is not attached to this agent' in addon

    def test_creating_a_version_stays_allowed_without_claiming_its_skills(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'creating or editing agent and pipeline versions works normally' in addon
        assert 'does not carry over skills attached to another version' in addon
        assert 'can still copy the skills' not in addon

    def test_agent_builder_alone_does_not_offer_the_skills_tools(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'elitea_core/skills' not in addon

    def test_with_skill_builder_enabled_the_model_is_not_told_it_is_missing(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp', 'skill_builder'])
        assert 'is not enabled in this conversation' not in addon
        assert 'elitea_core/skills' in addon

    def test_without_agent_builder_there_is_no_agent_to_substitute(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['project_context_builder'])
        assert 'Skill Builder tool' not in addon


class TestCurrentProjectSuffixes:

    def test_skills_and_project_context_follow_the_active_project(self, internal_tools):
        assert internal_tools.MCP_CURRENT_PROJECT_SUFFIXES == {
            'elitea_core/skills',
            'elitea_core/project_context',
        }
