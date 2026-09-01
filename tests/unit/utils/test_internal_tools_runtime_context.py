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

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'internal_tools',
        extra_stubs={'plugins.elitea_core.utils.mcp_config': mcp_config_mod},
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


class TestCurrentProjectSuffixes:

    def test_skills_and_project_context_follow_the_active_project(self, internal_tools):
        assert internal_tools.MCP_CURRENT_PROJECT_SUFFIXES == {
            'elitea_core/skills',
            'elitea_core/project_context',
        }
