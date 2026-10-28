"""Guards which builder toggles earn a hidden <runtime_context> block.

Re-broken by gating on a single tool key instead of the whole builder set.
"""
import pytest


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

    def test_skills_project_context_and_schedules_follow_the_active_project(self, internal_tools):
        assert internal_tools.MCP_CURRENT_PROJECT_SUFFIXES == {
            'elitea_core/skills',
            'elitea_core/project_context',
            'elitea_core/schedules',
        }


class TestScheduleBuilderRules:

    def test_rules_tell_the_model_to_use_the_profile_timezone_when_none_is_named(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['schedule_builder'])
        assert 'profile timezone' in addon
        assert 'names no timezone' in addon

    def test_rules_keep_a_timezone_the_user_names(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['schedule_builder'])
        assert 'keep it' in addon

    def test_rules_cover_the_delivery_target_confirmation_and_prompt(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['schedule_builder'])
        assert 'This chat' in addon
        assert 'delivery' in addon
        assert 'Confirm' in addon
        assert 'never invent a prompt' in addon

    def test_rules_name_the_context_values_they_depend_on(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['schedule_builder'])
        for tag in ('<conversation_uuid>', '<participant_id>', '<timezone>', '<project_id>'):
            assert tag in addon

    def test_no_schedule_rules_without_a_schedule_tool(self, internal_tools):
        for tools in (['skill_builder'], ['project_context_builder'], []):
            addon = internal_tools.get_mcp_entity_link_instructions(tools)
            assert 'profile timezone' not in addon
            assert 'schedule' not in addon.lower()

    def test_agent_builder_alone_gets_the_rules_with_its_schedule_tools(self, internal_tools):
        addon = internal_tools.get_mcp_entity_link_instructions(['internal_mcp'])
        assert 'Schedule rules:' in addon

    def test_schedules_join_the_project_scope_sentence_only_with_a_schedule_tool(self, internal_tools):
        with_tools = internal_tools.get_mcp_entity_link_instructions(['schedule_builder'])
        assert 'project context and schedules can only be read or changed in that project' in with_tools
        without = internal_tools.get_mcp_entity_link_instructions(['skill_builder'])
        assert 'schedules' not in without


class TestScheduleChatFacts:

    CHAT = '2f9d1d52-8a3f-4a10-9d0e-0b9d6a1c7e11'

    def facts(self, internal_tools, context, tools=('schedule_builder',), participant=12):
        return internal_tools.add_schedule_chat_facts(context, list(tools), self.CHAT, participant)

    def test_server_values_fill_the_chat_and_participant(self, internal_tools):
        facts = self.facts(internal_tools, {'user_id': 3})
        assert facts == {'user_id': 3, 'conversation_uuid': self.CHAT, 'participant_id': 12}

    def test_client_values_never_win(self, internal_tools):
        forged = {'conversation_uuid': 'x', 'participant_id': 99}
        facts = self.facts(internal_tools, forged)
        assert facts['conversation_uuid'] == self.CHAT
        assert facts['participant_id'] == 12
        assert 'participant_id' not in self.facts(internal_tools, forged, participant=None)

    def test_a_valid_client_timezone_is_kept(self, internal_tools):
        assert self.facts(internal_tools, {'timezone': 'Europe/Warsaw'})['timezone'] == 'Europe/Warsaw'

    @pytest.mark.parametrize('bad', ['Mars/Base', '../etc/passwd', '', 5, None, 'x' * 100])
    def test_an_invalid_client_timezone_is_dropped(self, internal_tools, bad):
        assert 'timezone' not in self.facts(internal_tools, {'timezone': bad})

    def test_agent_builder_alone_gets_the_facts(self, internal_tools):
        assert self.facts(internal_tools, {}, tools=('internal_mcp',))['conversation_uuid'] == self.CHAT

    def test_without_a_schedule_tool_the_client_cannot_add_the_facts(self, internal_tools):
        context = {'user_id': 3, 'conversation_uuid': 'x', 'participant_id': 1, 'timezone': 'UTC'}
        assert self.facts(internal_tools, context, tools=('skill_builder',)) == {'user_id': 3}
