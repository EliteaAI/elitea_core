"""The Schedules module (key schedule_builder) injects the schedule toolkit only when it is switched on."""
import types

import pytest

SCHEDULES_SUFFIX = 'elitea_core/schedules'
CHAT_PROJECT = 4


@pytest.fixture
def inject(internal_tools, monkeypatch):
    private_project = types.SimpleNamespace(id=1)
    rpc = types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(admin_get_user_private_project=lambda user_id: private_project)
    )
    monkeypatch.setattr(internal_tools.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(rpc=rpc), raising=False)
    monkeypatch.setattr(internal_tools, 'run_token_applies', lambda: False)
    monkeypatch.setattr(internal_tools, '_get_user_token', lambda user_id: 'token')

    def _inject(internal_tools_list, **kwargs):
        return internal_tools.inject_mcp_toolkits(
            user_id=3, current_project_id=CHAT_PROJECT, internal_tools=internal_tools_list, existing_tools=[], **kwargs
        )
    return _inject


def test_module_key_is_a_builder_key(internal_tools):
    assert internal_tools.MCP_SCHEDULE_BUILDER_INTERNAL_TOOL_KEY == 'schedule_builder'
    assert 'schedule_builder' in internal_tools.MCP_BUILDER_TOOL_KEYS


def test_module_on_injects_only_the_schedules_toolkit(inject):
    tools = inject(['schedule_builder'])
    assert [t['name'] for t in tools] == ['Elitea Schedules']
    assert tools[0]['settings']['url'].endswith(f'/app/{CHAT_PROJECT}/mcp/{SCHEDULES_SUFFIX}')


def test_module_off_injects_no_schedules_toolkit(inject):
    assert inject([]) == []
    assert inject(['skill_builder'])
    assert all('schedules' not in t['settings']['url'] for t in inject(['skill_builder', 'project_context_builder']))


def test_module_on_with_agent_builder_injects_schedules_once(inject):
    tools = inject(['internal_mcp', 'schedule_builder'])
    urls = [t['settings']['url'] for t in tools]
    assert sum(url.endswith(SCHEDULES_SUFFIX) for url in urls) == 1


def test_runtime_context_is_injected_when_module_is_on(internal_tools):
    assert internal_tools.should_inject_runtime_context(['schedule_builder'], False)


def test_module_on_carries_the_project_scope_in_the_url(inject, internal_tools):
    tools = inject(['schedule_builder'], scope_project_id=CHAT_PROJECT)
    assert [t['settings']['url'] for t in tools] == [
        f'{internal_tools._get_internal_base_url()}/app/{CHAT_PROJECT}/mcp/{SCHEDULES_SUFFIX}?scope_project_id={CHAT_PROJECT}'
    ]


def test_module_on_without_a_private_project_injects_nothing(inject, internal_tools, monkeypatch):
    rpc = types.SimpleNamespace(timeout=lambda seconds: types.SimpleNamespace(admin_get_user_private_project=lambda user_id: None))
    monkeypatch.setattr(internal_tools.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(rpc=rpc), raising=False)
    assert inject(['schedule_builder'], scope_project_id=CHAT_PROJECT) == []


def test_module_on_without_a_token_injects_nothing(inject, internal_tools, monkeypatch):
    monkeypatch.setattr(internal_tools, '_get_user_token', lambda user_id: None)
    assert inject(['schedule_builder'], scope_project_id=CHAT_PROJECT) == []
