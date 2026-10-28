"""The Schedules module (key schedule_builder) injects the schedule toolkit only when it is switched on."""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module

SCHEDULES_SUFFIX = 'elitea_core/schedules'
CHAT_PROJECT = 4


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


@pytest.fixture
def inject(internal_tools, monkeypatch):
    private_project = types.SimpleNamespace(id=1)
    rpc = types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(admin_get_user_private_project=lambda user_id: private_project)
    )
    monkeypatch.setattr(internal_tools.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(rpc=rpc), raising=False)
    monkeypatch.setattr(internal_tools, 'run_token_applies', lambda: False)
    monkeypatch.setattr(internal_tools, '_get_user_token', lambda user_id: 'token')

    def _inject(internal_tools_list):
        return internal_tools.inject_mcp_toolkits(
            user_id=3, current_project_id=CHAT_PROJECT, internal_tools=internal_tools_list, existing_tools=[]
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
