"""Guards that the schedule MCP group is confined to the session's project.

Re-broken by leaving the group out of the project clamp: a model-supplied project_id then
reaches the schedule handlers unchecked, and an omitted one is never filled from the session.
"""
import ast
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module

SCHEDULES = 'elitea_core/schedules'
SESSION_PROJECT = 2
FOREIGN_PROJECT = 7


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
        PLUGIN_ROOT / 'utils',
        'internal_tools',
        extra_stubs={
            'plugins.elitea_core.utils.mcp_config': mcp_config_mod,
            'plugins.elitea_core.utils.support_utils': support_utils_mod,
        },
    )


@pytest.fixture(scope='module')
def session_path_params(internal_tools):
    """The two path-parameter resolvers of McpService, run against the real suffix sets."""
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'mcp_service.py').read_text())
    service = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'McpService')
    methods = [
        n for n in service.body
        if isinstance(n, ast.FunctionDef)
        and n.name in ('__session_default_path_params', '__session_scoped_path_params')
    ]
    assert len(methods) == 2, 'session path-parameter resolvers moved or were renamed'
    namespace = {
        'MCP_CURRENT_PROJECT_SUFFIXES': internal_tools.MCP_CURRENT_PROJECT_SUFFIXES,
        'MCP_PROJECT_SCOPED_SUFFIXES': internal_tools.MCP_PROJECT_SCOPED_SUFFIXES,
    }
    # The resolvers are private methods of a class that needs the pylon runtime; running only their source keeps the test offline.
    exec(compile(ast.Module(body=methods, type_ignores=[]), '<resolvers>', 'exec'), namespace)  # noqa: S102

    def _resolve(scope_project_id):
        session = types.SimpleNamespace(
            entity_category=SCHEDULES, project_id=SESSION_PROJECT, scope_project_id=scope_project_id,
        )
        return (
            namespace['__session_default_path_params'](types.SimpleNamespace(session=session)),
            namespace['__session_scoped_path_params'](types.SimpleNamespace(session=session)),
        )
    return _resolve


def test_schedule_group_follows_the_active_project(internal_tools):
    assert SCHEDULES in internal_tools.MCP_CURRENT_PROJECT_SUFFIXES


def test_schedule_group_is_confined_to_the_scope_project(internal_tools):
    assert SCHEDULES in internal_tools.MCP_PROJECT_SCOPED_SUFFIXES


def test_omitted_project_defaults_to_the_session_project(session_path_params):
    defaults, _ = session_path_params(None)
    assert defaults == {'project_id': SESSION_PROJECT}


def test_scoped_session_pins_the_project_the_call_may_use(session_path_params):
    _, scoped = session_path_params(SESSION_PROJECT)
    assert scoped == {'project_id': SESSION_PROJECT}
    assert scoped['project_id'] != FOREIGN_PROJECT


def test_injected_schedule_url_targets_the_current_project_with_the_scope(internal_tools, monkeypatch):
    monkeypatch.setattr(internal_tools, 'run_token_applies', lambda: False)
    monkeypatch.setattr(internal_tools, '_get_user_token', lambda user_id: 'pat-token')
    monkeypatch.setattr(internal_tools, '_get_internal_base_url', lambda: 'http://localhost')
    monkeypatch.setattr(
        internal_tools.rpc_tools, 'RpcMixin',
        lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(
                timeout=lambda _s: types.SimpleNamespace(
                    admin_get_user_private_project=lambda user_id: types.SimpleNamespace(id=1)
                )
            )
        ),
        raising=False,
    )
    monkeypatch.setattr(
        internal_tools, 'MCP_ENDPOINT_CONFIGS', [{'suffix': SCHEDULES, 'name': 'Elitea Schedules'}]
    )
    tools = internal_tools.inject_mcp_toolkits(
        user_id=10,
        current_project_id=SESSION_PROJECT,
        internal_tools=[internal_tools.MCP_INTERNAL_TOOL_KEY],
        scope_project_id=SESSION_PROJECT,
    )
    assert [t['settings']['url'] for t in tools] == [
        f'http://localhost/app/{SESSION_PROJECT}/mcp/{SCHEDULES}?scope_project_id={SESSION_PROJECT}'
    ]
