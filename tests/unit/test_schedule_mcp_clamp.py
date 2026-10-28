"""Guards that the schedule MCP group is confined to the session's project and never takes a consent.

Re-broken by leaving the group out of the project clamp: a model-supplied project_id then
reaches the schedule handlers unchecked, and an omitted one is never filled from the session.
Also re-broken by letting a tool call carry `risk_ack`, which only a person sets in the UI.
"""
import ast
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

SCHEDULES = 'elitea_core/schedules'
SESSION_PROJECT = 2
FOREIGN_PROJECT = 7
SCHEDULE_TOOL_PATH = '/api/v2/elitea_core/schedules/prompt_lib/{project_id}'
SCHEDULES_API_ROOT = '/api/v2/elitea_core/schedules/'
PROJECT_PARAMS = [
    {'name': 'mode', 'in': 'path', 'schema': {'type': 'string', 'default': 'prompt_lib'}},
    {'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}},
]


class ScopeError(Exception):
    pass


@pytest.fixture(scope='module')
def internal_tools():
    """The clamp constants of utils/internal_tools.py, run from their source: the module needs the Pylon runtime."""
    names = ('MCP_ENDPOINT_CONFIGS', 'MCP_CURRENT_PROJECT_SUFFIXES', 'MCP_PROJECT_SCOPED_SUFFIXES')
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'internal_tools.py').read_text())
    constants = [n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], 'id', '') in names]
    assert len(constants) == len(names), 'clamp constants moved or were renamed'
    namespace = {}
    exec(compile(ast.Module(body=constants, type_ignores=[]), '<internal_tools>', 'exec'), namespace)  # noqa: S102
    return types.SimpleNamespace(**{name: namespace[name] for name in names})


@pytest.fixture(scope='module')
def mcp_service(internal_tools):
    """The schedule clamp of utils/mcp_service.py, run against the real suffix sets.

    The resolvers and the argument parser belong to classes that need the Pylon runtime; running only
    their source keeps the test offline.
    """
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'mcp_service.py').read_text())

    def pick(nodes, names):
        found = [n for n in nodes if isinstance(n, ast.FunctionDef) and n.name in names]
        assert len(found) == len(names), f'{names} moved or were renamed'
        return found

    def cls(name):
        return next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name).body

    resolvers = pick(cls('McpService'), ('__session_default_path_params', '__session_scoped_path_params'))
    parser = pick(cls('McpApiToolExecutor'), ('_parse_arguments',))
    parser[0].decorator_list = []
    helpers = pick(tree.body, ('_is_blank', '_same_path_value', '_consent_refusal'))
    namespace = {
        'MCP_CURRENT_PROJECT_SUFFIXES': internal_tools.MCP_CURRENT_PROJECT_SUFFIXES,
        'MCP_PROJECT_SCOPED_SUFFIXES': internal_tools.MCP_PROJECT_SCOPED_SUFFIXES,
        '_SCHEDULES_API_ROOT': SCHEDULES_API_ROOT,
        'McpProjectScopeError': ScopeError,
        'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        'sanitize_property_name': lambda name: name,
    }
    exec(compile(ast.Module(body=resolvers + parser + helpers, type_ignores=[]), '<mcp_service>', 'exec'), namespace)  # noqa: S102
    return namespace


@pytest.fixture(scope='module')
def session_path_params(mcp_service):
    def _resolve(scope_project_id, project_id=SESSION_PROJECT, category=SCHEDULES, path=SCHEDULE_TOOL_PATH):
        session = types.SimpleNamespace(
            entity_category=category, project_id=project_id, scope_project_id=scope_project_id,
        )
        return (
            mcp_service['__session_default_path_params'](types.SimpleNamespace(session=session)),
            mcp_service['__session_scoped_path_params'](types.SimpleNamespace(session=session), {'path': path}),
        )
    return _resolve


@pytest.fixture(scope='module')
def schedule_call(mcp_service, session_path_params):
    """The path parameters a schedule tool call ends up with, for a session of the given category."""
    def _call(arguments, category='elitea_core/chat', scope_project_id=None):
        defaults, scoped = session_path_params(scope_project_id, category=category)
        path_params, _, _ = mcp_service['_parse_arguments'](arguments, PROJECT_PARAMS, defaults, scoped)
        return path_params
    return _call


def test_schedule_group_follows_the_active_project(internal_tools):
    assert SCHEDULES in internal_tools.MCP_CURRENT_PROJECT_SUFFIXES


def test_schedule_group_is_an_injected_endpoint(internal_tools):
    assert SCHEDULES in {ep['suffix'] for ep in internal_tools.MCP_ENDPOINT_CONFIGS}


def test_omitted_project_defaults_to_the_session_project(session_path_params):
    defaults, _ = session_path_params(None)
    assert defaults == {'project_id': SESSION_PROJECT}


def test_scoped_session_pins_the_project_the_call_may_use(session_path_params):
    _, scoped = session_path_params(SESSION_PROJECT)
    assert scoped == {'project_id': SESSION_PROJECT}
    assert scoped['project_id'] != FOREIGN_PROJECT


def test_unscoped_session_pins_the_session_project(session_path_params):
    _, scoped = session_path_params(None)
    assert scoped == {'project_id': SESSION_PROJECT}


def test_session_without_a_project_is_refused(session_path_params):
    with pytest.raises(ScopeError):
        session_path_params(None, project_id=None)


@pytest.mark.parametrize('category', ['elitea_core/chat', '', None])
def test_schedule_tool_from_another_category_pins_the_session_project(session_path_params, category):
    _, scoped = session_path_params(None, category=category)
    assert scoped == {'project_id': SESSION_PROJECT}


def test_other_tool_from_another_category_is_not_pinned(session_path_params):
    other = '/api/v2/elitea_core/applications/{project_id}'
    _, scoped = session_path_params(None, category='elitea_core/chat', path=other)
    assert scoped == {}


@pytest.mark.parametrize('scope_project_id', [None, SESSION_PROJECT])
def test_chat_session_cannot_send_a_foreign_project_to_a_schedule_tool(schedule_call, scope_project_id):
    with pytest.raises(ScopeError, match=f'project {SESSION_PROJECT}'):
        schedule_call({'project_id': FOREIGN_PROJECT}, scope_project_id=scope_project_id)


def test_chat_session_schedule_call_uses_the_session_project(schedule_call):
    assert schedule_call({})['project_id'] == SESSION_PROJECT
    assert schedule_call({'project_id': str(SESSION_PROJECT)})['project_id'] == SESSION_PROJECT


ACKNOWLEDGEMENT = {'fingerprint': 'abc', 'confirmed_name': 'Daily report'}


def test_schedule_tool_call_with_a_risk_acknowledgement_is_refused(mcp_service):
    refusal = mcp_service['_consent_refusal'](SCHEDULE_TOOL_PATH, {'name': 'x', 'risk_ack': ACKNOWLEDGEMENT})
    assert 'confirm the risk review in the Elitea UI' in refusal


def test_schedule_tool_call_with_an_empty_risk_acknowledgement_goes_through(mcp_service):
    assert mcp_service['_consent_refusal'](SCHEDULE_TOOL_PATH, {'name': 'x', 'risk_ack': None}) is None
    assert mcp_service['_consent_refusal'](SCHEDULE_TOOL_PATH, {'name': 'x'}) is None


def test_other_tools_are_not_checked_for_a_risk_acknowledgement(mcp_service):
    other = '/api/v2/elitea_core/applications/{project_id}'
    assert mcp_service['_consent_refusal'](other, {'risk_ack': ACKNOWLEDGEMENT}) is None


@pytest.mark.parametrize('body', [
    {'name': 'x', 'approval_rule': 'run_without_asking'},
    {'approval_rule': 'run_without_asking', 'risk_ack': None},
])
def test_schedule_tool_call_cannot_set_run_without_asking(mcp_service, body):
    refusal = mcp_service['_consent_refusal'](SCHEDULE_TOOL_PATH, body)
    assert 'cannot set "Run without asking"' in refusal


def test_schedule_tool_call_may_save_pause_and_ask(mcp_service):
    assert mcp_service['_consent_refusal'](SCHEDULE_TOOL_PATH, {'name': 'x', 'approval_rule': 'pause_and_ask'}) is None
