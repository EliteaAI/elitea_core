"""EL-7011 US-17: the save-time risk scan (R1.1-R1.3, R2.1, R2.2) of utils/schedule_risk.py.

The module loads alone behind stand-ins for `tools`, the models and the owner expansion.
"""
import json
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'plugins.elitea_core'
VERSION_ID = 7


class ConfigurationExpandError(Exception):
    def __init__(self, errors):
        super().__init__(str(errors))
        self.errors = errors


def _error(field, error_type):
    return {'loc': (field,), 'msg': json.dumps({'error_type': error_type})}


class FakeSession:
    """Stands in for the SQLite stub session; the stub ApplicationVersion has no meta or instructions."""

    def __init__(self, version):
        self._version = version

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, _model, version_id):
        return self._version if version_id == VERSION_ID else None


def _tool(type_='jira', name='Jira', settings=None, meta=None):
    return types.SimpleNamespace(type=type_, name=name, settings=settings or {}, meta=meta or {})


def _version(agent_type='openai', meta=None, instructions=None, tools=()):
    return types.SimpleNamespace(
        id=VERSION_ID, application_id=3, agent_type=agent_type,
        meta=meta or {}, instructions=instructions, tools=list(tools),
    )


@pytest.fixture
def scan(isolated_sys_modules):
    state = types.SimpleNamespace(version=None, expand_errors={}, expanded={}, calls=[], secrets={})

    def expand_toolkit_settings(type_, settings, project_id, user_id):
        state.calls.append((type_, project_id, user_id))
        if type_ in state.expand_errors:
            raise ConfigurationExpandError(state.expand_errors[type_])
        return {**settings, **state.expanded.get(type_, {})}

    for name in (PKG, f'{PKG}.models', f'{PKG}.utils'):
        sys.modules[name] = types.ModuleType(name)
        sys.modules[name].__path__ = []
    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id: FakeSession(state.version))

    class VaultClient:
        def __init__(self, project_id):
            self.project_id = project_id

        def get_all_secrets(self):
            if isinstance(state.secrets, Exception):
                raise state.secrets
            return state.secrets

    tools.VaultClient = VaultClient
    sys.modules['tools'] = tools
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **k: None, warning=lambda *a, **k: None)
    sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    sys.modules['pylon.core.tools'] = pylon_tools
    models_all = types.ModuleType(f'{PKG}.models.all')
    models_all.ApplicationVersion = type('ApplicationVersion', (), {})
    sys.modules[models_all.__name__] = models_all
    app_tools = types.ModuleType(f'{PKG}.utils.application_tools')
    app_tools.ConfigurationExpandError = ConfigurationExpandError
    app_tools.expand_toolkit_settings = expand_toolkit_settings
    sys.modules[app_tools.__name__] = app_tools
    load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'pipeline_utils.py', f'{PKG}.utils.pipeline_utils')
    module = load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'schedule_risk.py', f'{PKG}.utils.schedule_risk')
    state.run = lambda version_id=VERSION_ID: module.scan_schedule_risks(12, version_id, 42)
    state.allowed = lambda: module.is_run_without_asking_allowed(12)
    return state


def _codes(warnings):
    return [w['code'] for w in warnings]


PIPELINE = """
nodes:
  - id: Review
    type: hitl
  - id: Show
    type: printer
  - id: Think
    type: llm
"""


class TestEL7011R1HumanSteps:
    def test_r1_1_agent_with_ask_user_gets_one_warning(self, scan):
        scan.version = _version(meta={'internal_tools': ['ask_user', 'other']})
        [warning] = scan.run()
        assert warning['code'] == 'human_input_step'

    def test_r1_2_pipeline_gets_one_warning_per_human_node(self, scan):
        scan.version = _version('pipeline', instructions=PIPELINE)
        warnings = scan.run()
        assert _codes(warnings) == ['human_input_step'] * 2
        assert [w['step'] for w in warnings] == ['Review', 'Show']

    def test_r1_1_version_without_human_steps_has_no_warning(self, scan):
        scan.version = _version(meta={'internal_tools': ['python']})
        assert scan.run() == []
        scan.version = _version('pipeline', instructions='nodes:\n  - id: A\n    type: llm\n  - id: B\n    type: tool\n')
        assert scan.run() == []

    @pytest.mark.parametrize('instructions', ['nodes: [unclosed', 'just text', None])
    def test_r1_3_unparsable_pipeline_gives_one_scan_incomplete(self, scan, instructions):
        scan.version = _version('pipeline', instructions=instructions)
        assert _codes(scan.run()) == ['scan_incomplete']

    def test_r1_3_missing_version_gives_one_scan_incomplete(self, scan):
        scan.version = _version()
        assert _codes(scan.run(version_id=999)) == ['scan_incomplete']

    def test_r1_3_non_credential_expansion_error_gives_one_scan_incomplete(self, scan):
        scan.version = _version(meta={'internal_tools': ['ask_user']}, tools=[_tool()])
        scan.expand_errors['jira'] = [{'loc': ('x',), 'msg': 'boom'}]
        assert _codes(scan.run()) == ['scan_incomplete']


class TestEL7011R2Credentials:
    def test_r2_1_one_warning_per_toolkit_for_each_error_type(self, scan):
        scan.version = _version(tools=[_tool('jira', 'Jira'), _tool('confluence', 'Confluence'), _tool('git', 'Git')])
        scan.expand_errors = {
            'jira': [_error('a', 'credential_not_found'), _error('b', 'credential_not_found')],
            'confluence': [_error('a', 'private_credential_not_found')],
            'git': [_error('a', 'credential_type_mismatch')],
        }
        warnings = scan.run()
        assert _codes(warnings) == ['missing_credential'] * 3
        assert [w['toolkit'] for w in warnings] == ['Jira', 'Confluence', 'Git']

    def test_r2_1_expands_for_the_owner_in_the_project(self, scan):
        scan.version = _version(tools=[_tool()])
        scan.run()
        assert scan.calls == [('jira', 12, 42)]

    def test_r2_1_clean_version_has_no_warning(self, scan):
        scan.version = _version(tools=[_tool()])
        assert scan.run() == []


class TestEL7011R2PersonalToken:
    @pytest.mark.parametrize('tool', [
        _tool('mcp', 'Remote'),
        _tool('mcp_github', 'Hub', meta={'mcp': True}),
        _tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'https://idp'}),
    ])
    def test_r2_2_mcp_and_inline_openapi_endpoint_warn(self, scan, tool):
        scan.version = _version(tools=[tool])
        [warning] = scan.run()
        assert warning == {'code': 'token_may_expire', 'message': warning['message'], 'toolkit': tool.name}

    @pytest.mark.parametrize('type_', ['sharepoint', 'openapi'])
    def test_r2_2_credential_with_discovery_endpoint_warns(self, scan, type_):
        scan.version = _version(tools=[_tool(type_, 'Docs', settings={f'{type_}_configuration': {'elitea_title': 'c'}})])
        scan.expanded[type_] = {f'{type_}_configuration': {'oauth_discovery_endpoint': 'https://idp'}}
        assert _codes(scan.run()) == ['token_may_expire']

    @pytest.mark.parametrize('type_', ['sharepoint', 'openapi'])
    def test_r2_2_app_only_credential_does_not_warn(self, scan, type_):
        scan.version = _version(tools=[_tool(type_, 'Docs', settings={f'{type_}_configuration': {'elitea_title': 'c'}})])
        scan.expanded[type_] = {f'{type_}_configuration': {'client_id': 'x'}}
        assert scan.run() == []

    def test_r2_2_toolkit_with_missing_credential_gets_only_the_credential_warning(self, scan):
        scan.version = _version(tools=[_tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'u'})])
        scan.expand_errors['openapi'] = [_error('a', 'credential_not_found')]
        assert _codes(scan.run()) == ['missing_credential']

    def test_r2_2_warning_text_holds_no_secret(self, scan):
        scan.version = _version(tools=[_tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'https://idp', 'token': 's3cret'})])
        assert 's3cret' not in json.dumps(scan.run())


@pytest.mark.parametrize(('secrets', 'allowed'), [
    ({'schedules_allow_run_without_asking': 'true'}, True),
    ({'schedules_allow_run_without_asking': ' YES '}, True),
    ({'schedules_allow_run_without_asking': '1'}, True),
    ({'schedules_allow_run_without_asking': 'no'}, False),
    ({}, False),
    (RuntimeError('vault down'), False),
])
def test_el7011_r3_2_run_without_asking_needs_the_project_secret(scan, secrets, allowed):
    """R3.2 (design D5): only a truthy project secret allows the rule; a store error refuses it."""
    scan.secrets = secrets
    assert scan.allowed() is allowed
