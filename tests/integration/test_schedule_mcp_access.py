"""The schedule MCP tools stay inside the session's project and show the API's refusals as tool errors.

Drives the real `McpService.__handle_call_tool_request` and `McpApiToolExecutor` with the real project
clamp sets of `utils/internal_tools.py`, so the chain session -> clamp -> executor -> `CallToolResult`
is covered. The in-process API is a stand-in that answers with the refusal shapes of the Schedules
API (`{'code', 'error'}` plus the status); the rules themselves are tested with the handlers.
"""
import contextlib
import importlib.util
import json
import pathlib
import re
import sys
import types
from collections import namedtuple

import pytest

pytestmark = pytest.mark.integration

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEDULES_TAG = 'elitea_core/schedules'
SESSION_PROJECT = 2
FOREIGN_PROJECT = 3
NOT_FOUND = 'Schedule not found'
FORBIDDEN = 'You do not have permission for this action'
INVALID_CRON = 'Enter a cron with 5 parts: minute hour day month weekday'

User = namedtuple('User', 'id verbs admin')
READ = {'list', 'details'}
EDIT = READ | {'create', 'update', 'delete', 'run'}
OWNER = User(10, EDIT, False)
OTHER = User(11, EDIT, False)
ADMIN = User(99, EDIT, True)
VIEWER = User(12, READ, False)

PATH_PARAMS = [
    {'name': 'mode', 'in': 'path', 'schema': {'type': 'string', 'default': 'prompt_lib'}},
    {'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}},
]
ITEM_PARAM = {'name': 'schedule_id', 'in': 'path', 'schema': {'type': 'integer'}}
BASE_PATH = '/api/v2/elitea_core/schedules/{mode}/{project_id}'
TOOLS = {
    'list_schedules': ('get', BASE_PATH, PATH_PARAMS),
    'get_schedule': ('get', BASE_PATH + '/{schedule_id}', [*PATH_PARAMS, ITEM_PARAM]),
    'create_schedule': ('post', BASE_PATH, PATH_PARAMS),
    'update_schedule': ('put', BASE_PATH + '/{schedule_id}', [*PATH_PARAMS, ITEM_PARAM]),
    'delete_schedule': ('delete', BASE_PATH + '/{schedule_id}', [*PATH_PARAMS, ITEM_PARAM]),
    'run_schedule_now': ('post', BASE_PATH + '/{schedule_id}/run', [*PATH_PARAMS, ITEM_PARAM]),
}
API_TOOLS = [
    {'value': name, 'label': name, 'method': method, 'path': path, 'parameters': parameters}
    for name, (method, path, parameters) in TOOLS.items()
]
VALID_BODY = {'name': 'Daily report', 'cron': '0 9 * * 1-5', 'application_id': 12}
ROUTE = re.compile(r'^/api/v2/elitea_core/schedules/prompt_lib/(\d+)(?:/(\d+))?(/run)?$')
VERB = {
    ('GET', False, False): 'list', ('GET', True, False): 'details', ('POST', False, False): 'create',
    ('PUT', True, False): 'update', ('DELETE', True, False): 'delete', ('POST', True, True): 'run',
}


class ScheduleApi:
    """The Schedules API as the tool executor meets it: one caller, rows per project, JSON refusals."""

    def __init__(self):
        self.user = OWNER
        self.calls = []
        self.runs = []
        self.rows = {}
        self.add(1, SESSION_PROJECT, OWNER, 'Owner schedule')
        self.add(2, SESSION_PROJECT, OTHER, 'Other schedule')

    def add(self, schedule_id, project_id, owner, name, cron='0 9 * * *'):
        self.rows[schedule_id] = {'id': schedule_id, 'project_id': project_id, 'created_by': owner.id,
                                  'name': name, 'cron': cron}

    def visible(self, project_id):
        return [row for row in self.rows.values() if row['project_id'] == project_id
                and (self.user.admin or row['created_by'] == self.user.id)]

    def handle(self, environ):
        self.calls.append((environ['method'], environ['path']))
        project, item, run = ROUTE.match(environ['path']).groups()
        project_id = int(project)
        verb = VERB[environ['method'], item is not None, run is not None]
        if verb not in self.user.verbs:
            return 403, {'code': 'forbidden', 'error': FORBIDDEN}
        row = next((r for r in self.visible(project_id) if str(r['id']) == item), None)
        if item is not None and row is None:
            return 404, {'code': 'not_found', 'error': NOT_FOUND}
        if verb == 'list':
            return 200, {'rows': self.visible(project_id)}
        if verb == 'create':
            if len(environ['body'].get('cron', '').split()) != 5:
                return 400, {'code': 'invalid_input', 'error': INVALID_CRON, 'field': 'cron'}
            schedule_id = max(self.rows) + 1
            self.add(schedule_id, project_id, self.user, environ['body']['name'], environ['body']['cron'])
            return 201, self.rows[schedule_id]
        if verb == 'update':
            row.update(environ['body'])
        elif verb == 'delete':
            del self.rows[row['id']]
        elif verb == 'run':
            self.runs.append(row['id'])
        return 200, row


@pytest.fixture()
def mcp_service_module(monkeypatch):
    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.utils'):
        package = sys.modules.get(name) or types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)

    quiet = lambda *args, **kwargs: None
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(info=quiet, warning=quiet, error=quiet, debug=quiet, exception=quiet)
    monkeypatch.setitem(sys.modules, 'pylon', sys.modules.get('pylon') or types.ModuleType('pylon'))
    monkeypatch.setitem(sys.modules, 'pylon.core', sys.modules.get('pylon.core') or types.ModuleType('pylon.core'))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', pylon_tools)

    tools_pkg = types.ModuleType('tools')
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: contextlib.nullcontext(None))
    tools_pkg.auth = types.SimpleNamespace(current_user=lambda: {'id': OWNER.id})
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.config = types.SimpleNamespace()
    tools_pkg.VaultClient = object
    tools_pkg.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    tools_pkg.openapi_registry = types.SimpleNamespace(get_mcp_api_tools=lambda *args, **kwargs: API_TOOLS)
    tools_pkg.sanitize_property_name = lambda name: name
    monkeypatch.setitem(sys.modules, 'tools', tools_pkg)

    stubs = {
        'models.all': {'Application': type('Application', (), {}),
                       'ApplicationVersion': type('ApplicationVersion', (), {})},
        'utils.application_tools': {'toolkits_listing': lambda **kwargs: {'rows': []}},
        'utils.application_utils': {'list_applications_api': lambda *args, **kwargs: {}},
        'utils.toolkits_utils': {'get_toolkit_schemas': lambda *args, **kwargs: {}},
        'utils.exceptions': {'PoolSaturationError': type('PoolSaturationError', (Exception,), {}),
                             'InputGuardrailBlockedError': type('InputGuardrailBlockedError', (Exception,), {})},
        'utils.mcp_session': {'SseSession': type('SseSession', (), {})},
        'utils.mcp_versioning': {'INTERNAL_MCP_ENVIRON_KEY': 'elitea.internal_mcp_request'},
        'utils.mcp_config': {'is_mcp_exposure_enabled': lambda: True},
        'utils.support_utils': {'get_support_config': lambda: {'enabled': False, 'project_id': None}},
        'utils.pipeline_execution': {'TriggerType': types.SimpleNamespace(MCP='mcp')},
    }
    for name, attributes in stubs.items():
        module = types.ModuleType(f'plugins.elitea_core.{name}')
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, module.__name__, module)

    def load(name):
        spec = importlib.util.spec_from_file_location(
            f'plugins.elitea_core.utils.{name}', PLUGIN_ROOT / 'utils' / f'{name}.py')
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        return module

    load('internal_tools')
    return load('mcp_service')


@pytest.fixture()
def api(mcp_service_module):
    schedule_api = ScheduleApi()
    executor = mcp_service_module.McpApiToolExecutor
    executor._build_wsgi_environ = staticmethod(
        lambda method, path, query, body: {'method': method, 'path': path, 'query': query, 'body': body})

    def execute(environ):
        status, body = schedule_api.handle(environ)
        return status, json.dumps(body)

    executor._execute_wsgi_request = staticmethod(execute)
    return schedule_api


@pytest.fixture()
def call_tool(mcp_service_module, api):
    def _call(name, arguments, scope_project_id=SESSION_PROJECT):
        dispatched = []
        service = mcp_service_module.McpService.__new__(mcp_service_module.McpService)
        service.session = types.SimpleNamespace(
            project_id=SESSION_PROJECT, tags=[], entity_category=SCHEDULES_TAG,
            scope_project_id=scope_project_id, dispatch_message=dispatched.append,
        )
        request = types.SimpleNamespace(id='1', params=types.SimpleNamespace(
            name=mcp_service_module._build_agent_identifier(name), arguments=arguments))
        service._McpService__handle_call_tool_request(request)
        result = json.loads(dispatched[0])['result']
        return result['isError'], result['content'][0]['text']
    return _call


def _payload(text):
    return json.loads(text)


class TestProjectOfTheCall:

    @pytest.mark.parametrize('scope_project_id', [SESSION_PROJECT, None])
    def test_a_call_without_a_project_runs_in_the_session_project(self, api, call_tool, scope_project_id):
        failed, text = call_tool('list_schedules', {}, scope_project_id)
        assert not failed
        assert api.calls == [('GET', f'/api/v2/elitea_core/schedules/prompt_lib/{SESSION_PROJECT}')]
        assert [row['name'] for row in _payload(text)['rows']] == ['Owner schedule']

    def test_the_session_project_named_in_the_call_is_served(self, api, call_tool):
        failed, _ = call_tool('list_schedules', {'project_id': SESSION_PROJECT})
        assert not failed
        assert len(api.calls) == 1

    @pytest.mark.parametrize('name', sorted(TOOLS))
    def test_a_foreign_project_is_refused_before_the_api_runs(self, api, call_tool, name):
        failed, text = call_tool(name, {'project_id': FOREIGN_PROJECT, 'schedule_id': 1, **VALID_BODY})
        assert failed
        assert text.startswith('Access denied:')
        assert str(SESSION_PROJECT) in text and str(FOREIGN_PROJECT) in text
        assert api.calls == []


class TestOtherUsersSchedules:

    def test_an_owner_reads_and_renames_the_own_schedule(self, api, call_tool):
        failed, text = call_tool('get_schedule', {'schedule_id': 1})
        assert not failed and _payload(text)['name'] == 'Owner schedule'
        failed, text = call_tool('update_schedule', {'schedule_id': 1, 'name': 'Renamed'})
        assert not failed and _payload(text)['name'] == 'Renamed'
        assert api.rows[1]['name'] == 'Renamed'

    def test_an_admin_reads_the_schedule_of_another_user(self, api, call_tool):
        api.user = ADMIN
        failed, text = call_tool('get_schedule', {'schedule_id': 1})
        assert not failed and _payload(text)['name'] == 'Owner schedule'

    @pytest.mark.parametrize('name, arguments', [
        ('get_schedule', {}),
        ('update_schedule', {'name': 'Taken over'}),
        ('delete_schedule', {}),
        ('run_schedule_now', {'trigger': 'manual'}),
    ])
    def test_the_api_refusal_for_a_schedule_of_another_user_is_the_tool_error(self, api, call_tool, name, arguments):
        before = json.dumps(api.rows, sort_keys=True)
        failed, text = call_tool(name, {'schedule_id': 2, **arguments})
        assert failed
        assert text == NOT_FOUND
        assert [call[1] for call in api.calls] == [
            f'/api/v2/elitea_core/schedules/prompt_lib/{SESSION_PROJECT}/2' + ('/run' if name == 'run_schedule_now' else '')
        ], 'the API, not the tool layer, decides'
        assert json.dumps(api.rows, sort_keys=True) == before
        assert api.runs == []

    @pytest.mark.parametrize('name, arguments', [
        ('create_schedule', VALID_BODY),
        ('update_schedule', {'schedule_id': 1, 'name': 'Renamed'}),
        ('delete_schedule', {'schedule_id': 1}),
    ])
    def test_a_viewer_is_refused_every_write(self, api, call_tool, name, arguments):
        api.user = VIEWER
        api.add(3, SESSION_PROJECT, VIEWER, 'Viewer schedule')
        arguments = {**arguments, 'schedule_id': 3} if 'schedule_id' in arguments else arguments
        before = json.dumps(api.rows, sort_keys=True)
        failed, text = call_tool(name, arguments)
        assert failed
        assert text == FORBIDDEN
        assert json.dumps(api.rows, sort_keys=True) == before


class TestCreateBody:

    def test_a_valid_body_creates_the_schedule(self, api, call_tool):
        failed, text = call_tool('create_schedule', VALID_BODY)
        assert not failed
        created = _payload(text)
        assert created['name'] == 'Daily report' and created['project_id'] == SESSION_PROJECT
        failed, text = call_tool('list_schedules', {})
        assert created['id'] in [row['id'] for row in _payload(text)['rows']]

    def test_an_invalid_cron_saves_nothing_and_returns_the_api_message(self, api, call_tool):
        failed, text = call_tool('create_schedule', {**VALID_BODY, 'cron': 'every day'})
        assert failed
        assert text == INVALID_CRON
        assert [row['id'] for row in api.visible(SESSION_PROJECT)] == [1]
        failed, text = call_tool('list_schedules', {})
        assert [row['id'] for row in _payload(text)['rows']] == [1]
