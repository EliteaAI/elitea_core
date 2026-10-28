"""Five schedule operations are MCP tools with unique names; every other schedule operation is not.

The tool name is derived by the shared registry from the method and the last two fixed parts of the path.
The real `utils/schedule_openapi.py` and the request models it reads are loaded; the handler modules
and the platform around them are lenient stubs, and the registry is a recorder.

Run via:
    python tests/run_tests.py unit/test_schedule_mcp_tools.py -v
"""
import ast
import importlib.util
import pathlib
import re
import sys
import types

import pytest
from fixtures import schedule_sqlite as sqlite_db

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'schedmcp_unit'
TAG = 'elitea_core/schedules'
BASE = '/api/v2/elitea_core/schedules/<string:mode>/<int:project_id>'
ITEM = BASE + '/<int:schedule_id>'
TOOLS = {
    ('get', BASE): 'get_elitea_core_schedules',
    ('post', BASE): 'post_elitea_core_schedules',
    ('put', ITEM): 'put_elitea_core_schedules',
    ('delete', ITEM): 'delete_elitea_core_schedules',
    ('post', f'{ITEM}/run'): 'post_schedules_run',
}
# The run token class of each write tool, the line `ENDPOINT_OPERATIONS` needs in utils/internal_obo.py
WRITE_OPERATIONS = {
    ('post', BASE): 'build',
    ('put', ITEM): 'build',
    ('delete', ITEM): 'build',
    ('post', f'{ITEM}/run'): 'run',
}
REAL_MODULES = (
    'utils/constants.py', 'models/pd/entity_schedule.py', 'models/pd/schedule_run.py',
    'models/pd/schedule_list.py', 'models/pd/generate_schedule_draft.py', 'utils/schedule_item_handlers.py',
    'utils/schedule_settings.py', 'utils/schedule_project_handlers.py', 'utils/schedule_openapi.py',
)
LENIENT_MODULES = (
    f'{PKG}.models.entity_schedule', f'{PKG}.models.enums.all', f'{PKG}.models.pd.index',
    *(f'{PKG}.utils.{name}' for name in (
        'schedule_access', 'schedule_claim', 'schedule_dispatch', 'schedule_notifications',
        'schedule_service', 'schedule_timing', 'authors', 'schedule_queries', 'trigger_governance',
    )),
)


def _load(monkeypatch, path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def openapi(monkeypatch):
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ('pylon', 'pylon.core', 'pylon.core.tools', 'tools', 'flask'):
        monkeypatch.setitem(sys.modules, name, sqlite_db.LenientModule(name))
    for name in ('', '.models', '.models.pd', '.models.enums', '.utils'):
        package = types.ModuleType(PKG + name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, PKG + name, package)
    for name in LENIENT_MODULES:
        monkeypatch.setitem(sys.modules, name, sqlite_db.LenientModule(name))
    service = sys.modules[f'{PKG}.utils.schedule_service']
    service.refusal, service.INVALID_INPUT = lambda code, text, status=400, **extra: ({}, status), 'invalid_input'
    for path in REAL_MODULES:
        module = _load(monkeypatch, path, f'{PKG}.{path.removesuffix(".py").replace("/", ".")}')
    return module


class Registry:
    """Records what `register_endpoint` receives; it takes no `operation_name`, like the installed shared plugin."""

    def __init__(self):
        self.calls = []

    def register_endpoint(
        self, plugin_name, path, method, name, description='', mcp_description='', summary='', tags=None,
        parameters=None, request_body=None, mcp_request_body=None, response_model=None, responses=None,
        security=None, deprecated=False, mcp_tool=False, available_to_users=False,
    ):
        self.calls.append({'path': path, 'method': method, 'mcp_tool': mcp_tool, 'mcp_request_body': mcp_request_body})


def _registered(openapi):
    registry = Registry()
    openapi.register_schedule_endpoints(registry, TAG)
    return registry.calls


def _derived_name(method, path):
    """The tool name the installed shared registry gives an endpoint: the method and the last two fixed path parts."""
    openapi_path = path.replace('<int:', '{').replace('<string:', '{').replace(">", '}')
    parts = [method] + [part for part in openapi_path.split('/') if part and not part.startswith('{')][-2:]
    return '_'.join(word.lower() for part in parts for word in part.split('_') if word)


def test_only_the_documented_schedule_operations_are_mcp_tools(openapi):
    tools = {(c['method'], c['path']) for c in _registered(openapi) if c['mcp_tool']}
    assert tools == set(TOOLS)


def test_every_schedule_operation_is_still_registered(openapi):
    assert len(_registered(openapi)) == len(openapi.OPENAPI_ENDPOINTS)


def test_derived_tool_names_are_unique_and_match_the_documented_list(openapi):
    derived = {(c['method'], c['path']): _derived_name(c['method'], c['path'])
               for c in _registered(openapi) if c['mcp_tool']}
    assert derived == TOOLS
    assert len(set(derived.values())) == len(derived)
    assert sorted(derived.values()) == sorted(openapi.MCP_TOOL_NAMES.values())


def test_registry_without_operation_name_takes_every_endpoint(openapi):
    assert all('operation_name' not in endpoint for endpoint in openapi.OPENAPI_ENDPOINTS)
    assert len(_registered(openapi)) == len(openapi.OPENAPI_ENDPOINTS)


def _edit_endpoint(openapi):
    return next(c for c in openapi.OPENAPI_ENDPOINTS if (c['method'], c['path']) == ('put', ITEM))


def test_the_edit_tool_schema_lists_the_enabled_field(openapi):
    properties = _registered_body(openapi, 'put', ITEM).model_json_schema()['properties']
    assert properties['enabled']['anyOf'][0]['type'] == 'boolean'
    assert {'name', 'cron'} <= set(properties)


def test_the_edit_text_says_enabled_pauses_or_resumes(openapi):
    description = _edit_endpoint(openapi)['description']
    assert re.search(r'`enabled` false to pause[^.]*true to resume', description)
    assert 'answers 400' not in description


def test_the_edit_answers_400_for_an_enabled_value_that_is_not_a_boolean(openapi):
    bad_input = _edit_endpoint(openapi)['responses']['400']['description']
    assert '`enabled` that is not true or false' in bad_input
    assert not hasattr(openapi, 'ENABLED_NOT_EDITABLE')


def _registered_body(openapi, method, path):
    return next(c for c in _registered(openapi) if (c['method'], c['path']) == (method, path))['mcp_request_body']


@pytest.mark.parametrize('method, path', [('post', BASE), ('put', ITEM)])
def test_the_save_tools_never_offer_the_risk_acknowledgement(openapi, method, path):
    endpoint = next(c for c in openapi.OPENAPI_ENDPOINTS if (c['method'], c['path']) == (method, path))
    assert 'risk_ack' in endpoint['request_body'].model_json_schema()['properties']
    tool_properties = _registered_body(openapi, method, path).model_json_schema()['properties']
    assert 'risk_ack' not in tool_properties
    assert {'name', 'cron'} <= set(tool_properties)


def test_the_read_only_tools_keep_their_own_schema(openapi):
    assert _registered_body(openapi, 'get', BASE) is None
    assert _registered_body(openapi, 'delete', ITEM) is None


def _endpoint_operations():
    """The `ENDPOINT_OPERATIONS` literal of utils/internal_obo.py, read without importing the Pylon runtime."""
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'internal_obo.py').read_text())
    assignment = next(
        n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], 'id', '') == 'ENDPOINT_OPERATIONS'
    )
    return ast.literal_eval(assignment.value)


def _operation_key(method, path):
    template = re.sub(r'<\w+:(\w+)>', r'{\1}', path)
    return method.upper(), template.removeprefix('/api/v2/')


def test_every_schedule_write_tool_has_a_run_token_operation(openapi):
    writes = {(c['method'], c['path']) for c in _registered(openapi) if c['mcp_tool'] and c['method'] != 'get'}
    assert writes == set(WRITE_OPERATIONS)
    operations = _endpoint_operations()
    assert {key: operations.get(_operation_key(*key)) for key in writes} == WRITE_OPERATIONS


@pytest.mark.parametrize('schedule_id', [0, -1, 2**31, 'abc'])
def test_the_risk_check_refuses_a_schedule_id_outside_the_database_range(openapi, schedule_id):
    models = sys.modules[f'{PKG}.models.pd.entity_schedule']
    body = {'entity_type': 'agent', 'application_id': 12, 'version_id': 34}
    assert models.ScheduleRiskCheck.model_validate({**body, 'schedule_id': 7}).schedule_id == 7
    with pytest.raises(ValueError):
        models.ScheduleRiskCheck.model_validate({**body, 'schedule_id': schedule_id})


@pytest.fixture
def executor(monkeypatch):
    """`McpApiToolExecutor` from the source of utils/mcp_service.py (the module needs the Pylon runtime), with the
    WSGI call replaced by a recorder, so a test sees whether the API was reached."""
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'mcp_service.py').read_text())
    functions = {'_path_segment', '_is_blank', '_same_path_value', '_consent_refusal', '_run_token_claims'}
    constants = {'MULTI_SEGMENT_PATH_PARAMS', '_SCHEDULES_API_ROOT'}
    classes = {'McpProjectScopeError', 'McpApiToolExecutor'}
    wanted = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in functions)
        or (isinstance(node, ast.Assign) and getattr(node.targets[0], 'id', '') in constants)
        or (isinstance(node, ast.ClassDef) and node.name in classes)
    ]
    assert len(wanted) == len(functions | constants | classes), 'mcp_service helpers moved or were renamed'
    namespace = {
        're': re, 'json': __import__('json'), 'traceback': __import__('traceback'),
        'sanitize_property_name': lambda name: name,
        'log': types.SimpleNamespace(**{level: (lambda *a, **k: None) for level in ('debug', 'warning', 'error')}),
    }
    exec(compile(ast.Module(body=wanted, type_ignores=[]), '<mcp_service>', 'exec'), namespace)  # noqa: S102
    cls = namespace['McpApiToolExecutor']
    cls.sent = []
    monkeypatch.setattr(cls, '_build_wsgi_environ', staticmethod(lambda *args: args))
    monkeypatch.setattr(cls, '_execute_wsgi_request', staticmethod(lambda environ: (cls.sent.append(environ), 200, '{}')[1:]))
    monkeypatch.setattr(cls, '_parse_response', staticmethod(lambda status, body: {'result': {}}))
    return cls


def _api_tool(method, path):
    parameters = [
        {'name': 'mode', 'in': 'path', 'schema': {'type': 'string', 'default': 'prompt_lib'}},
        {'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}},
    ]
    if '{schedule_id}' in path:
        parameters.append({'name': 'schedule_id', 'in': 'path', 'schema': {'type': 'integer'}})
    return {'method': method, 'path': path, 'label': 'schedule_tool', 'parameters': parameters}


SAVE_TOOLS = [
    ('POST', '/api/v2/elitea_core/schedules/{mode}/{project_id}', {'project_id': 1, 'name': 'Nightly'}),
    ('PUT', '/api/v2/elitea_core/schedules/{mode}/{project_id}/{schedule_id}',
     {'project_id': 1, 'schedule_id': 3, 'name': 'Nightly'}),
]
CONSENT_FIELDS = [
    {'risk_ack': {'fingerprint': 'abc', 'confirmed_name': 'Nightly'}},
    {'risk_ack': None},
    {'risk_ack': ''},
    {'approval_rule': 'run_without_asking'},
    {'approval_rule': 'run_without_asking', 'risk_ack': None},
]


@pytest.mark.parametrize('method, path, arguments', SAVE_TOOLS)
@pytest.mark.parametrize('consent', CONSENT_FIELDS)
def test_a_save_tool_call_with_a_consent_is_refused_before_the_api_is_called(executor, method, path, arguments, consent):
    result = executor.execute(_api_tool(method, path), {**arguments, **consent})
    assert list(result) == ['error'] and 'ask the user to confirm the risk review' in result['error']
    assert executor.sent == []


@pytest.mark.parametrize('method, path, arguments', SAVE_TOOLS)
def test_a_save_tool_call_with_pause_and_ask_reaches_the_api(executor, method, path, arguments):
    result = executor.execute(_api_tool(method, path), {**arguments, 'approval_rule': 'pause_and_ask'})
    assert result == {'result': {}}
    assert len(executor.sent) == 1
