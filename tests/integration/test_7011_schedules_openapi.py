"""EL-7011 BUG-11: the Swagger entries of the Schedules API (R1.1-R1.6, R2.3, R2.4).

Runs the real `register_api_class` and `OpenAPIRegistry` of the `shared` plugin over the real
`schedule.py`, `schedules.py`, `generate_schedule_draft.py` and `schedule_preferences.py`, plus the
real `Module._register_openapi`. The request models are real; every helper module the handlers call
is a lenient stub, because no handler runs here.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_schedules_openapi.py
"""
import importlib.util
import pathlib
import re
import sys
import types
from typing import get_args
from unittest.mock import MagicMock

import pytest
from fixtures import schedule_sqlite as sqlite_db

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
SHARED_TOOLS = pathlib.Path('shared') / 'tools' / 'openapi_tools.py'
# The shared plugin sits next to elitea_core in a pylon, or in the centry checkout of the workspace
OPENAPI_TOOLS = next(
    (path for path in (
        PLUGIN_ROOT.parent / SHARED_TOOLS,
        *(parent / 'centry' / 'pylon_main' / 'plugins' / SHARED_TOOLS for parent in PLUGIN_ROOT.parents),
    ) if path.exists()),
    None,
)

pytestmark = pytest.mark.skipif(OPENAPI_TOOLS is None, reason='the shared plugin is not checked out')

PKG = 'schedopenapi_7011'
TAG = 'elitea_core/schedules'
API_ROOT = '/api/v2/elitea_core'
SCHEDULE = f'{API_ROOT}/schedule/{{mode}}/{{project_id}}'
LIST = f'{API_ROOT}/schedules/{{mode}}/{{project_id}}'
API_MODULES = ('schedule', 'schedules', 'generate_schedule_draft', 'schedule_preferences')
REAL_MODULES = (
    'utils/constants.py', 'models/pd/entity_schedule.py', 'models/pd/schedule_run.py',
    'models/pd/schedule_list.py', 'models/pd/generate_schedule_draft.py',
)
LENIENT_MODULES = (
    'arbiter', f'{PKG}.models.entity_schedule', f'{PKG}.models.enums.all', f'{PKG}.models.pd.index',
    f'{PKG}.models.pd.sio', f'{PKG}.scripts.tool_icons', f'{PKG}.sio.asr',
    *(f'{PKG}.utils.{name}' for name in (
        'schedule_access', 'schedule_claim', 'schedule_dispatch', 'schedule_notifications',
        'schedule_service', 'schedule_timing', 'authors', 'schedule_queries', 'draft_llm_utils',
        'exceptions', 'predict_utils', 'schedule_draft_utils', 'service_prompt_utils', 'utils',
        'evaluation_run_utils', 'run_id', 'scheduler_bindings', 'sio_utils', 'prompt_eliminate_utils',
    )),
)
OTHER_TAGS = {
    'elitea_core/applications', 'elitea_core/analytics', 'elitea_core/authors', 'elitea_core/chat',
    'elitea_core/discovery', 'elitea_core/mcp', 'elitea_core/project_context', 'elitea_core/skills',
    'elitea_core/runtime', 'elitea_core/toolkits',
}
OPERATIONS = {
    ('post', SCHEDULE),
    ('get', f'{SCHEDULE}/{{schedule_id}}'),
    ('put', f'{SCHEDULE}/{{schedule_id}}'),
    ('delete', f'{SCHEDULE}/{{schedule_id}}'),
    ('post', f'{SCHEDULE}/{{schedule_id}}/run'),
    ('post', f'{SCHEDULE}/{{schedule_id}}/owner'),
    ('get', f'{SCHEDULE}/{{schedule_id}}/runs'),
    ('get', LIST),
    ('get', f'{LIST}/export'),
    ('post', f'{LIST}/import'),
    ('post', f'{API_ROOT}/generate_schedule_draft/{{mode}}/{{project_id}}'),
    ('get', f'{API_ROOT}/schedule_preferences/{{mode}}/{{project_id}}'),
    ('put', f'{API_ROOT}/schedule_preferences/{{mode}}/{{project_id}}'),
}


def _with_modes(reverse):
    """`api_tools.with_modes` builds a set, so its order changes per restart; `reverse` picks one of two."""
    def with_modes(url_params):
        params = set(url_params)
        params.update(f'<string:mode>/{i}' for i in url_params if not i.startswith('<string:mode>'))
        return sorted(params, reverse=reverse)
    return with_modes


def _load(monkeypatch, path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _install(monkeypatch, openapi, reverse=False):
    """Stub the platform around the plugin; returns the loaded `module.py`."""
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = MagicMock()
    pylon_tools.module = types.SimpleNamespace(ModuleModel=object)
    for name, module in (('pylon', types.ModuleType('pylon')), ('pylon.core', types.ModuleType('pylon.core')),
                         ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)

    tools = sqlite_db.LenientModule('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}), APIBase=type('APIBase', (), {}),
        with_modes=_with_modes(reverse), endpoint_metrics=lambda func: func,
    )
    tools.auth = MagicMock()  # module.py decorates its Socket.IO handlers with other auth decorators
    tools.auth.decorators.check_api = lambda spec: lambda func: func
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.register_openapi = openapi.register_openapi
    tools.openapi_registry = openapi.openapi_registry
    monkeypatch.setitem(sys.modules, 'tools', tools)
    monkeypatch.setitem(sys.modules, 'flask', sqlite_db.LenientModule('flask'))

    for name in ('', '.api', '.models', '.models.pd', '.models.enums', '.utils', '.scripts', '.sio'):
        package = types.ModuleType(PKG + name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, PKG + name, package)
    v2 = types.ModuleType(f'{PKG}.api.v2')
    v2.__path__ = []  # the folder scan finds nothing; the test registers the schedule modules itself
    monkeypatch.setitem(sys.modules, v2.__name__, v2)
    for name in LENIENT_MODULES:
        monkeypatch.setitem(sys.modules, name, sqlite_db.LenientModule(name))
    for path in REAL_MODULES:
        _load(monkeypatch, PLUGIN_ROOT / path, f'{PKG}.{path.removesuffix(".py").replace("/", ".")}')
    for name in API_MODULES:
        setattr(v2, name, _load(monkeypatch, PLUGIN_ROOT / 'api' / 'v2' / f'{name}.py', f'{v2.__name__}.{name}'))
    return _load(monkeypatch, PLUGIN_ROOT / 'module.py', f'{PKG}.module')


def _register(monkeypatch, reverse=False):
    """The registry after plugin start: the folder scan of the schedule modules, then `_register_openapi`."""
    openapi_tools = _load(monkeypatch, OPENAPI_TOOLS, 'openapi_tools_7011')
    plugin = _install(monkeypatch, openapi_tools, reverse)
    registry = openapi_tools.openapi_registry
    plugin.Module._register_openapi(types.SimpleNamespace(descriptor=types.SimpleNamespace(metadata={})))
    for name in API_MODULES:
        api = getattr(sys.modules[f'{PKG}.api.v2'], name).API
        openapi_tools.register_api_class(api, 'elitea_core', f'{API_ROOT}/{name}', registry)
    return registry


@pytest.fixture
def registry(monkeypatch):
    return _register(monkeypatch)


@pytest.fixture
def spec(registry):
    """The spec a regular user sees on /shared/swagger (only `available_to_users` operations)."""
    return registry.get_plugin_spec('elitea_core', full=False)


def _operations(spec):
    return {(method, path): operation for path, methods in spec['paths'].items()
            for method, operation in methods.items() if TAG in operation['tags']}


def _query(spec, method, path):
    return {p['name']: p for p in _operations(spec)[(method, path)]['parameters'] if p['in'] == 'query'}


def _route_shape(registry):
    return {(e['method'], e['path']): sorted(p['name'] for p in e['parameters'] if p['in'] == 'path')
            for e in registry._endpoints['elitea_core']}


def _body_schemas(spec):
    """Every request body schema: the referenced models with their nested ones, and the inline bodies."""
    schemas = dict(spec['components']['schemas'])
    schemas.pop('SchedulePreferenceUpdate')  # schedule_preferences.py is outside BUG-11
    for (method, path), operation in _operations(spec).items():
        for content in operation.get('requestBody', {}).get('content', {}).values():
            if '$ref' not in content['schema']:
                schemas[f'{method} {path}'] = content['schema']
    return schemas


# R1.1: each operation under its own path and method, with the path parameters of that path
def test_r1_1_el7011_every_schedules_operation_has_its_own_path(spec):
    operations = _operations(spec)
    assert set(operations) == OPERATIONS
    for (method, path), operation in operations.items():
        path_params = {p['name'] for p in operation['parameters'] if p['in'] == 'path'}
        assert path_params == set(re.findall(r'{(\w+)}', path)), (method, path)


# R1.6: a restart reorders `with_modes`; the published paths stay the same
def test_r1_6_el7011_paths_do_not_depend_on_url_params_order(monkeypatch):
    first = _route_shape(_register(monkeypatch))
    second = _route_shape(_register(monkeypatch, reverse=True))
    assert first == second


# R1.2: the list and the run records publish their query options with type, enum and default
def test_r1_2_el7011_list_and_runs_publish_query_enums(spec):
    filters = sys.modules[f'{PKG}.models.pd.schedule_list'].ScheduleListFilters.model_fields
    run_statuses = list(get_args(sys.modules[f'{PKG}.api.v2.schedule'].RunStatus))
    listing = _query(spec, 'get', LIST)
    assert set(listing) == {'entity_type', 'status', 'enabled', 'owner_ids', 'entity_id', 'search',
                            'limit', 'offset', 'sort_by', 'sort_order'}
    for name in ('sort_by', 'sort_order'):  # the published options are the ones the model accepts
        assert listing[name]['schema']['enum'] == list(get_args(filters[name].annotation))
        assert listing[name]['schema']['default'] == filters[name].default
    assert listing['entity_type']['schema']['enum'] == ['agent', 'pipeline']
    assert listing['status']['schema']['enum'] == run_statuses
    assert listing['limit']['schema'] == {'type': 'integer', 'default': 20, 'minimum': 1, 'maximum': 100}
    assert listing['enabled']['schema']['type'] == 'boolean'

    runs = _query(spec, 'get', f'{SCHEDULE}/{{schedule_id}}/runs')
    assert set(runs) == {'limit', 'offset', 'status'}
    assert runs['status']['schema']['enum'] == run_statuses
    assert runs['limit']['schema']['default'] == 20
    assert runs['offset']['schema']['default'] == 0

    assert _query(spec, 'get', f'{LIST}/export')['ids']['schema']['type'] == 'string'
    for params in (listing, runs):
        assert all(p['description'] for p in params.values())


# R1.4: the codes the handlers really answer, not the generic default list
@pytest.mark.parametrize('method, path, code', [
    ('post', SCHEDULE, '201'),
    ('post', f'{SCHEDULE}/{{schedule_id}}/run', '202'),
    ('delete', f'{SCHEDULE}/{{schedule_id}}', '204'),
    ('post', f'{SCHEDULE}/{{schedule_id}}/owner', '503'),
    ('post', f'{API_ROOT}/generate_schedule_draft/{{mode}}/{{project_id}}', '502'),
])
def test_r1_4_el7011_operations_publish_real_codes(spec, method, path, code):
    responses = _operations(spec)[(method, path)]['responses']
    assert code in responses
    assert all(r['description'] for r in responses.values())


def test_r1_4_el7011_create_and_delete_do_not_publish_200(spec):
    operations = _operations(spec)
    assert '200' not in operations[('post', SCHEDULE)]['responses']
    assert '200' not in operations[('delete', f'{SCHEDULE}/{{schedule_id}}')]['responses']


# R1.5: the tag has its description
def test_r1_5_el7011_tag_has_description(spec):
    tags = {tag['name']: tag for tag in spec['tags']}
    assert tags[TAG].get('description')


# R1.3: every request field has a description; the JSON bodies show an example
def test_r1_3_el7011_body_fields_have_descriptions(spec):
    schemas = _body_schemas(spec)
    assert {'ScheduleCreate', 'ScheduleUpdate', 'ScheduleRunRequest', 'GenerateScheduleDraftRequest'} <= set(schemas)
    for name, schema in schemas.items():
        for field, prop in schema.get('properties', {}).items():
            assert prop.get('description'), f'{name}.{field}'


@pytest.mark.parametrize('method, path', [
    ('post', SCHEDULE),
    ('put', f'{SCHEDULE}/{{schedule_id}}'),
    ('post', f'{SCHEDULE}/{{schedule_id}}/run'),
    ('post', f'{API_ROOT}/generate_schedule_draft/{{mode}}/{{project_id}}'),
])
def test_r1_3_el7011_json_bodies_show_an_example(spec, method, path):
    assert _operations(spec)[(method, path)]['requestBody']['content']['application/json']['examples']


# R2.3: no Schedules operation becomes an MCP tool
def test_r2_3_el7011_schedules_operations_stay_out_of_mcp(registry):
    assert registry.get_mcp_api_tools(plugins=['elitea_core'], filter_tags=[TAG]) == []


# R2.4: the other tags stay; the plugin registration adds Schedules paths only
def test_r2_4_el7011_other_tags_and_paths_are_unchanged(registry):
    tags = {tag['name'] for tag in registry._plugins['elitea_core']['tags']}
    assert tags == OTHER_TAGS | {TAG}
    for endpoint in registry._endpoints['elitea_core']:
        assert endpoint['path'].startswith(tuple(f'{API_ROOT}/{name}/' for name in API_MODULES))
        assert endpoint['tags'] == [TAG]


def test_r2_4_el7011_registration_without_openapi_registry_does_not_fail(monkeypatch):
    """In preload mode the shared plugin gives a registry with `register_plugin` only."""
    openapi_tools = _load(monkeypatch, OPENAPI_TOOLS, 'openapi_tools_7011')
    preload = types.SimpleNamespace(register_openapi=openapi_tools.register_openapi,
                                    openapi_registry=types.SimpleNamespace(register_plugin=lambda **kwargs: None))
    plugin = _install(monkeypatch, preload)
    plugin.Module._register_openapi(types.SimpleNamespace(descriptor=types.SimpleNamespace(metadata={})))
