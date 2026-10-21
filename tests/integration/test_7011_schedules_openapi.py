"""EL-7011 BUG-11, owner CR 2026-10-21: the Swagger entries of the one Schedules resource (R1.1-R1.6, R2.3, R2.4).

Runs the real `register_api_class` and `OpenAPIRegistry` of the `shared` plugin over the real
`api/v2/schedules.py`, its three handler modules and `utils/schedule_openapi.py`, plus the real
`Module._register_openapi`. The request models are real; every helper module the handlers call
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
BASE = f'{API_ROOT}/schedules/{{mode}}/{{project_id}}'
ITEM = f'{BASE}/{{schedule_id}}'
API_MODULES = ('schedules',)
REAL_MODULES = (
    'utils/constants.py', 'models/pd/entity_schedule.py', 'models/pd/schedule_run.py',
    'models/pd/schedule_list.py', 'models/pd/generate_schedule_draft.py', 'utils/schedule_item_handlers.py',
    'utils/schedule_settings.py', 'utils/schedule_project_handlers.py', 'utils/schedule_draft_handler.py',
    'utils/schedule_openapi.py',
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
# The 15 rows of the owner's route table (CR 2026-10-21), plus GET and PUT settings (CR "Run without asking")
# the run detail of the Test schedule panel (CR 2026-10-21, test panel log) and the dry-run risk check
# (CR risk review)
OPERATIONS = {
    ('get', BASE),
    ('post', BASE),
    ('get', ITEM),
    ('put', ITEM),
    ('delete', ITEM),
    ('post', f'{ITEM}/pause'),
    ('post', f'{ITEM}/resume'),
    ('post', f'{ITEM}/run'),
    ('get', f'{ITEM}/runs'),
    ('get', f'{ITEM}/runs/{{run_id}}'),
    ('post', f'{ITEM}/owner'),
    ('get', f'{BASE}/export'),
    ('post', f'{BASE}/import'),
    ('post', f'{BASE}/draft'),
    ('get', f'{BASE}/preferences'),
    ('put', f'{BASE}/preferences'),
    ('get', f'{BASE}/settings'),
    ('put', f'{BASE}/settings'),
    ('post', f'{BASE}/check'),
}
NO_JSON_SUCCESS = {('delete', ITEM), ('get', f'{BASE}/export')}  # 204 without a body, a YAML file


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


def _refusal(code, text, status=400, **extra):
    """The real `schedule_service.refusal`: the plugin load breaks when Swagger reads it as a dict."""
    return {'code': code, 'error': text, **extra}, status


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
    # The handler constants are built at import time with the real refusal shape: (body, status)
    service = sys.modules[f'{PKG}.utils.schedule_service']
    service.refusal, service.INVALID_INPUT = _refusal, 'invalid_input'
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
    for (method, path), operation in _operations(spec).items():
        for content in operation.get('requestBody', {}).get('content', {}).values():
            if '$ref' not in content['schema']:
                schemas[f'{method} {path}'] = content['schema']
    return schemas


# R1.1: each operation under its own path and method, with the path parameters of that path
def test_r1_1_el7011_every_schedules_operation_has_its_own_path(spec):
    operations = _operations(spec)
    assert set(operations) == OPERATIONS
    assert len(operations) == 19
    for (method, path), operation in operations.items():
        path_params = {p['name'] for p in operation['parameters'] if p['in'] == 'path'}
        assert path_params == set(re.findall(r'{(\w+)}', path)), (method, path)


def _routes():
    return sys.modules[f'{PKG}.api.v2.schedules']


def _openapi_path(tail):
    return re.sub(r'<\w+:(\w+)>', r'{\1}', f'{BASE}/{tail}'.rstrip('/'))


# CR 2026-10-21: the live route table serves exactly the documented operations, all through `schedules`
def test_cr_el7011_the_schedules_module_serves_every_documented_operation(registry):
    routes = _routes()
    assert {(method, _openapi_path(tail)) for method, tail in routes.ROUTES} == OPERATIONS
    served = {_openapi_path(url.removeprefix('<string:mode>/').removeprefix('<int:project_id>').lstrip('/'))
              for url in routes.API.url_params}
    assert served == {path for _, path in OPERATIONS}


# CR 2026-10-21: a docstring per handler, for the people who read the source
def test_cr_el7011_every_handler_has_a_docstring(registry):
    routes = _routes()
    for name in routes.ROUTES.values():
        assert (getattr(routes.PromptLibAPI, name).__doc__ or '').strip(), name


# CR 2026-10-21: a short summary and a description for humans: what, who may call, the main errors
def test_cr_el7011_every_operation_is_described_for_humans(spec):
    for key, operation in _operations(spec).items():
        assert 0 < len(operation['name']) <= 40, key
        description = operation['description']
        assert 'Who may call:' in description and 'Errors:' in description, key


# Owner request 2026-10-21 (US-55): Swagger shows a short summary next to each path
def test_cr_el7011_every_operation_has_a_summary(spec):
    for key, operation in _operations(spec).items():
        assert 0 < len(operation.get('summary', '').strip()) <= 40, key


# CR 2026-10-21 (US-55): every JSON success answer shows an example
def test_cr_el7011_success_answers_show_an_example(spec):
    for key, operation in _operations(spec).items():
        successes = {code: r for code, r in operation['responses'].items() if code.startswith('2')}
        assert successes, key
        if key in NO_JSON_SUCCESS:
            continue
        for code, response in successes.items():
            assert response['content']['application/json']['example'], (key, code)


# CR 2026-10-21: PUT edits fields only; pause and resume take no body
def test_cr_el7011_put_has_no_enabled_field_and_pause_resume_have_no_body(spec):
    operations = _operations(spec)
    put_body = operations[('put', ITEM)]['requestBody']['content']['application/json']['schema']['$ref']
    assert 'enabled' not in spec['components']['schemas'][put_body.rsplit('/', 1)[-1]]['properties']
    assert '400' in operations[('put', ITEM)]['responses']
    bad_input = operations[('put', ITEM)]['responses']['400']['content']['application/json']['example']
    assert (bad_input['code'], bad_input['field']) == ('invalid_input', 'enabled')  # BUG-29: no tuple spread
    for action in ('pause', 'resume'):
        assert 'requestBody' not in operations[('post', f'{ITEM}/{action}')]


# Owner CR 2026-10-21 "Run without asking": any member reads the setting, only a schedules admin saves it
def test_cr_el7011_settings_publish_the_one_key_and_the_admin_rule(spec):
    operations = _operations(spec)
    body = operations[('put', f'{BASE}/settings')]['requestBody']['content']['application/json']['schema']['$ref']
    assert set(spec['components']['schemas'][body.rsplit('/', 1)[-1]]['properties']) == {'allow_run_without_asking'}
    assert '`schedules.admin`' in operations[('put', f'{BASE}/settings')]['description']
    assert '`schedules.list`' in operations[('get', f'{BASE}/settings')]['description']
    assert {'400', '403'} <= set(operations[('put', f'{BASE}/settings')]['responses'])


# Owner CR 2026-10-21 (test panel log): the run detail shows its summary, its events and the polling rule
def test_cr_el7011_the_run_detail_documents_summary_events_and_polling(spec):
    operation = _operations(spec)[('get', f'{ITEM}/runs/{{run_id}}')]
    example = operation['responses']['200']['content']['application/json']['example']
    assert set(example['summary']) == {'duration_ms', 'tokens', 'cost_usd'}
    assert {event['kind'] for event in example['events']} == {
        'started', 'account', 'risk_check', 'agent_started', 'tool_call', 'paused', 'decision', 'finished'}
    assert all({'at', 'kind', 'text', 'level'} <= set(event) for event in example['events'])
    assert 'every 2 s' in operation['description'] and '`schedules.details`' in operation['description']
    # Owner decision 2026-10-21: the text says that a caller who is not the owner gets no tool result
    assert 'not the owner' in operation['description'] and '`summary`' in operation['description']
    assert {'403', '404'} <= set(operation['responses'])
    runs_row = _operations(spec)[('get', f'{ITEM}/runs')]['responses']['200']['content']['application/json']
    assert {'version_id', 'duration_ms'} <= set(runs_row['example']['rows'][0])


# Owner CR 2026-10-21 (risk review): the check and the save refusals are documented with their codes
def test_cr_el7011_risk_review_codes_are_documented_with_examples(spec):
    operations = _operations(spec)
    check = operations[('post', f'{BASE}/check')]
    assert '`schedules.create`' in check['description'] and 'nothing' in check['description'].lower()
    example = check['responses']['200']['content']['application/json']['example']
    assert {'blocking', 'sensitive', 'notices', 'run_without_asking_allowed', 'fingerprint'} == set(example)
    assert {item['code'] for item in example['sensitive']} == {'sensitive_action', 'personal_credential'}
    assert 'personal_credential' in check['description']
    assert check['requestBody']['content']['application/json']['examples']
    for key in (('post', BASE), ('put', ITEM)):
        operation = operations[key]
        conflict = str(operation['responses']['409'])
        for code in ('schedule_blocked', 'risk_confirmation_required', 'risk_changed'):
            assert code in operation['description'] and code in conflict, (key, code)
        assert 'risk_ack' in operation['description'] and 'risk_ack' in str(operation['responses']['400'])
    body = operations[('post', BASE)]['requestBody']['content']['application/json']['schema']['$ref']
    assert 'risk_ack' in spec['components']['schemas'][body.rsplit('/', 1)[-1]]['properties']


# R1.6: a restart reorders `with_modes`; the published paths stay the same
def test_r1_6_el7011_paths_do_not_depend_on_url_params_order(monkeypatch):
    first = _route_shape(_register(monkeypatch))
    second = _route_shape(_register(monkeypatch, reverse=True))
    assert first == second


# R1.2: the list and the run records publish their query options with type, enum and default
def test_r1_2_el7011_list_and_runs_publish_query_enums(spec):
    list_models = sys.modules[f'{PKG}.models.pd.schedule_list']
    filters = list_models.ScheduleListFilters.model_fields
    run_statuses = list(get_args(list_models.RunStatus))
    assert sys.modules[f'{PKG}.utils.schedule_openapi'].RunStatus is list_models.RunStatus  # one source of the values
    listing = _query(spec, 'get', BASE)
    assert set(listing) == {'entity_type', 'status', 'enabled', 'owner_ids', 'entity_id', 'search',
                            'limit', 'offset', 'sort_by', 'sort_order'}
    for name in ('sort_by', 'sort_order'):  # the published options are the ones the model accepts
        assert listing[name]['schema']['enum'] == list(get_args(filters[name].annotation))
        assert listing[name]['schema']['default'] == filters[name].default
    assert listing['entity_type']['schema']['enum'] == list(get_args(list_models.EntityType)) == ['agent', 'pipeline']
    assert listing['status']['schema']['enum'] == run_statuses
    assert listing['limit']['schema'] == {'type': 'integer', 'default': 20, 'minimum': 1, 'maximum': 100}
    assert listing['enabled']['schema']['type'] == 'boolean'

    runs = _query(spec, 'get', f'{ITEM}/runs')
    assert set(runs) == {'limit', 'offset', 'status'}
    assert runs['status']['schema']['enum'] == run_statuses
    assert runs['limit']['schema']['default'] == 20
    assert runs['offset']['schema']['default'] == 0

    assert _query(spec, 'get', f'{BASE}/export')['ids']['schema']['type'] == 'string'
    for params in (listing, runs):
        assert all(p['description'] for p in params.values())


# R1.4: the codes the handlers really answer, not the generic default list
@pytest.mark.parametrize('method, path, code', [
    ('post', BASE, '201'),
    ('post', f'{ITEM}/run', '202'),
    ('post', f'{ITEM}/run', '409'),
    ('delete', ITEM, '204'),
    ('post', f'{ITEM}/owner', '503'),
    ('post', f'{ITEM}/resume', '409'),
    ('post', f'{BASE}/draft', '502'),
])
def test_r1_4_el7011_operations_publish_real_codes(spec, method, path, code):
    responses = _operations(spec)[(method, path)]['responses']
    assert code in responses
    assert all(r['description'] for r in responses.values())


def test_r1_4_el7011_create_and_delete_do_not_publish_200(spec):
    operations = _operations(spec)
    assert '200' not in operations[('post', BASE)]['responses']
    assert '200' not in operations[('delete', ITEM)]['responses']


# R1.5: the tag has its description
def test_r1_5_el7011_tag_has_description(spec):
    tags = {tag['name']: tag for tag in spec['tags']}
    assert tags[TAG].get('description')


# R1.3: every request field has a description; the JSON bodies show an example
def test_r1_3_el7011_body_fields_have_descriptions(spec):
    schemas = _body_schemas(spec)
    assert {'ScheduleCreate', 'ScheduleEdit', 'ScheduleRunRequest', 'GenerateScheduleDraftRequest'} <= set(schemas)
    for name, schema in schemas.items():
        for field, prop in schema.get('properties', {}).items():
            assert prop.get('description'), f'{name}.{field}'


# R1.3 (AC3, D6): every field of the create, update, owner, run, draft and preference bodies shows an example
def test_r1_3_el7011_body_fields_have_examples(spec):
    schemas = _body_schemas(spec)
    assert {'ScheduleCreate', 'ScheduleEdit', 'ScheduleRunRequest', 'GenerateScheduleDraftRequest',
            'SchedulePreferenceUpdate', 'ScheduleProjectSettings', f'post {ITEM}/owner'} <= set(schemas)
    schemas.pop(f'post {BASE}/import')  # a file upload: no example value fits a binary part
    for name, schema in schemas.items():
        for field, prop in schema.get('properties', {}).items():
            assert prop.get('examples'), f'{name}.{field}'


# R1.3: the list query and row models describe each field; the query fields show an example
def test_r1_3_el7011_list_models_describe_fields(spec):
    list_models = sys.modules[f'{PKG}.models.pd.schedule_list']
    for model in (list_models.ScheduleListFilters, list_models.ScheduleListItem, list_models.ScheduleOwner):
        for field, info in model.model_fields.items():
            assert info.description, f'{model.__name__}.{field}'
    for field, info in list_models.ScheduleListFilters.model_fields.items():
        assert info.examples, f'ScheduleListFilters.{field}'


@pytest.mark.parametrize('method, path', [
    ('post', BASE),
    ('put', ITEM),
    ('post', f'{ITEM}/run'),
    ('post', f'{BASE}/draft'),
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
        assert endpoint['path'].startswith(f'{API_ROOT}/schedules/')
        assert endpoint['tags'] == [TAG]
        assert endpoint['mcp_tool'] is False


def test_r2_4_el7011_registration_without_openapi_registry_does_not_fail(monkeypatch):
    """In preload mode the shared plugin gives a registry with `register_plugin` only."""
    openapi_tools = _load(monkeypatch, OPENAPI_TOOLS, 'openapi_tools_7011')
    preload = types.SimpleNamespace(register_openapi=openapi_tools.register_openapi,
                                    openapi_registry=types.SimpleNamespace(register_plugin=lambda **kwargs: None))
    plugin = _install(monkeypatch, preload)
    plugin.Module._register_openapi(types.SimpleNamespace(descriptor=types.SimpleNamespace(metadata={})))


# R2.4, R6.2, R8.3 (EL-7011 BUG-29): every error answer shows the one refusal body, `code` and `error`
def test_r2_4_el7011_every_error_answer_example_has_code(spec):
    for key, operation in _operations(spec).items():
        for status, response in operation['responses'].items():
            if int(status) < 400:
                continue
            example = response['content']['application/json']['example']
            assert example['code'] and example['error'], (key, status)
            assert re.fullmatch(r'[a-z_]+', example['code']), (key, status)
