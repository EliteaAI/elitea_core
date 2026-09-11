"""Issue #6410, round two - one continuous chain from the published tool schema to a 200.

PR #403 specified the schema side and the handler side independently, in different files, by
different mechanisms - and shipped a contract that contradicted itself: the published
`put_elitea_core_skill` schema marked `user_id` **required**, the MCP executor routes every
argument that is not a declared path/query parameter into the request **body**, and the
version-targeted handler validates that raw body against a model with `extra="forbid"` and no
`user_id` field. Omitting the key stopped the SDK from starting the tool; sending it 400'd.

Nothing in the test suite could see that, because no test ran the schema and the handler on the
same path. This one does: the real `models/pd/skill.py`, the real `shared/tools/openapi_tools.py`
registration, the real `McpApiToolExecutor._parse_arguments`, and the real `PromptLibAPI.put`,
in a single assertion chain. Only `skill_utils` (the DB writers) is a stand-in, and it records
which writer ran with what - a status code alone distinguishes neither branch (both return 200)
nor a real write from an all-`None` no-op.

Run via:
    python tests/run_tests.py integration/test_6410b_mcp_put_skill_round_trip.py -v
"""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
OPENAPI_TOOLS = PLUGIN_ROOT.parent / 'shared' / 'tools' / 'openapi_tools.py'

pytestmark = pytest.mark.skipif(
    not OPENAPI_TOOLS.exists(),
    reason='shared plugin is not checked out next to elitea_core',
)

PKG = 'skillpkg_6410b_integration'

BASE_PATH = '/api/v2/elitea_core/skill'
PINNED_PATH = '/api/v2/elitea_core/skill/prompt_lib/13/7'

URL_PARAMS = [
    '<int:project_id>/<int:skill_id>',
    '<int:project_id>/<int:skill_id>/<int:version_id>',
]

CALLS = {'update_skill': [], 'update_skill_version': []}


class _Args(dict):
    pass


class _Request:
    args = _Args()
    json = {}
    method = 'PUT'
    path = PINNED_PATH
    environ = {}

    @classmethod
    def get_json(cls, silent=False):
        return cls.json


def _with_modes(url_params):
    params = set()
    for i in url_params:
        if not i.startswith('<string:mode>'):
            params.add('<string:mode>' if i == '' else f'<string:mode>/{i}')
        params.add(i)
    return list(params)


def _register(name, module):
    sys.modules[name] = module
    return module


def _load_real(rel_path, name, root=None):
    spec = importlib.util.spec_from_file_location(name, (root or PLUGIN_ROOT) / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_openapi_tools():
    for name in ('pylon', 'pylon.core'):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)
    pylon_core_tools = types.ModuleType('pylon.core.tools')
    pylon_core_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    _register('pylon.core.tools', pylon_core_tools)

    spec = importlib.util.spec_from_file_location('openapi_tools_6410b', OPENAPI_TOOLS)
    module = importlib.util.module_from_spec(spec)
    _register('openapi_tools_6410b', module)
    spec.loader.exec_module(module)
    return module


def _install_package(openapi_tools):
    def _endpoint_metrics(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        @staticmethod
        def with_modes(_):
            return _with_modes(URL_PARAMS)

        endpoint_metrics = staticmethod(_endpoint_metrics)

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.rpc_tools = types.SimpleNamespace()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration',
                                         DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
        current_user=lambda: {'id': 1},
    )
    tools.register_openapi = openapi_tools.register_openapi
    tools.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools.this = types.SimpleNamespace()
    tools.openapi_registry = types.SimpleNamespace(get_mcp_api_tools=lambda *a, **k: [])
    tools.sanitize_property_name = openapi_tools.sanitize_property_name
    _register('tools', tools)

    flask = types.ModuleType('flask')
    flask.request = _Request
    _register('flask', flask)

    for name in (PKG, f'{PKG}.api', f'{PKG}.models', f'{PKG}.models.pd',
                 f'{PKG}.models.enums', f'{PKG}.utils'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        _register(name, mod)

    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    _register(f'{PKG}.api.v2', v2_pkg)

    _load_real('models/pd/collection_base.py', f'{PKG}.models.pd.collection_base')
    _load_real('models/pd/tag.py', f'{PKG}.models.pd.tag')
    _load_real('models/enums/all.py', f'{PKG}.models.enums.all')
    _load_real('utils/constants.py', f'{PKG}.utils.constants')

    authors = types.ModuleType(f'{PKG}.utils.authors')
    authors.get_authors_data = lambda author_ids: []
    _register(f'{PKG}.utils.authors', authors)

    skill_version = _load_real('models/pd/skill_version.py', f'{PKG}.models.pd.skill_version')
    skill_models = _load_real('models/pd/skill.py', f'{PKG}.models.pd.skill')

    def _record(name):
        def call(*args, **kwargs):
            CALLS[name].append(kwargs)
            return {'id': 1}
        return call

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('delete_skill', 'create_skill_version', 'delete_skill_version',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: {'id': 1})
    skill_utils.update_skill = _record('update_skill')
    skill_utils.update_skill_version = _record('update_skill_version')
    skill_utils.get_skill_version_by_id = (
        lambda *a, **k: type('SkillVersion', (), {'id': k.get('version_id')})())
    skill_utils.get_skill_details = lambda *a, **k: {'data': {'id': 1}}
    skill_utils.SkillError = type('SkillError', (Exception,), {'http_status': 400})
    _register(f'{PKG}.utils.skill_utils', skill_utils)

    folder_access = types.ModuleType(f'{PKG}.utils.folder_access')
    folder_access.require_folder_access = lambda *a, **k: (lambda f: f)
    _register(f'{PKG}.utils.folder_access', folder_access)

    mcp_versioning = types.ModuleType(f'{PKG}.utils.mcp_versioning')
    mcp_versioning.INTERNAL_MCP_ENVIRON_KEY = 'elitea.internal_mcp_request'
    _register(f'{PKG}.utils.mcp_versioning', mcp_versioning)

    api = _load_real('api/v2/skill.py', f'{PKG}.api.v2.skill')
    return types.SimpleNamespace(api=api, skill=skill_models, skill_version=skill_version)


def _cleanup():
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask', 'openapi_tools_6410b'):
        sys.modules.pop(key, None)


@pytest.fixture()
def stack():
    saved = {k: v for k, v in sys.modules.items()
             if k in ('tools', 'flask') or k.startswith(PKG)}
    openapi_tools = _load_openapi_tools()
    loaded = _install_package(openapi_tools)
    registry = openapi_tools.OpenAPIRegistry()
    # register_plugin first: get_plugin_spec() returns {} for an unregistered plugin.
    registry.register_plugin(plugin_name='elitea_core', base_path=BASE_PATH)
    openapi_tools.register_api_class(loaded.api.API, 'elitea_core', BASE_PATH, registry)
    _Request.args = _Args()
    _Request.json = {}
    for calls in CALLS.values():
        calls.clear()
    yield types.SimpleNamespace(
        openapi_tools=openapi_tools,
        registry=registry,
        api=loaded.api,
        skill=loaded.skill,
        skill_version=loaded.skill_version,
        tools={tool['value']: tool for tool in registry.get_mcp_api_tools(
            plugins=['elitea_core'], filter_tags=['elitea_core/skills'])},
    )
    _cleanup()
    sys.modules.update(saved)


MCP_STUBS = (
    ('plugins.elitea_core.models.all', {'Application': type('Application', (), {}),
                                        'ApplicationVersion': type('ApplicationVersion', (), {})}),
    ('plugins.elitea_core.utils.application_tools', {'toolkits_listing': lambda **k: {'rows': []}}),
    ('plugins.elitea_core.utils.application_utils', {'list_applications_api': lambda *a, **k: {}}),
    ('plugins.elitea_core.utils.toolkits_utils', {'get_toolkit_schemas': lambda *a, **k: {}}),
    ('plugins.elitea_core.utils.exceptions',
     {'PoolSaturationError': type('PoolSaturationError', (Exception,), {})}),
    ('plugins.elitea_core.utils.internal_tools',
     {'MCP_CURRENT_PROJECT_SUFFIXES': {'elitea_core/project_context', 'elitea_core/skills'},
      'MCP_PROJECT_SCOPED_SUFFIXES': {'elitea_core/project_context', 'elitea_core/skills',
                                      'elitea_core/applications'}}),
    ('plugins.elitea_core.utils.mcp_session', {'SseSession': type('SseSession', (), {})}),
    ('plugins.elitea_core.utils.mcp_versioning',
     {'INTERNAL_MCP_ENVIRON_KEY': 'elitea.internal_mcp_request'}),
)


@pytest.fixture()
def executor(stack, monkeypatch):
    """The real `McpApiToolExecutor`, loaded against the stubbed package tree."""
    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.utils',
                 'plugins.elitea_core.models'):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        monkeypatch.setitem(sys.modules, name, mod)

    for name, attrs in MCP_STUBS:
        mod = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(mod, attr, value)
        monkeypatch.setitem(sys.modules, name, mod)

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.mcp_service', PLUGIN_ROOT / 'utils' / 'mcp_service.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module.McpApiToolExecutor


def _dispatch(executor, tool, arguments):
    path_params, query_params, body_params = executor._parse_arguments(
        arguments, tool['parameters'])
    return executor._build_url_path(tool['path'], path_params), query_params, body_params


def _drive(stack, query_params, body_params, project_id=13, skill_id=7):
    """Feed the executor's own output into the real handler, the way WSGI would."""
    _Request.args = _Args({k: str(v) for k, v in query_params.items()})
    _Request.json = body_params
    return stack.api.PromptLibAPI.put(None, project_id=project_id, skill_id=skill_id)


def test_a_version_targeted_tool_call_round_trips_into_a_200(executor, stack):
    """The whole chain, in order: schema -> arguments -> executor -> handler -> writer."""
    tool = stack.tools['put_elitea_core_skill']
    schema = tool['args_schema']

    # (a) nothing the caller must send can land in the body.
    assert schema['required'] == ['project_id', 'skill_id']
    assert 'user_id' not in schema['properties']
    assert 'version' in schema['properties']

    # (b) a schema-conforming version edit routes cleanly.
    url, query, body = _dispatch(executor, tool, {
        'project_id': 13, 'skill_id': 7, 'version_id': 8,
        'version': {'instructions': 'x'},
    })
    assert url == PINNED_PATH
    assert query == {'version_id': 8}
    assert body == {'version': {'instructions': 'x'}}

    # (c) and the handler writes that version - not the default one, and not an all-None no-op.
    detail, status = _drive(stack, query, body)
    assert status == 200, detail
    write, = CALLS['update_skill_version']
    assert write['version_id'] == 8
    assert write['update_data'].instructions == 'x'
    assert CALLS['update_skill'] == []


def test_a_versionless_tool_call_still_round_trips(executor, stack):
    """The control: the one MCP shape that worked before the fix must keep working, through the
    metadata branch, with the author still resolved server-side."""
    tool = stack.tools['put_elitea_core_skill']

    url, query, body = _dispatch(executor, tool, {
        'project_id': 13, 'skill_id': 7,
        'name': 'my-skill', 'version': {'id': 8, 'instructions': 'x'},
    })
    assert url == PINNED_PATH
    assert query == {}
    assert body == {'name': 'my-skill', 'version': {'id': 8, 'instructions': 'x'}}

    detail, status = _drive(stack, query, body)
    assert status == 200, detail
    write, = CALLS['update_skill']
    assert write['update_data'].name == 'my-skill'
    assert write['update_data'].version.instructions == 'x'
    assert write['update_data'].user_id == 1
    assert CALLS['update_skill_version'] == []


def test_every_required_tool_argument_is_acceptable_to_the_handler(executor, stack):
    """The invariant #403 broke. Stated as an implication rather than `body_params == {}`, so a
    future genuinely-required body field is not punished: every argument the schema *forces* a
    caller to send must either be consumed into the URL, or be a field of both request models -
    i.e. accepted by whichever branch the URL selects. `user_id` was neither."""
    tool = stack.tools['put_elitea_core_skill']
    schema = tool['args_schema']
    required = schema.get('required', [])
    assert required, 'a tool with no required arguments would make this vacuous'

    arguments = {name: 1 for name in required}
    path_params, query_params, _ = executor._parse_arguments(arguments, tool['parameters'])
    consumed = set(path_params) | set(query_params)

    http_fields = set(stack.skill.SkillUpdateModel.model_fields)
    version_fields = set(stack.skill_version.SkillVersionUpdateModel.model_fields)
    for name in required:
        if name in consumed:
            continue
        assert name in http_fields and name in version_fields, (
            f'required tool argument {name!r} is routed into the request body, '
            f'which at least one branch of put() rejects'
        )


def test_the_public_http_body_still_carries_the_transport_fields(stack):
    """`mcp_request_body` narrows only the model-facing projection. `_build_paths` reads
    `request_body`, so the published OpenAPI contract - and every direct HTTP caller - is
    unchanged, and the narrow DTO never leaks into components/schemas."""
    endpoint, = [e for e in stack.registry._endpoints['elitea_core'] if e['method'] == 'put']
    assert endpoint['request_body'] is stack.skill.SkillUpdateModel
    assert endpoint['mcp_request_body'] is stack.skill.SkillMcpUpdateModel

    spec = stack.registry.get_plugin_spec('elitea_core', full=True)
    assert 'SkillMcpUpdateModel' not in spec['components']['schemas']
    body_schema = spec['components']['schemas']['SkillUpdateModel']
    assert body_schema['required'] == ['project_id', 'user_id']
