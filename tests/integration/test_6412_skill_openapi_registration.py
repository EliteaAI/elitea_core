"""Issue #6412 - the MCP schema the real registration code derives from `skill.py`.

Runs the actual `register_api_class` / `build_mcp_input_schema` from the `shared` plugin over the
real `skill.py` API class. Before the fix this produced
`/api/v2/elitea_core/skill/{mode}/{project_id}/{skill_id}/{version_id}` with `version_id` in
`required` for every method - the exact schema/route mismatch reported in #6412 - because the URL
pattern was chosen by matching the handler signature and path parameters were force-required.

Only the request-body models are stand-ins; the path handling under test is entirely real code.
"""
import functools
import importlib.util
import pathlib
import sys
import types
from typing import Optional

import pytest
from pydantic import BaseModel, Field

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
OPENAPI_TOOLS = PLUGIN_ROOT.parent / 'shared' / 'tools' / 'openapi_tools.py'

pytestmark = pytest.mark.skipif(
    not OPENAPI_TOOLS.exists(),
    reason='shared plugin is not checked out next to elitea_core',
)

PKG = 'skillpkg_6412_integration'

BASE_PATH = '/api/v2/elitea_core/skill'
PINNED_PATH = '/api/v2/elitea_core/skill/{mode}/{project_id}/{skill_id}'

URL_PARAMS = [
    '<int:project_id>/<int:skill_id>',
    '<int:project_id>/<int:skill_id>/<int:version_id>',
]


class SkillUpdateRelationModel(BaseModel):
    entity_version_id: int
    entity_type: str = 'agent'
    has_relation: bool = False
    skill_version_id: Optional[int] = None


class SkillArgsForwardingModel(BaseModel):
    """Mirrors `models/pd/skill.py:66` - `exclude=True` hides these from dumps, not from the schema."""

    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)


class SkillUpdateModel(SkillArgsForwardingModel):
    name: Optional[str] = None
    description: Optional[str] = None
    meta: Optional[dict] = None


class SkillVersionCreateModel(BaseModel):
    name: str
    instructions: str


class SkillVersionUpdateModel(BaseModel):
    instructions: Optional[str] = None


class _Request:
    args = {}
    json = {}


def _with_modes(url_params):
    params = set()
    for i in url_params:
        if not i.startswith('<string:mode>'):
            params.add('<string:mode>' if i == '' else f'<string:mode>/{i}')
        params.add(i)
    return list(params)


def _load_openapi_tools():
    pylon = sys.modules.get('pylon') or types.ModuleType('pylon')
    pylon_core = sys.modules.get('pylon.core') or types.ModuleType('pylon.core')
    pylon_core_tools = types.ModuleType('pylon.core.tools')
    pylon_core_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules['pylon'] = pylon
    sys.modules['pylon.core'] = pylon_core
    sys.modules['pylon.core.tools'] = pylon_core_tools

    spec = importlib.util.spec_from_file_location('openapi_tools_6412', OPENAPI_TOOLS)
    module = importlib.util.module_from_spec(spec)
    sys.modules['openapi_tools_6412'] = module
    spec.loader.exec_module(module)
    return module


def _install_package(openapi_tools, url_params):
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
            return _with_modes(url_params)

        endpoint_metrics = staticmethod(_endpoint_metrics)

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
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
    tools.sanitize_property_name = lambda name: name

    flask = types.ModuleType('flask')
    flask.request = _Request

    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    api_pkg = types.ModuleType(f'{PKG}.api')
    api_pkg.__path__ = []
    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = []

    pd_skill = types.ModuleType(f'{PKG}.models.pd.skill')
    pd_skill.SkillUpdateModel = SkillUpdateModel
    pd_skill.SkillUpdateRelationModel = SkillUpdateRelationModel

    pd_skill_version = types.ModuleType(f'{PKG}.models.pd.skill_version')
    pd_skill_version.SkillVersionCreateModel = SkillVersionCreateModel
    pd_skill_version.SkillVersionUpdateModel = SkillVersionUpdateModel

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('get_skill_details', 'update_skill', 'delete_skill', 'create_skill_version',
                 'update_skill_version', 'delete_skill_version', 'get_skill_version_by_id',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: None)
    skill_utils.SkillError = type('SkillError', (Exception,), {'http_status': 400})

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    for name, mod in {
        PKG: pkg,
        f'{PKG}.api': api_pkg,
        f'{PKG}.api.v2': v2_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.skill': pd_skill,
        f'{PKG}.models.pd.skill_version': pd_skill_version,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.skill_utils': skill_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.api.v2.skill'
    spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / 'api' / 'v2' / 'skill.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


def _register(url_params):
    openapi_tools = _load_openapi_tools()
    skill = _install_package(openapi_tools, url_params)
    registry = openapi_tools.OpenAPIRegistry()
    openapi_tools.register_api_class(skill.API, 'elitea_core', BASE_PATH, registry)
    return registry


def _cleanup():
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask', 'openapi_tools_6412'):
        sys.modules.pop(key, None)


@pytest.fixture()
def registry():
    saved = {k: v for k, v in sys.modules.items() if k in ('tools', 'flask')}
    yield _register(URL_PARAMS)
    _cleanup()
    sys.modules.update(saved)


@pytest.fixture()
def mcp_tools(registry):
    return {tool['value']: tool for tool in registry.get_mcp_api_tools(
        plugins=['elitea_core'], filter_tags=['elitea_core/skills'])}


def _endpoints(registry):
    return {e['method']: e for e in registry._endpoints['elitea_core']}


def test_every_method_registers_the_versionless_path(registry):
    for method, endpoint in _endpoints(registry).items():
        assert endpoint['path'] == PINNED_PATH, method


def test_no_endpoint_publishes_version_id_as_a_path_parameter(registry):
    for method, endpoint in _endpoints(registry).items():
        path_params = {p['name'] for p in endpoint['parameters'] if p['in'] == 'path'}
        assert 'version_id' not in path_params, method


def test_mcp_tool_names_are_unchanged(mcp_tools):
    assert set(mcp_tools) == {
        'get_elitea_core_skill', 'put_elitea_core_skill', 'patch_elitea_core_skill'}


def test_relation_patch_schema_matches_the_route(mcp_tools):
    schema = mcp_tools['patch_elitea_core_skill']['args_schema']
    assert schema['required'] == ['project_id', 'skill_id', 'entity_version_id']
    assert 'version_id' not in schema['properties']
    assert 'skill_version_id' in schema['properties']


def test_skill_details_takes_an_optional_version_id(mcp_tools):
    schema = mcp_tools['get_elitea_core_skill']['args_schema']
    assert schema['required'] == ['project_id', 'skill_id']
    assert schema['properties']['version_id']['type'] == 'integer'


def test_metadata_update_no_longer_demands_a_version(mcp_tools):
    """#6411 gave PUT a documented `version_id` *query* selector, matching get/delete and the
    skill_export siblings, so the property exists again - but the point of #6412 stands: it must
    never be required, and it must never be a path parameter (pinned separately above)."""
    schema = mcp_tools['put_elitea_core_skill']['args_schema']
    assert schema['properties']['version_id']['type'] == 'integer'
    assert 'version_id' not in schema['required']


def test_metadata_update_still_demands_the_server_injected_user_id(mcp_tools):
    """Pre-existing wart, newly reachable: `SkillUpdateModel` inherits `user_id` as a required
    body field even though `put` overwrites it from `auth.current_user()`. Pinned so the follow-up
    that removes it has to update this test deliberately."""
    schema = mcp_tools['put_elitea_core_skill']['args_schema']
    assert schema['required'] == ['project_id', 'skill_id', 'user_id']


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
def executor(registry, monkeypatch):
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


def test_relation_patch_reaches_the_route_the_handler_accepts(executor, mcp_tools):
    """The exact call from #6412, minus the version_id the tool no longer demands."""
    url, query, body = _dispatch(executor, mcp_tools['patch_elitea_core_skill'], {
        'project_id': 13, 'skill_id': 7,
        'entity_version_id': 477, 'skill_version_id': 8, 'has_relation': True,
    })
    assert url == '/api/v2/elitea_core/skill/prompt_lib/13/7'
    assert query == {}
    assert body == {'entity_version_id': 477, 'skill_version_id': 8, 'has_relation': True}


def test_skill_details_sends_the_version_as_a_query_argument(executor, mcp_tools):
    url, query, body = _dispatch(executor, mcp_tools['get_elitea_core_skill'], {
        'project_id': 13, 'skill_id': 7, 'version_id': 8})
    assert url == '/api/v2/elitea_core/skill/prompt_lib/13/7'
    assert query == {'version_id': 8}
    assert body == {}


def test_skill_details_without_a_version_hits_the_default(executor, mcp_tools):
    url, query, _ = _dispatch(executor, mcp_tools['get_elitea_core_skill'], {
        'project_id': 13, 'skill_id': 7})
    assert url == '/api/v2/elitea_core/skill/prompt_lib/13/7'
    assert query == {}


def test_unfilled_path_placeholder_is_named(executor):
    with pytest.raises(ValueError, match='skill_id'):
        executor._build_url_path('/api/v2/elitea_core/skill/prompt_lib/{skill_id}', {})


def test_a_braced_parameter_value_is_not_mistaken_for_a_placeholder(executor):
    """Unresolved placeholders come from the template, so a `{` inside a value is just a value."""
    assert executor._build_url_path(
        '/api/v2/artifacts/artifact/{mode}/{project_id}/{filename}',
        {'mode': 'prompt_lib', 'project_id': 2, 'filename': 'weird{name}.txt'},
    ) == '/api/v2/artifacts/artifact/prompt_lib/2/weird{name}.txt'


def test_registration_does_not_depend_on_url_params_ordering():
    saved = {k: v for k, v in sys.modules.items() if k in ('tools', 'flask')}
    try:
        paths = []
        for url_params in (URL_PARAMS, list(reversed(URL_PARAMS))):
            registry = _register(url_params)
            paths.append({m: e['path'] for m, e in _endpoints(registry).items()})
            _cleanup()
        assert paths[0] == paths[1]
    finally:
        _cleanup()
        sys.modules.update(saved)
