"""Issue #6411 - the spec the real registration code derives for the skill satellite endpoints.

Runs the actual `register_api_class` from the `shared` plugin over the real `skill_export.py`,
`skill_export_fork.py` and `upload_skill_icon.py`. Before the fix each published its version
selector as a *required* path parameter while its description promised the selector was optional -
the contradiction reported in #6411 against `put_elitea_core_skill` and fixed there by #6412.

`upload_skill_icon` additionally had three URL patterns scoring equally for `get`, so the published
path depended on set iteration order and changed between pylon restarts.

Only leaf dependencies are stand-ins; the path handling under test is entirely real code.
"""
import functools
import importlib.util
import pathlib
import sys
import types
from typing import Optional

import pytest
from pydantic import BaseModel

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
OPENAPI_TOOLS = PLUGIN_ROOT.parent / 'shared' / 'tools' / 'openapi_tools.py'

pytestmark = pytest.mark.skipif(
    not OPENAPI_TOOLS.exists(),
    reason='shared plugin is not checked out next to elitea_core',
)

PKG = 'skillpkg_6411_integration'

BASE = '/api/v2/elitea_core'
SKILL_SUFFIX = '{mode}/{project_id}/{skill_id}'

EXPECTED_PATHS = {
    ('skill_export', 'get'): f'{BASE}/skill_export/{SKILL_SUFFIX}',
    ('skill_export_fork', 'get'): f'{BASE}/skill_export_fork/{SKILL_SUFFIX}',
    ('upload_skill_icon', 'get'): f'{BASE}/upload_skill_icon/{{mode}}/{{project_id}}',
    ('upload_skill_icon', 'post'): f'{BASE}/upload_skill_icon/{{mode}}/{{project_id}}',
    ('upload_skill_icon', 'put'):
        f'{BASE}/upload_skill_icon/{{mode}}/{{project_id}}/{{skill_version_id}}',
    ('upload_skill_icon', 'delete'):
        f'{BASE}/upload_skill_icon/{{mode}}/{{project_id}}/{{icon_name}}',
}

MODULES = ('skill_export', 'skill_export_fork', 'upload_skill_icon')

# The selector each module used to publish as a required path parameter.
SELECTORS = {
    'skill_export': 'version_id',
    'skill_export_fork': 'version_id',
    'upload_skill_icon': 'skill_version_id',
}


class UpdateIcon(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None


class _Request:
    args = {}
    json = {}
    files = {}
    form = {}


def _with_modes(url_params):
    """Order-preserving twin of `api_tools.with_modes`, which returns `list(set(...))`.

    Preserving order lets a test register the same class under two different orderings and assert
    the published path does not move.
    """
    params = []
    for i in url_params:
        if not i.startswith('<string:mode>'):
            params.append('<string:mode>' if i == '' else f'<string:mode>/{i}')
        params.append(i)
    return params


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

    spec = importlib.util.spec_from_file_location('openapi_tools_6411', OPENAPI_TOOLS)
    module = importlib.util.module_from_spec(spec)
    sys.modules['openapi_tools_6411'] = module
    spec.loader.exec_module(module)
    return module


def _install_package(openapi_tools, reverse_url_params=False):
    def _endpoint_metrics(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    def expand_url_params(url_params):
        expanded = _with_modes(url_params)
        return list(reversed(expanded)) if reverse_url_params else expanded

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        with_modes = staticmethod(expand_url_params)
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
    flask.Response = type('Response', (), {'__init__': lambda self, *a, **k: None})

    def _package(name, path=None):
        mod = types.ModuleType(name)
        mod.__path__ = path or []
        return mod

    modules = {
        PKG: _package(PKG),
        f'{PKG}.api': _package(f'{PKG}.api'),
        f'{PKG}.api.v2': _package(f'{PKG}.api.v2', [str(PLUGIN_ROOT / 'api' / 'v2')]),
        f'{PKG}.models': _package(f'{PKG}.models'),
        f'{PKG}.models.pd': _package(f'{PKG}.models.pd'),
        f'{PKG}.utils': _package(f'{PKG}.utils'),
        'flask': flask,
        'tools': tools,
    }

    pd_skill = types.ModuleType(f'{PKG}.models.pd.skill')
    pd_skill.SkillUpdateModel = type('SkillUpdateModel', (BaseModel,), {})
    pd_skill.SkillUpdateRelationModel = type('SkillUpdateRelationModel', (BaseModel,), {})
    modules[f'{PKG}.models.pd.skill'] = pd_skill

    pd_skill_version = types.ModuleType(f'{PKG}.models.pd.skill_version')
    pd_skill_version.SkillVersionCreateModel = type('SkillVersionCreateModel', (BaseModel,), {})
    pd_skill_version.SkillVersionUpdateModel = type('SkillVersionUpdateModel', (BaseModel,), {})
    modules[f'{PKG}.models.pd.skill_version'] = pd_skill_version

    pd_icon_meta = types.ModuleType(f'{PKG}.models.pd.icon_meta')
    pd_icon_meta.UpdateIcon = UpdateIcon
    modules[f'{PKG}.models.pd.icon_meta'] = pd_icon_meta

    models_skill = types.ModuleType(f'{PKG}.models.skill')
    models_skill.SkillVersion = type('SkillVersion', (), {})
    modules[f'{PKG}.models.skill'] = models_skill

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('get_skill_details', 'update_skill', 'delete_skill', 'create_skill_version',
                 'update_skill_version', 'delete_skill_version', 'get_skill_version_by_id',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: None)
    skill_utils.SkillError = type('SkillError', (Exception,), {'http_status': 400})
    modules[f'{PKG}.utils.skill_utils'] = skill_utils

    export_import = types.ModuleType(f'{PKG}.utils.skill_export_import')
    export_import.export_skill_md = lambda *a, **k: None
    export_import.build_skill_fork_payload = lambda *a, **k: None
    modules[f'{PKG}.utils.skill_export_import'] = export_import

    export_import_utils = types.ModuleType(f'{PKG}.utils.export_import_utils')
    export_import_utils.content_disposition_attachment = lambda *a, **k: ''
    modules[f'{PKG}.utils.export_import_utils'] = export_import_utils

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'
    modules[f'{PKG}.utils.constants'] = constants

    sys.modules.update(modules)

    loaded = {}
    # `skill` first: the satellites import SKILL_PATH/resolve_version_id from it.
    for name in ('skill',) + MODULES:
        full = f'{PKG}.api.v2.{name}'
        spec = importlib.util.spec_from_file_location(
            full, PLUGIN_ROOT / 'api' / 'v2' / f'{name}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
        loaded[name] = module
    return loaded


def _cleanup():
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask', 'openapi_tools_6411'):
        sys.modules.pop(key, None)


def _register(reverse_url_params=False):
    openapi_tools = _load_openapi_tools()
    loaded = _install_package(openapi_tools, reverse_url_params)
    registry = openapi_tools.OpenAPIRegistry()
    for name, module in loaded.items():
        if name in MODULES:
            openapi_tools.register_api_class(
                module.API, 'elitea_core', f'{BASE}/{name}', registry)
    return registry


@pytest.fixture()
def registry():
    saved = {k: v for k, v in sys.modules.items() if k in ('tools', 'flask')}
    yield _register()
    _cleanup()
    sys.modules.update(saved)


def _endpoints(registry):
    return {(e['path'].split('/')[4], e['method']): e
            for e in registry._endpoints['elitea_core']}


@pytest.mark.parametrize('key,expected', sorted(EXPECTED_PATHS.items()))
def test_published_path_is_pinned(registry, key, expected):
    endpoints = _endpoints(registry)
    assert key in endpoints, f'{key} was not registered at all'
    assert endpoints[key]['path'] == expected


@pytest.mark.parametrize('module', MODULES)
def test_the_version_selector_is_not_a_path_parameter(registry, module):
    """The #6411 regression: the selector the description calls optional was published required."""
    selector = SELECTORS[module]
    for (name, method), endpoint in _endpoints(registry).items():
        if name != module:
            continue
        optional_by_description = (method, name) != ('put', 'upload_skill_icon')
        if not optional_by_description:
            continue
        path_params = {p['name'] for p in endpoint['parameters'] if p['in'] == 'path'}
        assert selector not in path_params, f'{name}.{method}'


@pytest.mark.parametrize('module,method', [
    ('skill_export', 'get'),
    ('skill_export_fork', 'get'),
    ('upload_skill_icon', 'post'),
])
def test_the_version_selector_is_an_optional_query_parameter(registry, module, method):
    endpoint = _endpoints(registry)[(module, method)]
    selector = next(
        (p for p in endpoint['parameters']
         if p['name'] == SELECTORS[module] and p['in'] == 'query'), None)
    assert selector is not None, f'{module}.{method} does not expose its selector as a query param'
    assert selector['required'] is False


def test_upload_skill_icon_put_still_requires_the_version():
    """The one selector that genuinely is required - its description never claimed otherwise."""
    registry = _register()
    try:
        endpoint = _endpoints(registry)[('upload_skill_icon', 'put')]
        path_params = {p['name'] for p in endpoint['parameters'] if p['in'] == 'path'}
        assert 'skill_version_id' in path_params
    finally:
        _cleanup()


def test_published_paths_do_not_depend_on_url_params_ordering():
    """`with_modes` returns `list(set(...))`, so ordering varies between pylon restarts."""
    forward = {k: e['path'] for k, e in _endpoints(_register()).items()}
    _cleanup()
    reverse = {k: e['path'] for k, e in _endpoints(_register(reverse_url_params=True)).items()}
    _cleanup()
    assert forward == reverse
