"""Issue #6411 - version selection on the skill satellite endpoints.

`skill_export`, `skill_export_fork` and `upload_skill_icon` each declared their version selector as
an optional *path* parameter. OpenAPI cannot express that, so it was published as required while
the description promised it could be omitted. Each is now pinned to the short path with the
selector re-declared as a query argument, resolved through `skill.resolve_version_id`.

The Flask routes carrying the trailing segment stay registered - every EliteaUI caller builds that
shape (`features/skill/api/skillsApi.js`), so both forms must keep working.

`tests/integration/test_6411_skill_satellite_registration.py` pins the spec the real registration
code derives from these decorators.
"""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'skillpkg_6411_unit'

MODULES = ('skill_export', 'skill_export_fork', 'upload_skill_icon')

SELECTORS = {
    'skill_export': ('get', 'version_id'),
    'skill_export_fork': ('get', 'version_id'),
    'upload_skill_icon': ('post', 'skill_version_id'),
}

VERSIONED_ROUTE = {
    'skill_export': '<int:project_id>/<int:skill_id>/<int:version_id>',
    'skill_export_fork': '<int:project_id>/<int:skill_id>/<int:version_id>',
    'upload_skill_icon': '<string:mode>/<int:project_id>/<int:skill_version_id>',
}

CALLS = {'export': [], 'fork': [], 'icon_bind': []}


class _Args(dict):
    pass


class _FakeFile:
    def seek(self, *args):
        return None

    def tell(self):
        return 10


class _Request:
    args = _Args()
    json = {}
    files = {}
    form = {}


class _Response:
    def __init__(self, content, **kwargs):
        self.content = content
        self.kwargs = kwargs


def _with_modes(url_params):
    """Order-preserving twin of `api_tools.with_modes`, which returns `list(set(...))`.

    Faithful expansion matters here: an identity stub would hide whether a pinned
    `path_suffix_override` corresponds to a route that is actually registered.
    """
    params = []
    for i in url_params:
        if not i.startswith('<string:mode>'):
            params.append('<string:mode>' if i == '' else f'<string:mode>/{i}')
        params.append(i)
    return params


def _install_package():
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        warning=lambda *a, **k: None, info=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    for name in ('pylon', 'pylon.core'):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    sys.modules['pylon.core.tools'] = pylon_tools

    def _register_openapi(**meta):
        def decorator(func):
            func._openapi = meta
            return func
        return decorator

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

        with_modes = staticmethod(_with_modes)
        endpoint_metrics = staticmethod(_endpoint_metrics)

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration',
                                         DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
        current_user=lambda: {'id': 1},
    )
    tools.register_openapi = _register_openapi
    tools.db = types.SimpleNamespace(get_session=lambda pid: None)

    flask = types.ModuleType('flask')
    flask.request = _Request
    flask.Response = _Response

    def _package(name, path=None):
        mod = types.ModuleType(name)
        mod.__path__ = path or []
        return mod

    def _record(bucket, result):
        def call(*args, **kwargs):
            CALLS[bucket].append(kwargs)
            return result
        return call

    pd_skill = types.ModuleType(f'{PKG}.models.pd.skill')
    pd_skill.SkillUpdateModel = type('SkillUpdateModel', (), {})
    pd_skill.SkillUpdateRelationModel = type('SkillUpdateRelationModel', (), {})

    pd_skill_version = types.ModuleType(f'{PKG}.models.pd.skill_version')
    pd_skill_version.SkillVersionCreateModel = type('SkillVersionCreateModel', (), {})
    pd_skill_version.SkillVersionUpdateModel = type('SkillVersionUpdateModel', (), {})

    pd_icon_meta = types.ModuleType(f'{PKG}.models.pd.icon_meta')
    pd_icon_meta.UpdateIcon = type('UpdateIcon', (), {})

    models_skill = types.ModuleType(f'{PKG}.models.skill')
    models_skill.SkillVersion = type('SkillVersion', (), {})

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('get_skill_details', 'update_skill', 'delete_skill', 'create_skill_version',
                 'update_skill_version', 'delete_skill_version', 'get_skill_version_by_id',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: None)
    skill_utils.SkillError = type('SkillError', (Exception,), {'http_status': 400})

    export_import = types.ModuleType(f'{PKG}.utils.skill_export_import')
    export_import.export_skill_md = _record(
        'export', {'ok': True, 'content': 'md', 'filename': 'skill.md'})
    export_import.build_skill_fork_payload = _record('fork', {'name': 'skill'})

    export_import_utils = types.ModuleType(f'{PKG}.utils.export_import_utils')
    export_import_utils.content_disposition_attachment = lambda *a, **k: ''

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    for name, mod in {
        PKG: _package(PKG),
        f'{PKG}.api': _package(f'{PKG}.api'),
        f'{PKG}.api.v2': _package(f'{PKG}.api.v2', [str(PLUGIN_ROOT / 'api' / 'v2')]),
        f'{PKG}.models': _package(f'{PKG}.models'),
        f'{PKG}.models.skill': models_skill,
        f'{PKG}.models.pd': _package(f'{PKG}.models.pd'),
        f'{PKG}.models.pd.skill': pd_skill,
        f'{PKG}.models.pd.skill_version': pd_skill_version,
        f'{PKG}.models.pd.icon_meta': pd_icon_meta,
        f'{PKG}.utils': _package(f'{PKG}.utils'),
        f'{PKG}.utils.skill_utils': skill_utils,
        f'{PKG}.utils.skill_export_import': export_import,
        f'{PKG}.utils.export_import_utils': export_import_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

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


@pytest.fixture()
def api(monkeypatch):
    saved = {k: v for k, v in sys.modules.items()
             if k in ('tools', 'flask') or k.startswith(PKG)}
    loaded = _install_package()
    _Request.args = _Args()
    _Request.files = {}
    _Request.form = {}
    for bucket in CALLS.values():
        bucket.clear()
    yield loaded
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask'):
        sys.modules.pop(key, None)
    sys.modules.update(saved)


def _meta(api, module, method_name):
    return getattr(api[module].PromptLibAPI, method_name)._openapi


def _icon_handler(api, tmp_path):
    """A stand-in `self` for upload_skill_icon: real icon dir, recorded rpc calls."""
    call = types.SimpleNamespace(
        social_save_image=lambda *a, **k: {'ok': True, 'data': {'name': 'icon.png'}},
        social_update_icon_with_entity=lambda *a, **k: CALLS['icon_bind'].append(a),
    )
    module = types.SimpleNamespace(
        skill_icon_path=tmp_path,
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(call=call)),
    )
    return types.SimpleNamespace(module=module)


@pytest.mark.parametrize('module', MODULES)
def test_the_selector_is_an_optional_query_parameter(api, module):
    method_name, selector = SELECTORS[module]
    param, = [p for p in _meta(api, module, method_name)['parameters']
              if p['name'] == selector]
    assert param['in'] == 'query'
    assert param['required'] is False


@pytest.mark.parametrize('module', MODULES)
def test_the_description_names_the_selector_as_a_query_parameter(api, module):
    """#6411 is a description-vs-spec contradiction, so the wording is part of the fix."""
    method_name, selector = SELECTORS[module]
    description = _meta(api, module, method_name)['description']
    assert 'path segment' not in description, f'{module} still calls the selector a path segment'
    assert f'{selector} query parameter' in description


@pytest.mark.parametrize('module', MODULES)
def test_the_selector_query_parameter_is_documented(api, module):
    method_name, selector = SELECTORS[module]
    param, = [p for p in _meta(api, module, method_name)['parameters']
              if p['name'] == selector]
    assert param.get('description'), f'{module}.{selector} has no description'


@pytest.mark.parametrize('module', MODULES)
def test_the_selector_method_is_pinned_to_the_short_path(api, module):
    method_name, _ = SELECTORS[module]
    assert _meta(api, module, method_name)['path_suffix_override'] == (
        api['upload_skill_icon'].ICONS_PATH if module == 'upload_skill_icon'
        else api['skill'].SKILL_PATH
    )


@pytest.mark.parametrize('module', MODULES)
def test_no_method_declares_an_optional_path_parameter(api, module):
    for method_name in ('get', 'post', 'put', 'delete'):
        method = getattr(api[module].PromptLibAPI, method_name, None)
        if method is None or not hasattr(method, '_openapi'):
            continue
        for param in method._openapi['parameters']:
            assert not (param['in'] == 'path' and param.get('required') is False), (
                f'{module}.{method_name} declares optional path parameter {param["name"]}'
            )


@pytest.mark.parametrize('module', MODULES)
def test_every_pinned_path_is_actually_routable(api, module):
    """`path_suffix_override` decides the path the spec publishes; `url_params` decides what Flask
    serves. They are independent literals - skill_export imports SKILL_PATH from skill.py while
    declaring its own url_params - so they can drift and send every spec-driven caller to a 404.
    The registry-based assertions cannot see this: the registry is built from the override."""
    routes = api[module].API.url_params
    checked = 0
    for method_name in ('get', 'post', 'put', 'delete'):
        method = getattr(api[module].PromptLibAPI, method_name, None)
        if method is None or not hasattr(method, '_openapi'):
            continue
        override = method._openapi.get('path_suffix_override')
        if override is None:
            continue
        checked += 1
        assert override in routes, (
            f'{module}.{method_name} publishes {override!r}, which no url_params entry registers'
        )
    assert checked, f'{module} pinned no paths, so the assertion above never ran'


@pytest.mark.parametrize('module', MODULES)
def test_the_versioned_route_is_still_registered_for_the_ui(api, module):
    assert VERSIONED_ROUTE[module] in api[module].API.url_params


@pytest.mark.parametrize('module,bucket', [
    ('skill_export', 'export'),
    ('skill_export_fork', 'fork'),
])
def test_export_reads_the_version_from_the_query(api, module, bucket):
    _Request.args = _Args(version_id='8')
    api[module].PromptLibAPI.get(None, project_id=1, skill_id=2)
    assert CALLS[bucket][-1]['version_id'] == 8


@pytest.mark.parametrize('module,bucket', [
    ('skill_export', 'export'),
    ('skill_export_fork', 'fork'),
])
def test_export_prefers_the_path_segment_over_the_query(api, module, bucket):
    _Request.args = _Args(version_id='8')
    api[module].PromptLibAPI.get(None, project_id=1, skill_id=2, version_id=3)
    assert CALLS[bucket][-1]['version_id'] == 3


@pytest.mark.parametrize('module,bucket', [
    ('skill_export', 'export'),
    ('skill_export_fork', 'fork'),
])
def test_export_without_a_version_falls_back_to_the_default(api, module, bucket):
    api[module].PromptLibAPI.get(None, project_id=1, skill_id=2)
    assert CALLS[bucket][-1]['version_id'] is None


@pytest.mark.parametrize('module', ('skill_export', 'skill_export_fork'))
def test_export_rejects_a_non_integer_version(api, module):
    _Request.args = _Args(version_id='abc')
    body, status = api[module].PromptLibAPI.get(None, project_id=1, skill_id=2)
    assert status == 400
    assert 'version_id' in body['error']


def test_icon_upload_binds_the_version_from_the_query(api, tmp_path):
    _Request.args = _Args(skill_version_id='8')
    _Request.files = {'file': _FakeFile()}
    api['upload_skill_icon'].PromptLibAPI.post(_icon_handler(api, tmp_path), project_id=1)
    assert CALLS['icon_bind'], 'the icon was never bound to a skill version'
    assert CALLS['icon_bind'][-1][1] == 8


def test_icon_upload_prefers_the_path_segment_over_the_query(api, tmp_path):
    _Request.args = _Args(skill_version_id='8')
    _Request.files = {'file': _FakeFile()}
    api['upload_skill_icon'].PromptLibAPI.post(
        _icon_handler(api, tmp_path), project_id=1, skill_version_id=3)
    assert CALLS['icon_bind'][-1][1] == 3


def test_icon_upload_without_a_version_binds_nothing(api, tmp_path):
    _Request.files = {'file': _FakeFile()}
    api['upload_skill_icon'].PromptLibAPI.post(_icon_handler(api, tmp_path), project_id=1)
    assert CALLS['icon_bind'] == []


def test_icon_upload_rejects_a_non_integer_version(api, tmp_path):
    _Request.args = _Args(skill_version_id='abc')
    _Request.files = {'file': _FakeFile()}
    body, status = api['upload_skill_icon'].PromptLibAPI.post(
        _icon_handler(api, tmp_path), project_id=1)
    assert status == 400
    assert 'skill_version_id' in body['error']


def test_resolve_version_id_names_the_selector_it_was_given(api):
    """The 400 must name the key the caller actually sent, not a hard-coded 'version_id'."""
    _Request.args = _Args(skill_version_id='abc')
    value, error = api['skill'].resolve_version_id(None, 'skill_version_id')
    assert value is None
    assert error == ({'error': 'skill_version_id must be an integer'}, 400)


def test_resolve_version_id_ignores_a_differently_named_query_key(api):
    _Request.args = _Args(version_id='8')
    assert api['skill'].resolve_version_id(None, 'skill_version_id') == (None, None)
