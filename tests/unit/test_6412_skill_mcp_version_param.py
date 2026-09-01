"""Issue #6412 - `patch_elitea_core_skill` failed on every call.

`skill.py` registers two Flask patterns for one resource (with and without a trailing
`<int:version_id>`), but OpenAPI/MCP can publish only one path per method. The picker in
`shared/tools/openapi_tools.py` chose the pattern by matching the *handler signature*, and
`patch`'s signature names `version_id` purely to power its own reject-guard - so the published
tool demanded the one segment the handler refuses, and no working call existed.

The fix pins every method to the version-less route with `path_suffix_override` and moves version
selection to a query argument or the request body. These tests pin the decorator contract that
makes that work; `tests/integration/test_6412_skill_openapi_registration.py` pins the schema the
real registration code derives from it.
"""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'skillpkg_6412_unit'

METHODS = ('get', 'post', 'put', 'patch', 'delete')


class _Args(dict):
    pass


class _Request:
    args = _Args()
    json = {}
    method = 'GET'
    path = '/api/v2/elitea_core/skill/prompt_lib/1/2'


LOGGED = {'warning': []}


class _Payload:
    """Stand-in for the pydantic request models; detaches on PATCH, no version on PUT."""

    has_relation = False
    entity_version_id = 1
    entity_type = 'agent'
    skill_version_id = None
    version = None

    @classmethod
    def model_validate(cls, _payload):
        return cls()


def _install_package():
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        warning=lambda *a, **k: LOGGED['warning'].append(a),
        info=lambda *a, **k: None, error=lambda *a, **k: None,
        debug=lambda *a, **k: None, exception=lambda *a, **k: None,
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

        @staticmethod
        def with_modes(params):
            return params

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
    pd_skill.SkillUpdateModel = type('SkillUpdateModel', (_Payload,), {})
    pd_skill.SkillUpdateRelationModel = type('SkillUpdateRelationModel', (_Payload,), {})

    pd_skill_version = types.ModuleType(f'{PKG}.models.pd.skill_version')
    pd_skill_version.SkillVersionCreateModel = type('SkillVersionCreateModel', (_Payload,), {})
    pd_skill_version.SkillVersionUpdateModel = type('SkillVersionUpdateModel', (_Payload,), {})

    skill_utils = types.ModuleType(f'{PKG}.utils.skill_utils')
    for name in ('update_skill', 'delete_skill', 'create_skill_version',
                 'update_skill_version', 'delete_skill_version', 'get_skill_version_by_id',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: {'id': 1})
    skill_utils.get_skill_details = lambda *a, **k: {'data': {'id': 1}}
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


@pytest.fixture()
def skill_api(monkeypatch):
    saved = {k: v for k, v in sys.modules.items()
             if k == 'tools' or k == 'flask' or k.startswith(PKG)}
    module = _install_package()
    _Request.args = _Args()
    LOGGED['warning'].clear()
    yield module
    for key in [k for k in sys.modules if k.startswith(PKG)]:
        del sys.modules[key]
    for key in ('tools', 'flask'):
        sys.modules.pop(key, None)
    sys.modules.update(saved)


def _meta(skill_api, method_name):
    return getattr(skill_api.PromptLibAPI, method_name)._openapi


def _params(skill_api, method_name, name):
    return [p for p in _meta(skill_api, method_name)['parameters'] if p['name'] == name]


def test_every_method_is_pinned_to_the_versionless_path(skill_api):
    for method_name in METHODS:
        assert _meta(skill_api, method_name)['path_suffix_override'] == skill_api.SKILL_PATH


def test_no_method_declares_an_optional_path_parameter(skill_api):
    for method_name in METHODS:
        for param in _meta(skill_api, method_name)['parameters']:
            assert not (param['in'] == 'path' and param.get('required') is False), (
                f"{method_name} declares optional path parameter {param['name']}"
            )


def test_relation_patch_does_not_take_a_version_id(skill_api):
    assert _params(skill_api, 'patch', 'version_id') == []


def test_metadata_put_selects_the_version_through_the_body(skill_api):
    assert _params(skill_api, 'put', 'version_id') == []
    assert _meta(skill_api, 'put')['request_body'] is sys.modules[
        f'{PKG}.models.pd.skill'].SkillUpdateModel


@pytest.mark.parametrize('method_name', ('get', 'delete'))
def test_version_id_is_an_optional_query_parameter(skill_api, method_name):
    param, = _params(skill_api, method_name, 'version_id')
    assert param['in'] == 'query'
    assert param['required'] is False


def test_versioned_route_is_still_registered_for_the_ui(skill_api):
    assert '<int:project_id>/<int:skill_id>/<int:version_id>' in skill_api.API.url_params


def test_patch_still_rejects_the_versioned_route(skill_api):
    body, status = skill_api.PromptLibAPI.patch(None, project_id=1, skill_id=2, version_id=5)
    assert status == 400
    assert 'skill_version_id' in body['error']


def test_resolve_version_id_prefers_the_path_argument(skill_api):
    _Request.args = _Args(version_id='9')
    assert skill_api.resolve_version_id(3) == (3, None)


@pytest.mark.parametrize('query, expected', [
    ({}, None),
    ({'version_id': ''}, None),
    ({'version_id': '8'}, 8),
])
def test_resolve_version_id_reads_the_query(skill_api, query, expected):
    _Request.args = _Args(query)
    assert skill_api.resolve_version_id(None) == (expected, None)


def test_resolve_version_id_rejects_a_non_integer(skill_api):
    _Request.args = _Args(version_id='abc')
    version_id, error = skill_api.resolve_version_id(None)
    assert version_id is None
    assert error[1] == 400


def test_delete_refuses_a_mistyped_version_selector(skill_api):
    """`DELETE ...?versionId=7` must not quietly become "delete the skill and every version" —
    the only method where ignoring an unrecognised parameter is irreversible."""
    _Request.args = _Args(versionId='7')
    body, status = skill_api.PromptLibAPI.delete(None, project_id=1, skill_id=2)
    assert status == 400
    assert 'versionId' in body['error']


@pytest.mark.parametrize('method_name, query', [
    ('get', 'versionId'),
    ('put', 'versionId'),
    ('patch', 'versionId'),
    ('put', 'version_id'),
    ('patch', 'version_id'),
])
def test_reversible_methods_ignore_an_unsupported_query_param_and_log_it(
        skill_api, method_name, query):
    """PUT and PATCH select the version through the body, so a query `version_id` is as
    unsupported there as a typo - logged and ignored, never a new 400."""
    _Request.args = _Args({query: '7'})
    _, status = getattr(skill_api.PromptLibAPI, method_name)(None, project_id=1, skill_id=2)
    assert status == 200
    assert any(query in str(call) for call in LOGGED['warning'])


def test_an_ignored_query_key_cannot_forge_a_log_line(skill_api):
    """Werkzeug percent-decodes query keys, so `?%0A...=1` puts a real newline in the key and
    would let a caller write their own WARNING lines into the log."""
    _Request.args = _Args({'\n2026-09-01 ERROR forged': '1'})
    skill_api.PromptLibAPI.get(None, project_id=1, skill_id=2)

    message, args = LOGGED['warning'][0][0], LOGGED['warning'][0][1:]
    rendered = message % args
    assert '\n' not in rendered
    assert '\\n2026-09-01 ERROR forged' in rendered


def test_delete_does_not_echo_control_characters_from_a_rejected_key(skill_api):
    _Request.args = _Args({'\nforged': '1'})
    body, status = skill_api.PromptLibAPI.delete(None, project_id=1, skill_id=2)
    assert status == 400
    assert '\n' not in body['error']


def test_a_recognised_query_param_is_not_logged(skill_api):
    _Request.args = _Args(version_id='7')
    skill_api.PromptLibAPI.get(None, project_id=1, skill_id=2)
    assert LOGGED['warning'] == []


