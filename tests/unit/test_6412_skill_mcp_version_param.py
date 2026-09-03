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

# Which skill_utils writer ran, and with what - a status code alone cannot tell the version
# branch from the metadata branch, since both return 200.
CALLS = {'update_skill': [], 'update_skill_version': []}


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


def _with_modes(url_params):
    """Order-preserving twin of `api_tools.with_modes`, which returns `list(set(...))`.

    Faithful expansion matters: an identity stub would hide whether a pinned
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
    for name in ('delete_skill', 'create_skill_version', 'delete_skill_version',
                 'attach_skill_to_agent', 'detach_skill_from_agent'):
        setattr(skill_utils, name, lambda *a, **k: {'id': 1})

    def _record(name):
        def call(*args, **kwargs):
            CALLS[name].append(kwargs)
            return {'id': 1}
        return call

    skill_utils.update_skill = _record('update_skill')
    skill_utils.update_skill_version = _record('update_skill_version')
    # Returns a SkillVersion row, and callers read `.id` off it - not a dict. Echoes the id it was
    # asked for so a test can prove which version the handler targeted. Deliberately a minimal
    # class rather than SimpleNamespace, which would answer to any attribute typo.
    skill_utils.get_skill_version_by_id = (
        lambda *a, **k: type('SkillVersion', (), {'id': k.get('version_id')})())
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
    _Request.json = {}
    LOGGED['warning'].clear()
    for calls in CALLS.values():
        calls.clear()
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
        # .get, not [...]: an unpinned method is the very thing under test, and the fake
        # register_openapi in this harness omits kwargs the decorator did not pass.
        assert _meta(skill_api, method_name).get('path_suffix_override') == skill_api.SKILL_PATH, (
            f'{method_name} is not pinned to SKILL_PATH'
        )


def test_no_method_declares_an_optional_path_parameter(skill_api):
    for method_name in METHODS:
        for param in _meta(skill_api, method_name)['parameters']:
            assert not (param['in'] == 'path' and param.get('required') is False), (
                f"{method_name} declares optional path parameter {param['name']}"
            )


def test_relation_patch_does_not_take_a_version_id(skill_api):
    assert _params(skill_api, 'patch', 'version_id') == []


def test_metadata_put_selects_the_version_through_the_body(skill_api):
    """The nested body remains PUT's documented request shape; #6411 added the query selector as
    a second, documented way in, so `version_id` is no longer absent from the parameters - but it
    must never come back as a *path* parameter, which is what #6412 was about."""
    param, = _params(skill_api, 'put', 'version_id')
    assert param['in'] == 'query'
    assert param['required'] is False
    assert _meta(skill_api, 'put')['request_body'] is sys.modules[
        f'{PKG}.models.pd.skill'].SkillUpdateModel


@pytest.mark.parametrize('method_name', ('get', 'delete'))
def test_version_id_is_an_optional_query_parameter(skill_api, method_name):
    param, = _params(skill_api, method_name, 'version_id')
    assert param['in'] == 'query'
    assert param['required'] is False


def test_every_pinned_path_is_actually_routable(skill_api):
    """SKILL_PATH decides the path the spec publishes; `url_params` decides what Flask serves.
    They are independent literals in this file (and skill_export/skill_export_fork import
    SKILL_PATH while declaring their own), so they can drift and send every spec-driven caller to
    a 404. The registry assertions cannot see it - the registry is built from the override.

    Pinning is not assumed here - a method that legitimately does not pin is skipped, and
    `test_every_method_is_pinned_to_the_versionless_path` is what requires skill.py to pin at all.
    """
    routes = skill_api.API.url_params
    checked = 0
    for method_name in METHODS:
        override = _meta(skill_api, method_name).get('path_suffix_override')
        if override is None:
            continue
        checked += 1
        assert override in routes, (
            f'{method_name} publishes {override!r}, which no url_params entry registers'
        )
    assert checked, 'no method pinned a path, so the assertion above never ran'


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
    ('patch', 'version_id'),
])
def test_reversible_methods_ignore_an_unsupported_query_param_and_log_it(
        skill_api, method_name, query):
    """PATCH selects the version through the body, so a query `version_id` is as unsupported
    there as a typo - logged and ignored, never a new 400. It writes no version content, so
    ignoring the key cannot corrupt one.

    `('put', 'version_id')` was dropped from this list reviewing #6411: PUT *does* write version
    content, and ignoring the key rewrote the default version while the caller believed they had
    targeted another. PUT now *honours* it, like get/delete and the skill_export siblings - see
    `test_put_honours_a_query_version_selector`. A typo'd key on PUT is still ignored, which is
    what the `versionId` case above pins."""
    _Request.args = _Args({query: '7'})
    _, status = getattr(skill_api.PromptLibAPI, method_name)(None, project_id=1, skill_id=2)
    assert status == 200
    assert any(query in str(call) for call in LOGGED['warning'])


def test_put_honours_a_query_version_selector(skill_api):
    """Changed reviewing #6411. `version_id` is a live query selector on get/delete and on
    skill_export/skill_export_fork, so a caller reaching for it on PUT is making a reasonable
    assumption - and used to silently get the default version rewritten instead.

    Asserting the status is not enough: the unfixed path fell through to the metadata branch,
    which also returns 200. What distinguishes them is *which writer ran and on which version*."""
    _Request.args = _Args(version_id='8')
    _, status = skill_api.PromptLibAPI.put(None, project_id=1, skill_id=2)
    assert status == 200
    assert LOGGED['warning'] == [], 'a recognised selector must not be logged as unsupported'
    assert [c['version_id'] for c in CALLS['update_skill_version']] == [8]
    assert CALLS['update_skill'] == [], 'the default version must not have been rewritten'


def test_put_still_writes_when_no_version_selector_is_sent(skill_api):
    _Request.args = _Args()
    _, status = skill_api.PromptLibAPI.put(None, project_id=1, skill_id=2)
    assert status == 200
    assert len(CALLS['update_skill']) == 1
    assert CALLS['update_skill_version'] == []


def test_put_honours_a_path_version_selector(skill_api):
    """The shape EliteaUI's Compare-versions dialog sends - the path form must keep targeting the
    version it names, independently of the query selector added for #6411."""
    _, status = skill_api.PromptLibAPI.put(None, project_id=1, skill_id=2, version_id=8)
    assert status == 200
    assert [c['version_id'] for c in CALLS['update_skill_version']] == [8]
    assert CALLS['update_skill'] == []


def test_put_accepts_the_compare_dialog_body_verbatim(skill_api):
    """The exact body the Compare-versions dialog sends, captured off the wire in a Playwright
    sweep. `status`, `author_id`, `author` and `created_at` are not fields of
    SkillVersionUpdateModel - they survive today only because the model ignores extras. Pinned
    because tightening that model to extra="forbid" would 400 every Compare-dialog save."""
    _Request.json = {
        'id': 209, 'name': 'base', 'instructions': 'text', 'status': 'draft', 'author_id': 3,
        'author': {'id': 3, 'email': 'admin@centry.user', 'name': 'admin@centry.user',
                   'avatar': None},
        'tags': [], 'created_at': '2026-09-03T06:14:58.695546', 'meta': {},
    }
    _, status = skill_api.PromptLibAPI.put(None, project_id=1, skill_id=2, version_id=209)
    assert status == 200


def test_put_rejects_a_non_integer_query_version_selector(skill_api):
    _Request.args = _Args(version_id='abc')
    body, status = skill_api.PromptLibAPI.put(None, project_id=1, skill_id=2)
    assert status == 400
    assert 'version_id' in body['error']


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


