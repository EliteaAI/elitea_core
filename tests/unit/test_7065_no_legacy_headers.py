"""Issue #7065 - PATCH version needs the unsecret permission and no code uses X-SECRET / X-USERSESSION.

PATCH version returns a version with toolkit credentials unsecreted. #7064 made the platform send
expanded details with every predict, so the runtime no longer calls it for healthy trees. #7065 then:
  * requires `configuration.secrets.secret.unsecret` on PATCH (editor, admin, system; not viewer),
    instead of `models.applications.version.details`, which viewers have;
  * takes the acting user from the request's own auth, not from an X-USERSESSION header;
  * removes X-SECRET everywhere (`check_secret_header`, the public_application skills gate and
    the predict payload headers).

These are checked on the source: the handlers pull in most of the plugin, and the properties here
are about which permission a decorator names and which headers code reads, not runtime state.

Run via:
    python tests/run_tests.py unit/test_7065_no_legacy_headers.py -v
"""
import ast
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
LEGACY = ('X-SECRET', 'X-USERSESSION', 'XSECRET', 'secrets_header_value', 'check_secret_header')


def _plugin_sources():
    for path in sorted(PLUGIN_ROOT.rglob('*.py')):
        rel = path.relative_to(PLUGIN_ROOT)
        if rel.parts[0] in ('tests', 'static', 'node_modules') or '__pycache__' in rel.parts:
            continue
        yield rel, path.read_text(encoding='utf-8')


def _method(path, class_name, name):
    tree = ast.parse((PLUGIN_ROOT / path).read_text(encoding='utf-8'))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    return next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)


def _literal_check_api(func):
    # recommended_roles uses c.MODE names, so read the permission list and the viewer flags by hand
    for deco in func.decorator_list:
        if not (isinstance(deco, ast.Call) and getattr(deco.func, 'attr', None) == 'check_api'):
            continue
        spec = deco.args[0]
        out = {}
        for key, value in zip(spec.keys, spec.values):
            if key.value == 'permissions':
                out['permissions'] = [e.value for e in value.elts]
            if key.value == 'recommended_roles':
                out['viewer'] = []
                for roles in value.values:
                    for role_key, role_value in zip(roles.keys, roles.values):
                        if role_key.value == 'viewer':
                            out['viewer'].append(role_value.value)
        return out
    return None


@pytest.mark.parametrize('rel,source', list(_plugin_sources()), ids=lambda v: str(v) if isinstance(v, pathlib.Path) else '')
def test_no_plugin_code_mentions_the_legacy_headers(rel, source):
    found = [name for name in LEGACY if name in source]
    assert not found, f"{rel} still mentions {found}"


def test_the_secret_header_helper_is_gone():
    assert not (PLUGIN_ROOT / 'utils' / 'secrets.py').exists()


def test_patch_version_requires_the_unsecret_permission_and_excludes_viewers():
    spec = _literal_check_api(_method('api/v2/version.py', 'PromptLibAPI', 'patch'))

    assert spec['permissions'] == ['configuration.secrets.secret.unsecret']
    assert spec['viewer'] and not any(spec['viewer'])


def test_patch_version_keeps_the_write_folder_check():
    patch = _method('api/v2/version.py', 'PromptLibAPI', 'patch')
    folder = [d for d in patch.decorator_list
              if isinstance(d, ast.Call) and getattr(d.func, 'id', None) == 'require_folder_access']

    assert folder and any(k.arg == 'write' and k.value.value is True for k in folder[0].keywords)


def test_patch_version_takes_the_user_from_the_request_auth():
    source = ast.unparse(_method('api/v2/version.py', 'PromptLibAPI', 'patch'))

    assert 'auth.current_user()' in source
    assert 'request.headers' not in source
    assert 'get_referenced_auth_context' not in source


def test_get_version_is_unchanged_for_viewers():
    # GET returns the plain version (no unsecreting); elitea-mcp-client and the UI use it
    spec = _literal_check_api(_method('api/v2/version.py', 'PromptLibAPI', 'get'))

    assert spec['permissions'] == ['models.applications.version.details']
    assert any(spec['viewer'])


def test_runtime_skills_are_served_on_the_query_flag_alone():
    source = ast.unparse(_method('api/v2/public_application.py', 'PromptLibAPI', 'get'))

    assert "serve_runtime_skills = request.args.get('runtime') == 'true'" in source
    assert 'request.headers' not in source
