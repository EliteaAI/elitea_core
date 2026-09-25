"""Issue #6688 - the MCP OAuth token proxy forwards the RFC 8707 ``resource`` indicator.

monday.com's authorization server rejects a flow without ``resource`` ("resource parameter is
required"), and the MCP authorization spec requires it on the token request too. The real proxy
handler and the real token helpers run here against a recorded token endpoint; only the pylon
runtime around them is stubbed. Re-breaks if the request model drops the field, the handler stops
passing it, or either helper leaves it out of the form body.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

ROOT = 'oauthproxy_6688_root'
PKG = f'{ROOT}.elitea_core'

TOKEN_ENDPOINT = 'https://auth.example.test/oauth/token'
RESOURCE = 'https://mcp.example.test/mcp'


class _Response:
    ok = True
    text = ''

    def json(self):
        return {'access_token': 'issued', 'token_type': 'Bearer'}


class _Request:
    json = {}


def _passthrough(*_args, **_kwargs):
    return lambda func: func


def _package(monkeypatch, name, path=None):
    module = types.ModuleType(name)
    module.__path__ = [str(path)] if path else []
    monkeypatch.setitem(sys.modules, name, module)
    return module


def _module(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    monkeypatch.setitem(sys.modules, name, module)
    return module


def _load(monkeypatch, name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _purge_stubbed(prefix):
    for name in [n for n, m in sys.modules.items() if n.split('.')[0] == prefix and getattr(m, '__file__', None) is None]:
        del sys.modules[name]


@pytest.fixture
def proxy(monkeypatch):
    _purge_stubbed('pydantic')
    _purge_stubbed('requests')

    request = _Request()
    _module(monkeypatch, 'flask', request=request)
    _module(
        monkeypatch, 'tools',
        api_tools=types.SimpleNamespace(
            APIModeHandler=object,
            APIBase=object,
            endpoint_metrics=lambda func: func,
            with_modes=lambda params: params,
        ),
        auth=types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=_passthrough)),
        config=types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default'),
        db=None,
        VaultClient=lambda project_id: types.SimpleNamespace(unsecret=lambda value: value),
        register_openapi=_passthrough,
    )
    _package(monkeypatch, 'pylon')
    _package(monkeypatch, 'pylon.core')
    _module(monkeypatch, 'pylon.core.tools', log=types.SimpleNamespace(debug=print, error=print, warning=print))

    _package(monkeypatch, ROOT)
    _package(monkeypatch, f'{ROOT}.configurations')
    _module(monkeypatch, f'{ROOT}.configurations.utils', expand_configuration=None)
    _package(monkeypatch, PKG)
    _package(monkeypatch, f'{PKG}.api')
    _package(monkeypatch, f'{PKG}.api.v2', PLUGIN_ROOT / 'api' / 'v2')
    _package(monkeypatch, f'{PKG}.models')
    _package(monkeypatch, f'{PKG}.models.pd')
    _module(monkeypatch, f'{PKG}.models.elitea_tools', EliteATool=None)
    _load(monkeypatch, f'{PKG}.models.pd.mcp_oauth', PLUGIN_ROOT / 'models' / 'pd' / 'mcp_oauth.py')
    _package(monkeypatch, f'{PKG}.utils')
    _module(monkeypatch, f'{PKG}.utils.mcp_config', is_mcp_exposure_enabled=lambda: True)
    oauth_utils = _load(monkeypatch, f'{PKG}.utils.mcp_oauth', PLUGIN_ROOT / 'utils' / 'mcp_oauth.py')

    posted = []

    def record(url, data=None, **_kwargs):
        posted.append((url, dict(data)))
        return _Response()

    monkeypatch.setattr(oauth_utils.requests, 'post', record)
    handler = _load(monkeypatch, f'{PKG}.api.v2.mcp_oauth_proxy', PLUGIN_ROOT / 'api' / 'v2' / 'mcp_oauth_proxy.py')
    return types.SimpleNamespace(api=handler.ProjectAPI(), request=request, posted=posted)


def _post(proxy, **body):
    proxy.request.json = {'token_endpoint': TOKEN_ENDPOINT, 'client_id': 'dcr-client', 'used_dcr': True, **body}
    return proxy.api.post(2)


def test_the_code_exchange_sends_the_resource_to_the_token_endpoint(proxy):
    result = _post(proxy, code='auth-code', redirect_uri='http://localhost/app/mcp-auth-callback',
                   code_verifier='verifier', resource=RESOURCE)

    assert result == ({'access_token': 'issued', 'token_type': 'Bearer'}, 200)
    assert proxy.posted == [(TOKEN_ENDPOINT, {
        'grant_type': 'authorization_code',
        'code': 'auth-code',
        'redirect_uri': 'http://localhost/app/mcp-auth-callback',
        'client_id': 'dcr-client',
        'code_verifier': 'verifier',
        'resource': RESOURCE,
    })]


def test_the_refresh_sends_the_resource_to_the_token_endpoint(proxy):
    _post(proxy, grant_type='refresh_token', refresh_token='refresh-me', resource=RESOURCE)

    assert proxy.posted == [(TOKEN_ENDPOINT, {
        'grant_type': 'refresh_token',
        'refresh_token': 'refresh-me',
        'client_id': 'dcr-client',
        'resource': RESOURCE,
    })]


@pytest.mark.parametrize('body', [
    {'code': 'auth-code', 'redirect_uri': 'http://localhost/app/mcp-auth-callback'},
    {'grant_type': 'refresh_token', 'refresh_token': 'refresh-me'},
])
def test_a_request_without_a_resource_sends_none(proxy, body):
    _post(proxy, **body)

    assert 'resource' not in proxy.posted[0][1]
