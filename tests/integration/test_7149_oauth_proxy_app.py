"""Issue #7149 - a Default-app credential signs in without the browser ever holding the secret.

The browser sends ``oauth_app_id``; the proxy resolves the admin app's client ID and secret and
posts them only to the app's own authority. Reuses the #6688 harness: the real proxy handler and
token helpers run against a recorded token endpoint.
"""
import types

import pytest

from integration.test_6688_oauth_proxy_resource_indicator import ROOT, _module, proxy  # noqa: F401 (fixture)

AUTHORITY = 'https://login.microsoftonline.com/organizations'
TOKEN_ENDPOINT = f'{AUTHORITY}/oauth2/v2.0/token'


class Unavailable(LookupError):
    pass


@pytest.fixture
def entra(monkeypatch, proxy):  # noqa: F811
    calls = []

    def resolve_app_client(app_id, project_id):
        calls.append((app_id, project_id))
        if app_id != 'app-1':
            raise Unavailable('The Microsoft Entra ID app selected for this credential no longer exists')
        return {'client_id': 'admin-client', 'client_secret': 'admin-secret', 'token_endpoint': TOKEN_ENDPOINT}

    _module(monkeypatch, f'{ROOT}.configurations.oauth_apps', resolve_app_client=resolve_app_client)
    return types.SimpleNamespace(proxy=proxy, calls=calls)


def _exchange(proxy, **body):
    proxy.request.json = {'grant_type': 'authorization_code', 'code': 'c', 'redirect_uri': 'http://localhost/cb',
                          'code_verifier': 'v', **body}
    return proxy.api.post(5)


def test_the_app_secret_is_resolved_server_side(entra):
    result = _exchange(entra.proxy, token_endpoint=TOKEN_ENDPOINT, oauth_app_id='app-1')

    assert result[1] == 200
    assert entra.calls == [('app-1', 5)]
    (url, form), = entra.proxy.posted
    assert url == TOKEN_ENDPOINT
    assert form['client_id'] == 'admin-client'
    assert form['client_secret'] == 'admin-secret'


def test_a_browser_supplied_client_is_ignored(entra):
    _exchange(entra.proxy, token_endpoint=TOKEN_ENDPOINT, oauth_app_id='app-1',
              client_id='spoofed', client_secret='spoofed')
    form = entra.proxy.posted[0][1]
    assert (form['client_id'], form['client_secret']) == ('admin-client', 'admin-secret')


def test_the_secret_never_goes_to_another_endpoint(entra):
    result = _exchange(entra.proxy, token_endpoint='https://attacker.example/token', oauth_app_id='app-1')
    assert result[1] == 403
    assert entra.proxy.posted == []


def test_an_unavailable_app_is_reported(entra):
    body, status = _exchange(entra.proxy, token_endpoint=TOKEN_ENDPOINT, oauth_app_id='gone')
    assert status == 400
    assert body['error'] == 'app_unavailable'
    assert entra.proxy.posted == []
