"""A Dataverse credential has no saved token endpoint, so the token proxy serves only its tenant's endpoint.

Without the pin, anyone who can edit a toolkit could aim the proxy at their own host and receive the
client secret of a shared Dataverse credential. The real proxy handler and token helpers run against a
recorded token endpoint (the #6688 harness); the toolkit row and the caller's rights are stubbed.
"""
import types

import pytest

from integration import test_6688_oauth_proxy_resource_indicator as harness

proxy = harness.proxy

TENANT_ENDPOINT = 'https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token'
FOREIGN_ENDPOINT = 'https://attacker.example/token'
STORED_SECRET = 'stored-client-secret'
TOOLKIT_EDIT = 'models.applications.tool.patch'

DATAVERSE = {'dataverse_configuration': {
    'url': 'https://org.crm4.dynamics.com', 'tenant_id': 'tenant-1',
    'client_id': 'cid', 'client_secret': STORED_SECRET,
}}


class _Session:
    def __init__(self, toolkit):
        self.toolkit = toolkit

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def query(self, *_args):
        return self

    def filter(self, *_args):
        return self

    def first(self):
        return self.toolkit


@pytest.fixture
def toolkit(monkeypatch, proxy):
    state = types.SimpleNamespace(settings=DATAVERSE, permissions=[])
    handler = proxy.handler
    monkeypatch.setattr(handler, 'db', types.SimpleNamespace(
        with_project_schema_session=lambda _project_id: _Session(types.SimpleNamespace(settings=state.settings))))
    monkeypatch.setattr(handler, 'EliteATool', types.SimpleNamespace(id=1))
    monkeypatch.setattr(handler, 'auth', types.SimpleNamespace(
        resolve_permissions=lambda **_kwargs: state.permissions, current_user=lambda: {'id': 1}))
    monkeypatch.setattr(handler, 'VaultClient', lambda _project_id: types.SimpleNamespace(
        unsecret=lambda value: 'vault-value' if value == '{{secret.shared}}' else value))
    state.proxy = proxy
    return state


def _exchange(state, token_endpoint, extra=None):
    state.proxy.request.json = {'token_endpoint': token_endpoint, 'toolkit_id': 1, 'code': 'c',
                                'redirect_uri': 'http://localhost/cb', **(extra or {})}
    return state.proxy.api.post(5)


def test_the_stored_secret_goes_to_the_endpoint_of_the_credential_tenant(toolkit):
    body, status = _exchange(toolkit, TENANT_ENDPOINT)

    assert status == 200
    (url, form), = toolkit.proxy.posted
    assert url == TENANT_ENDPOINT
    assert (form['client_id'], form['client_secret']) == ('cid', STORED_SECRET)


@pytest.mark.parametrize('permissions', [[], [TOOLKIT_EDIT]], ids=['viewer', 'toolkit_editor'])
def test_a_foreign_endpoint_is_refused_whatever_the_rights_of_the_caller(toolkit, permissions):
    toolkit.permissions = permissions

    body, status = _exchange(toolkit, FOREIGN_ENDPOINT)

    assert status == 400
    assert body['error'] == 'invalid_token_endpoint'
    assert STORED_SECRET not in str(body)
    assert toolkit.proxy.posted == []


def test_a_foreign_endpoint_is_refused_on_refresh_too(toolkit):
    toolkit.permissions = [TOOLKIT_EDIT]

    body, status = _exchange(toolkit, FOREIGN_ENDPOINT, {'grant_type': 'refresh_token', 'refresh_token': 'r'})

    assert (status, body['error']) == (400, 'invalid_token_endpoint')
    assert toolkit.proxy.posted == []


def test_a_vault_reference_is_not_resolved_for_a_foreign_endpoint(toolkit):
    toolkit.permissions = [TOOLKIT_EDIT]

    _, status = _exchange(toolkit, FOREIGN_ENDPOINT, {'client_secret': '{{secret.shared}}'})

    assert status == 400
    assert toolkit.proxy.posted == []


def test_a_viewer_cannot_resolve_a_vault_reference_even_for_the_tenant_endpoint(toolkit):
    body, status = _exchange(toolkit, TENANT_ENDPOINT, {'client_secret': '{{secret.shared}}'})

    assert (status, body['error']) == (403, 'access_denied')
    assert toolkit.proxy.posted == []


def test_a_credential_without_a_tenant_serves_no_endpoint(toolkit):
    toolkit.settings = {'dataverse_configuration': {'client_id': 'cid', 'client_secret': STORED_SECRET}}
    toolkit.permissions = [TOOLKIT_EDIT]

    _, status = _exchange(toolkit, TENANT_ENDPOINT)

    assert status == 400
    assert toolkit.proxy.posted == []


def test_another_sign_in_type_keeps_the_rights_rule(toolkit):
    toolkit.settings = {'teams_configuration': {'client_id': 'cid', 'client_secret': STORED_SECRET}}
    toolkit.permissions = [TOOLKIT_EDIT]

    _, status = _exchange(toolkit, FOREIGN_ENDPOINT)

    assert status == 200
    assert toolkit.proxy.posted[0][1]['client_secret'] == STORED_SECRET


def test_another_sign_in_type_still_withholds_the_secret_from_a_viewer(toolkit):
    toolkit.settings = {'teams_configuration': {'client_id': 'cid', 'client_secret': STORED_SECRET}}

    _, status = _exchange(toolkit, FOREIGN_ENDPOINT)

    assert status == 200
    assert 'client_secret' not in toolkit.proxy.posted[0][1]
