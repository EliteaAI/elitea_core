"""The token proxy serves a Dataverse toolkit only at the Entra endpoint of its own tenant.

A Dataverse credential can be shared in a project, and the proxy loads its client secret from the toolkit
row. A caller who can edit toolkits must not be able to point the proxy at another host and read the secret
from the request it posts. The real proxy handler and token helpers run against a recorded token endpoint
(the harness of the resource-indicator tests); only the toolkit row and the caller's rights are faked.
"""
import contextlib
import types

import pytest
from integration import test_6688_oauth_proxy_resource_indicator as harness

proxy = harness.proxy

TENANT = 'tenant-1'
OWN_ENDPOINT = f'https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token'
DATAVERSE = {'url': 'https://org.crm4.dynamics.com', 'tenant_id': TENANT, 'client_id': 'cid',
             'client_secret': 'real-secret'}
PATCH = 'models.applications.tool.patch'


class _Session:
    def __init__(self, toolkit):
        self.toolkit = toolkit

    def query(self, *_):
        return self

    def filter(self, *_):
        return self

    def first(self):
        return self.toolkit


@pytest.fixture
def toolkits(proxy, monkeypatch):
    """Serve one toolkit row; ``permissions`` is what the caller holds in the project."""
    state = types.SimpleNamespace(settings={}, permissions=[])

    @contextlib.contextmanager
    def project_session(_project_id):
        yield _Session(types.SimpleNamespace(settings=state.settings))

    handler = proxy.handler
    monkeypatch.setattr(handler, 'db', types.SimpleNamespace(with_project_schema_session=project_session))
    monkeypatch.setattr(handler, 'EliteATool', types.SimpleNamespace(id=1))
    monkeypatch.setattr(handler.auth, 'resolve_permissions', lambda mode, project_id: state.permissions, raising=False)
    state.proxy = proxy
    return state


def _sign_in(toolkits, token_endpoint, client_secret=None, grant=None):
    proxy = toolkits.proxy
    proxy.request.json = {'token_endpoint': token_endpoint, 'toolkit_id': 1, 'code': 'auth-code',
                          'redirect_uri': 'http://localhost/cb', 'client_secret': client_secret, **(grant or {})}
    return proxy.api.post(2)


def test_a_dataverse_toolkit_signs_in_at_its_own_tenant_endpoint_with_the_stored_secret(toolkits):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}

    _, status = _sign_in(toolkits, OWN_ENDPOINT)

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_id'], form['client_secret']) == (OWN_ENDPOINT, 'cid', 'real-secret')


@pytest.mark.parametrize('endpoint', [
    'https://attacker.example/token',
    'https://login.microsoftonline.com.attacker.example/tenant-1/oauth2/v2.0/token',
    'https://login.microsoftonline.com/other-tenant/oauth2/v2.0/token',
    'http://login.microsoftonline.com/tenant-1/oauth2/v2.0/token',
    'https://login.microsoftonline.com/tenant-1/oauth2/token',
])
@pytest.mark.parametrize('permissions', [[], [PATCH]], ids=['plain member', 'toolkit editor'])
def test_a_dataverse_toolkit_refuses_any_other_endpoint_whatever_the_callers_rights(toolkits, endpoint, permissions):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, endpoint)

    assert (status, body['error']) == (403, 'access_denied')
    assert toolkits.proxy.posted == []


@pytest.mark.parametrize('grant', [
    {},
    {'grant_type': 'refresh_token', 'refresh_token': 'refresh-me', 'code': None, 'redirect_uri': None},
])
def test_a_refused_dataverse_endpoint_gets_neither_a_code_nor_a_refresh_token(toolkits, grant):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = [PATCH]

    _sign_in(toolkits, 'https://attacker.example/token', grant=grant)

    assert toolkits.proxy.posted == []


@pytest.mark.parametrize('tenant', [None, '', 'a/../b', 'host.example/x', 'x y'])
def test_a_dataverse_credential_without_a_usable_tenant_has_no_acceptable_endpoint(toolkits, tenant):
    toolkits.settings = {'dataverse_configuration': {**DATAVERSE, 'tenant_id': tenant}}
    toolkits.permissions = [PATCH]

    _, status = _sign_in(toolkits, OWN_ENDPOINT)

    assert status == 403 and toolkits.proxy.posted == []


def test_a_vault_reference_for_a_dataverse_toolkit_is_not_resolved_for_another_endpoint(toolkits):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = [PATCH]

    _, status = _sign_in(toolkits, 'https://attacker.example/token', client_secret='{{secret.shared}}')

    assert status == 403 and toolkits.proxy.posted == []


@pytest.fixture
def vault(toolkits, monkeypatch):
    """The vault resolves ``{{secret.shared}}`` to a real value."""
    resolve = lambda value: 'vault-secret' if value == '{{secret.shared}}' else value
    monkeypatch.setattr(toolkits.proxy.handler, 'VaultClient',
                        lambda project_id: types.SimpleNamespace(unsecret=resolve))


def test_a_member_without_the_edit_right_gets_the_stored_secret_at_the_own_endpoint(toolkits):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = []

    _, status = _sign_in(toolkits, OWN_ENDPOINT)

    assert status == 200
    (_, form), = toolkits.proxy.posted
    assert form['client_secret'] == 'real-secret'


def test_a_member_without_the_edit_right_gets_no_vault_secret_even_at_the_own_endpoint(toolkits, vault):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = []

    body, status = _sign_in(toolkits, OWN_ENDPOINT, client_secret='{{secret.shared}}')

    assert (status, body['error']) == (403, 'access_denied')
    assert toolkits.proxy.posted == []


def test_a_toolkit_editor_resolves_a_vault_reference_at_the_own_endpoint(toolkits, vault):
    toolkits.settings = {'dataverse_configuration': dict(DATAVERSE)}
    toolkits.permissions = [PATCH]

    _, status = _sign_in(toolkits, OWN_ENDPOINT, client_secret='{{secret.shared}}')

    assert status == 200
    (_, form), = toolkits.proxy.posted
    assert form['client_secret'] == 'vault-secret'


def test_another_sign_in_type_keeps_the_saved_endpoint_or_toolkit_edit_rule(toolkits):
    saved = 'https://login.microsoftonline.com/org-tenant/oauth2/v2.0/token'
    toolkits.settings = {'teams_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                                 'token_endpoint': saved}}

    _sign_in(toolkits, saved)
    _sign_in(toolkits, 'https://attacker.example/token')
    toolkits.permissions = [PATCH]
    _sign_in(toolkits, 'https://attacker.example/token')

    assert [(url, form.get('client_secret')) for url, form in toolkits.proxy.posted] == [
        (saved, 'real-secret'),
        ('https://attacker.example/token', None),
        ('https://attacker.example/token', 'real-secret'),
    ]
