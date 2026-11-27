"""The token proxy serves a toolkit credential only at the sign-in address of its own identity provider.

A credential (Dataverse, Teams, Outlook, SharePoint, MS Graph, OpenAPI) can be shared in a project, and the
proxy loads its client secret from the toolkit row. A caller who can edit toolkits, an admin included, must
not be able to point the proxy at another host and read the secret from the request it posts. The real proxy handler and token helpers run against a recorded token endpoint
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


MS_GUID_ENDPOINT = 'https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/oauth2/v2.0/token'
MS_DOMAIN_ENDPOINT = 'https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0/oauth2/token'
OPENAPI_TOKEN_URL = 'https://idp.example/oauth/token'
REFUSED_TEXT = 'token_endpoint is not the sign-in address of this credential'
FOREIGN_ENDPOINTS = [
    'https://attacker.example/token',
    'https://webhook.site/6f1d0c2e-0000-4000-8000-000000000000',
    'https://login.microsoftonline.com.attacker.example/common/oauth2/v2.0/token',
    'https://user@login.microsoftonline.com/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com:8443/common/oauth2/v2.0/token',
    'http://login.microsoftonline.com/common/oauth2/v2.0/token',
]
# The credential as the form saves it: Microsoft types keep a discovery endpoint and no token endpoint
MICROSOFT_CREDENTIALS = {
    'teams': {'teams_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                      'oauth_discovery_endpoint': 'https://login.microsoftonline.com/common/v2.0'}},
    'outlook': {'outlook_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                          'oauth_discovery_endpoint': 'https://login.microsoftonline.com/common/v2.0'}},
    'ms_graph': {'ms_graph_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                            'oauth_discovery_endpoint': 'https://login.microsoftonline.com/common/v2.0'}},
    'sharepoint': {'sharepoint_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                                'oauth_discovery_endpoint': 'https://login.microsoftonline.com/common/v2.0'}},
}
OPENAPI_CREDENTIAL = {'openapi_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                                'token_url': OPENAPI_TOKEN_URL}}
EDITOR_OR_ADMIN = [PATCH]  # editor and admin hold the same right here
ROLES = pytest.mark.parametrize('permissions', [[], EDITOR_OR_ADMIN], ids=['viewer', 'editor or admin'])
CREDENTIALS = pytest.mark.parametrize('settings', [*MICROSOFT_CREDENTIALS.values(), OPENAPI_CREDENTIAL],
                                      ids=[*MICROSOFT_CREDENTIALS, 'openapi'])
BOTH_GRANTS = pytest.mark.parametrize('grant', [
    {},
    {'grant_type': 'refresh_token', 'refresh_token': 'refresh-me', 'code': None, 'redirect_uri': None},
], ids=['code grant', 'refresh grant'])


@CREDENTIALS
@ROLES
@BOTH_GRANTS
@pytest.mark.parametrize('endpoint', FOREIGN_ENDPOINTS)
def test_a_foreign_endpoint_is_refused_for_every_credential_type_and_posts_nothing(
        toolkits, settings, permissions, grant, endpoint):
    toolkits.settings = {key: dict(value) for key, value in settings.items()}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, endpoint, grant=grant)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert toolkits.proxy.posted == []


@pytest.mark.parametrize('settings', MICROSOFT_CREDENTIALS.values(), ids=list(MICROSOFT_CREDENTIALS))
@pytest.mark.parametrize('endpoint', [MS_GUID_ENDPOINT, MS_DOMAIN_ENDPOINT])
def test_a_microsoft_credential_gets_its_secret_at_the_microsoft_login_host_when_the_caller_can_edit(
        toolkits, settings, endpoint):
    toolkits.settings = {key: dict(value) for key, value in settings.items()}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, endpoint)

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_id'], form['client_secret']) == (endpoint, 'cid', 'real-secret')


def test_a_viewer_at_the_microsoft_login_host_gets_no_stored_secret(toolkits):
    toolkits.settings = {key: dict(value) for key, value in MICROSOFT_CREDENTIALS['teams'].items()}

    _, status = _sign_in(toolkits, MS_GUID_ENDPOINT)

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert url == MS_GUID_ENDPOINT and 'client_secret' not in form


@ROLES
def test_an_openapi_credential_gets_its_secret_only_at_its_saved_token_url(toolkits, permissions):
    toolkits.settings = {key: dict(value) for key, value in OPENAPI_CREDENTIAL.items()}
    toolkits.permissions = permissions

    _, status = _sign_in(toolkits, OPENAPI_TOKEN_URL)

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_secret']) == (OPENAPI_TOKEN_URL, 'real-secret')


@ROLES
def test_an_openapi_credential_with_only_a_discovery_endpoint_is_refused(toolkits, permissions):
    toolkits.settings = {'openapi_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                                   'oauth_discovery_endpoint': 'https://idp.example/.well-known'}}
    toolkits.permissions = permissions

    _, status = _sign_in(toolkits, OPENAPI_TOKEN_URL)

    assert status == 403 and toolkits.proxy.posted == []


@ROLES
@pytest.mark.parametrize('endpoint', [OPENAPI_TOKEN_URL, MS_GUID_ENDPOINT, 'https://attacker.example/token'])
def test_a_sharepoint_app_only_credential_is_refused_for_any_endpoint(toolkits, permissions, endpoint):
    toolkits.settings = {'sharepoint_configuration': {'client_id': 'cid', 'client_secret': 'real-secret',
                                                      'site_url': 'https://contoso.sharepoint.com/sites/x'}}
    toolkits.permissions = permissions

    _, status = _sign_in(toolkits, endpoint)

    assert status == 403 and toolkits.proxy.posted == []


@pytest.mark.parametrize('settings', [*MICROSOFT_CREDENTIALS.values(), OPENAPI_CREDENTIAL, {'dataverse_configuration': DATAVERSE}],
                         ids=[*MICROSOFT_CREDENTIALS, 'openapi', 'dataverse'])
def test_a_vault_reference_is_not_resolved_for_a_foreign_endpoint(toolkits, vault, settings):
    toolkits.settings = {key: dict(value) for key, value in settings.items()}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, 'https://attacker.example/token', client_secret='{{secret.shared}}')

    assert status == 403 and toolkits.proxy.posted == []


def test_the_teams_secret_goes_to_the_microsoft_host_and_never_to_a_foreign_one(toolkits):
    toolkits.settings = {key: dict(value) for key, value in MICROSOFT_CREDENTIALS['teams'].items()}
    toolkits.permissions = EDITOR_OR_ADMIN

    _sign_in(toolkits, MS_GUID_ENDPOINT)
    toolkits.permissions = []
    _, refused_for_viewer = _sign_in(toolkits, 'https://attacker.example/token')
    toolkits.permissions = EDITOR_OR_ADMIN
    _, refused_for_editor = _sign_in(toolkits, 'https://attacker.example/token')

    assert (refused_for_viewer, refused_for_editor) == (403, 403)
    assert [(url, form.get('client_secret')) for url, form in toolkits.proxy.posted] == [
        (MS_GUID_ENDPOINT, 'real-secret'),
    ]


def test_a_toolkit_without_an_oauth_credential_keeps_the_edit_right_rule(toolkits):
    toolkits.settings = {'client_id': 'cid', 'client_secret': 'real-secret'}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, 'https://mcp.example/token')

    assert status == 200
    (_, form), = toolkits.proxy.posted
    assert form['client_secret'] == 'real-secret'
