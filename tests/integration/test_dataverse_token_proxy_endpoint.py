"""The token proxy serves a toolkit credential only at the sign-in address of its own identity provider.

A credential (Dataverse, Teams, Outlook, SharePoint, MS Graph, OpenAPI) can be shared in a project, and the
proxy loads its client secret from the toolkit row. A caller who can edit toolkits, an admin included, must
not be able to point the proxy at another host and read the secret from the request it posts. The real
proxy handler and token helpers run against a recorded token endpoint (the harness of the
resource-indicator tests); only the toolkit row and the caller's rights are faked.
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
def id_bound(monkeypatch):
    """The request model reads the database id bound from the schedule model; the harness stubs the package."""
    harness._load(monkeypatch, f'{harness.PKG}.models.pd.schedule_list',
                  harness.PLUGIN_ROOT / 'models' / 'pd' / 'schedule_list.py')


@pytest.fixture
def toolkits(id_bound, proxy, monkeypatch):
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

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
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


SOME_GUID = '11111111-2222-3333-4444-555555555555'
MS_GUID_ENDPOINT = f'https://login.microsoftonline.com/{SOME_GUID}/oauth2/v2.0/token'
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


@pytest.fixture
def shared_credential(toolkits, monkeypatch):
    """Expanding a titled credential overlays the shared values on the inline dict, as the real one does."""
    shared = {'client_id': 'cid', 'client_secret': 'real-secret', 'token_url': OPENAPI_TOKEN_URL}

    def expand(payload, **_kwargs):
        payload.update(shared)

    monkeypatch.setattr(toolkits.proxy.handler, 'expand_configuration', expand)
    return shared


@ROLES
@pytest.mark.parametrize('planted', ['token_url', 'token_endpoint'])
@BOTH_GRANTS
def test_an_endpoint_planted_in_the_inline_credential_is_not_the_saved_one(
        toolkits, shared_credential, permissions, planted, grant):
    shared_credential.pop('token_url')  # the shared credential saves no endpoint of its own
    toolkits.settings = {'openapi_configuration': {'elitea_title': 'shared', planted: 'https://attacker.example/token'}}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, 'https://attacker.example/token', grant=grant)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert toolkits.proxy.posted == []


@ROLES
@BOTH_GRANTS
def test_an_inline_client_next_to_a_credential_reference_does_not_make_its_endpoint_the_saved_one(
        toolkits, shared_credential, permissions, grant):
    # a toolkit of a type with no credential field in its schema is saved with whatever its settings hold
    shared_credential.pop('token_url')
    toolkits.settings = {'openapi_configuration': {
        'elitea_title': 'shared', 'client_id': 'planted-cid', 'client_secret': '{{secret.shared}}',
        'token_url': 'https://attacker.example/token'}}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, 'https://attacker.example/token', grant=grant)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert toolkits.proxy.posted == []


def test_a_shared_credential_still_signs_in_at_its_own_saved_endpoint(toolkits, shared_credential):
    toolkits.settings = {'openapi_configuration': {'elitea_title': 'shared', 'token_url': 'https://attacker.example/t'}}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, OPENAPI_TOKEN_URL)

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_secret']) == (OPENAPI_TOKEN_URL, 'real-secret')


@ROLES
def test_a_sharepoint_app_sign_in_replaces_a_foreign_endpoint_with_the_app_endpoint(
        toolkits, monkeypatch, permissions):
    app = ('app-cid', 'app-secret', MS_GUID_ENDPOINT, 'offline_access Sites.Read.All')
    monkeypatch.setattr(toolkits.proxy.handler, '_resolve_oauth_app_client', lambda *_: app)
    toolkits.settings = {'sharepoint_configuration': {'oauth_app_id': 'app-1'}}
    toolkits.permissions = permissions

    _, status = _sign_in(toolkits, 'https://attacker.example/token',
                         grant={'oauth_app_id': 'app-1', 'credential_type': 'sharepoint'})

    assert status == 200
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_id'], form['client_secret']) == (MS_GUID_ENDPOINT, 'app-cid', 'app-secret')


@ROLES
@pytest.mark.parametrize('app_endpoint', [
    'https://login.microsoftonline.us/tenant-1/oauth2/v2.0/token',
    'https://idp.example/custom/authority/token',
])
@pytest.mark.parametrize('sent', ['', 'https://attacker.example/token'])
def test_an_admin_app_is_served_at_its_own_endpoint_whatever_host_it_is_on(
        toolkits, monkeypatch, permissions, app_endpoint, sent):
    app = ('app-cid', 'app-secret', app_endpoint, 'offline_access Sites.Read.All')
    monkeypatch.setattr(toolkits.proxy.handler, '_resolve_oauth_app_client', lambda *_: app)
    toolkits.settings = {'sharepoint_configuration': {'oauth_app_id': 'app-1'}}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, sent, grant={'oauth_app_id': 'app-1', 'credential_type': 'sharepoint'})

    assert status == 200, body
    (url, form), = toolkits.proxy.posted
    assert (url, form['client_id'], form['client_secret']) == (app_endpoint, 'app-cid', 'app-secret')


@ROLES
@pytest.mark.parametrize('planted', ['oauth_app_id', 'oauth_discovery_endpoint'])
@BOTH_GRANTS
def test_a_planted_key_does_not_turn_an_app_only_shared_credential_into_a_sign_in(
        toolkits, monkeypatch, permissions, planted, grant):
    def expand(payload, **_kwargs):  # the shared credential is ACS app-only: a client and no sign-in
        payload.update({'client_id': 'cid', 'client_secret': 'real-secret'})

    monkeypatch.setattr(toolkits.proxy.handler, 'expand_configuration', expand)
    toolkits.settings = {'sharepoint_configuration': {'elitea_title': 'shared', planted: 'planted'}}
    toolkits.permissions = permissions

    body, status = _sign_in(toolkits, MS_GUID_ENDPOINT, grant=grant)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert toolkits.proxy.posted == []


def test_a_shared_credential_keeps_its_uuid_reference_when_expanded(toolkits, monkeypatch):
    seen = []

    def expand(payload, **_kwargs):
        seen.append(dict(payload))
        payload.update({'client_id': 'cid', 'client_secret': 'real-secret',
                        'oauth_discovery_endpoint': 'https://login.microsoftonline.com/common/v2.0'})

    monkeypatch.setattr(toolkits.proxy.handler, 'expand_configuration', expand)
    toolkits.settings = {'teams_configuration': {
        'elitea_title': 'shared', 'private': False, 'configuration_uuid': 'u-1', 'configuration_project_id': 7,
        'token_url': 'https://attacker.example/token', 'client_secret': ''}}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, MS_GUID_ENDPOINT)

    assert status == 200
    assert seen == [{'elitea_title': 'shared', 'private': False, 'configuration_uuid': 'u-1',
                     'configuration_project_id': 7}]
    (_, form), = toolkits.proxy.posted
    assert form['client_secret'] == 'real-secret'


def test_a_toolkit_without_an_oauth_credential_keeps_the_edit_right_rule(toolkits):
    toolkits.settings = {'client_id': 'cid', 'client_secret': 'real-secret'}
    toolkits.permissions = EDITOR_OR_ADMIN

    _, status = _sign_in(toolkits, 'https://mcp.example/token')

    assert status == 200
    (_, form), = toolkits.proxy.posted
    assert form['client_secret'] == 'real-secret'


# A sign-in that names a saved credential and no toolkit: the server loads it and decides the endpoint by its type
CREDENTIAL_UUID = '11111111-aaaa-4bbb-8ccc-222222222222'
CREDENTIAL_TYPES = {
    **{name: (name, config[f'{name}_configuration']) for name, config in MICROSOFT_CREDENTIALS.items()},
    'openapi': ('openapi', OPENAPI_CREDENTIAL['openapi_configuration']),
    'dataverse': ('dataverse', DATAVERSE),
}
EVERY_CREDENTIAL = pytest.mark.parametrize('credential_type, saved', CREDENTIAL_TYPES.values(),
                                           ids=list(CREDENTIAL_TYPES))


@pytest.fixture
def credential(toolkits, monkeypatch):
    """The saved credential the reference names; ``seen`` records what the handler asked the expansion for."""
    state = types.SimpleNamespace(type='openapi', data=dict(OPENAPI_CREDENTIAL['openapi_configuration']),
                                  error=None, seen=[], proxy=toolkits.proxy, toolkits=toolkits)
    handler = toolkits.proxy.handler

    def expand(payload, **kwargs):
        state.seen.append((dict(payload), kwargs))
        if state.error:
            raise state.error
        payload.update(state.data)
        payload['configuration_type'] = state.type

    monkeypatch.setattr(handler, 'expand_configuration', expand)
    monkeypatch.setattr(handler.auth, 'current_user', lambda: {'id': 3}, raising=False)
    return state


def _credential_sign_in(credential, token_endpoint, permissions=(), grant=None, **body):
    credential.toolkits.permissions = list(permissions)
    proxy = credential.proxy
    proxy.request.json = {'token_endpoint': token_endpoint, 'code': 'auth-code', 'redirect_uri': 'http://localhost/cb',
                          'configuration_uuid': CREDENTIAL_UUID, **(grant or {}), **body}
    return proxy.api.post(2)


def _use(credential, credential_type, saved):
    credential.type, credential.data = credential_type, dict(saved)


@pytest.fixture
def counting_vault(toolkits, monkeypatch):
    """A vault that resolves ``{{secret.shared}}`` and counts every call."""
    calls = []

    def unsecret(value):
        calls.append(value)
        return 'vault-secret' if value == '{{secret.shared}}' else value

    monkeypatch.setattr(toolkits.proxy.handler, 'VaultClient', lambda project_id: types.SimpleNamespace(unsecret=unsecret))
    return calls


@ROLES
def test_a_vault_reference_without_a_toolkit_or_credential_is_refused_for_every_role(toolkits, counting_vault, permissions):
    toolkits.permissions = permissions
    toolkits.proxy.request.json = {'token_endpoint': 'https://attacker.example/token', 'code': 'auth-code',
                                   'redirect_uri': 'http://localhost/cb', 'client_secret': '{{secret.shared}}'}

    body, status = toolkits.proxy.api.post(2)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert toolkits.proxy.posted == [] and counting_vault == []


@pytest.mark.parametrize('credential_type, saved, endpoint, permissions', [
    ('teams', MICROSOFT_CREDENTIALS['teams']['teams_configuration'], MS_GUID_ENDPOINT, EDITOR_OR_ADMIN),
    ('openapi', OPENAPI_CREDENTIAL['openapi_configuration'], OPENAPI_TOKEN_URL, []),
    ('openapi', OPENAPI_CREDENTIAL['openapi_configuration'], OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN),
    ('dataverse', DATAVERSE, OWN_ENDPOINT, []),
    ('dataverse', DATAVERSE, OWN_ENDPOINT, EDITOR_OR_ADMIN),
], ids=['teams editor', 'openapi viewer', 'openapi editor', 'dataverse viewer', 'dataverse editor'])
@pytest.mark.parametrize('sent_type', [False, True], ids=['no type sent', 'same type sent'])
def test_a_credential_reference_signs_in_with_the_stored_secret_at_its_own_endpoint(
        credential, credential_type, saved, endpoint, permissions, sent_type):
    _use(credential, credential_type, saved)
    extra = {'credential_type': credential_type} if sent_type else {}

    body, status = _credential_sign_in(credential, endpoint, permissions, **extra)

    assert status == 200, body
    (url, form), = credential.proxy.posted
    assert (url, form['client_id'], form['client_secret']) == (endpoint, 'cid', 'real-secret')


@EVERY_CREDENTIAL
@ROLES
@BOTH_GRANTS
@pytest.mark.parametrize('endpoint', FOREIGN_ENDPOINTS)
def test_a_credential_reference_refuses_a_foreign_endpoint_and_posts_nothing(
        credential, credential_type, saved, permissions, grant, endpoint):
    _use(credential, credential_type, saved)

    body, status = _credential_sign_in(credential, endpoint, permissions, grant=grant)

    assert (status, body) == (403, {'error': 'access_denied', 'error_description': REFUSED_TEXT}), body
    assert credential.proxy.posted == []


@pytest.mark.parametrize('error', [LookupError('no such credential in /tmp/secret-path'), ValueError('Recursion error')])
@ROLES
def test_a_credential_the_caller_cannot_see_is_not_found(credential, permissions, error):
    credential.error = error

    body, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, permissions)

    assert status == 404, body
    assert 'secret-path' not in str(body) and 'Recursion' not in str(body)
    assert credential.proxy.posted == []


@pytest.mark.parametrize('sent', ['teams', 'sharepoint', 'dataverse', ''])
def test_a_credential_type_that_differs_from_the_request_is_refused(credential, sent):
    body, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN, credential_type=sent)

    assert status == 400, body
    assert body['error'] == 'invalid_request'
    assert credential.proxy.posted == []


@pytest.mark.parametrize('project_id', [0, -1, 2**31, 'abc', [7], {'a': 1}])
def test_a_credential_project_id_outside_the_database_range_is_a_bad_request(credential, project_id):
    body, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN, configuration_project_id=project_id)

    assert status == 400, body
    assert credential.proxy.posted == [] and credential.seen == []


@pytest.mark.parametrize('extra, payload', [
    ({}, {'configuration_uuid': CREDENTIAL_UUID}),
    # a project id also marks the lookup private: only then does the expansion count the caller's personal project as visible
    ({'configuration_project_id': 2}, {'configuration_uuid': CREDENTIAL_UUID, 'configuration_project_id': 2, 'private': True}),
    ({'configuration_project_id': 9}, {'configuration_uuid': CREDENTIAL_UUID, 'configuration_project_id': 9, 'private': True}),
], ids=['none', 'this project', 'another project'])
def test_the_credential_is_looked_up_for_this_project_and_the_current_user(credential, extra, payload):
    _credential_sign_in(credential, OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN, **extra)

    assert credential.seen == [(payload, {'current_project_id': 2, 'user_id': 3, 'unsecret': True})]


def test_a_vault_reference_next_to_a_credential_reference_is_not_resolved(credential, counting_vault):
    body, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN, client_secret='{{secret.shared}}')

    assert status == 200, body
    (_, form), = credential.proxy.posted
    assert form['client_secret'] == 'real-secret'
    assert counting_vault == []


def test_a_viewer_gets_no_stored_secret_of_a_microsoft_credential(credential):
    _use(credential, 'teams', MICROSOFT_CREDENTIALS['teams']['teams_configuration'])

    _, status = _credential_sign_in(credential, MS_GUID_ENDPOINT, [])

    assert status == 200
    (url, form), = credential.proxy.posted
    assert url == MS_GUID_ENDPOINT and 'client_secret' not in form


@ROLES
def test_a_credential_of_another_type_signs_in_only_at_its_saved_endpoint(credential, permissions):
    _use(credential, 'jira', {'client_id': 'cid', 'client_secret': 'real-secret', 'token_url': OPENAPI_TOKEN_URL})

    _, own = _credential_sign_in(credential, OPENAPI_TOKEN_URL, permissions)
    _, foreign = _credential_sign_in(credential, 'https://attacker.example/token', permissions)

    assert (own, foreign) == (200, 403)
    assert [(url, form['client_secret']) for url, form in credential.proxy.posted] == [(OPENAPI_TOKEN_URL, 'real-secret')]


@ROLES
@pytest.mark.parametrize('saved', [{'client_id': 'cid', 'client_secret': 'real-secret'}, {}],
                         ids=['no saved endpoint', 'empty credential'])
def test_a_credential_of_another_type_with_no_saved_endpoint_signs_in_nowhere(credential, permissions, saved):
    _use(credential, 'jira', saved)

    _, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, permissions)

    assert status == 403 and credential.proxy.posted == []


def test_a_dcr_client_next_to_a_credential_reference_keeps_its_own_secret_and_loads_no_stored_one(credential):
    _, status = _credential_sign_in(credential, OPENAPI_TOKEN_URL, EDITOR_OR_ADMIN,
                                    client_id='dcr-client', client_secret='dcr-secret', used_dcr=True)

    assert status == 200
    (_, form), = credential.proxy.posted
    assert (form['client_id'], form['client_secret']) == ('dcr-client', 'dcr-secret')
