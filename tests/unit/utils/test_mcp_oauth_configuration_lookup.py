"""MCP OAuth proxy must resolve the OAuth client from every delegated-OAuth credential type.

Teams/Outlook toolkits keep client_id/client_secret in ``teams_configuration`` /
``outlook_configuration``. The chat auth card strips the secret, so the proxy is the only
place it can come from; missing those keys sent no secret and Entra answered AADSTS7000218.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def mcp_oauth():
    # mcp_oauth imports its sibling entra_obo with a relative import, so both load under one
    # throwaway package whose path is the utils folder (the package __init__ is never run).
    package = types.ModuleType('mcp_oauth_pkg_under_test')
    package.__path__ = [str(PLUGIN_ROOT / 'utils')]
    sys.modules[package.__name__] = package
    # The helpers under test never touch the network; `requests` is only needed by the
    # token functions and is not in tests/requirements-dev.txt, so stub it while loading.
    stub_requests = 'requests' not in sys.modules and importlib.util.find_spec('requests') is None
    if stub_requests:
        sys.modules['requests'] = types.SimpleNamespace(Response=object, Session=object)
    try:
        module = importlib.import_module(f'{package.__name__}.mcp_oauth')
    finally:
        if stub_requests:
            sys.modules.pop('requests', None)
        for name in [n for n in sys.modules if n.startswith(f'{package.__name__}.') or n == package.__name__]:
            sys.modules.pop(name, None)
    return module


@pytest.mark.parametrize('key', [
    'sharepoint_configuration',
    'openapi_configuration',
    'teams_configuration',
    'outlook_configuration',
    'ms_graph_configuration',
    'dataverse_configuration',
])
def test_every_delegated_oauth_configuration_supplies_the_secret(mcp_oauth, key):
    settings = {key: {'client_id': 'cid', 'client_secret': 'real-secret', 'scopes': ['User.Read']}}
    sources = [settings, *mcp_oauth.get_oauth_configurations(settings)]

    assert mcp_oauth.pick_oauth_setting(sources, 'client_secret') == 'real-secret'
    assert mcp_oauth.pick_oauth_setting(sources, 'client_id') == 'cid'


def test_dataverse_toolkit_settings_supply_the_stored_client_secret(mcp_oauth):
    settings = {
        'dataverse_configuration': {
            'url': 'https://org.crm4.dynamics.com',
            'tenant_id': 'tenant-1',
            'client_id': 'cid',
            'client_secret': 'real-secret',
        },
    }
    configs = mcp_oauth.get_oauth_configurations(settings)

    assert configs == [settings['dataverse_configuration']]
    assert mcp_oauth.pick_oauth_setting([settings, *configs], 'client_secret') == 'real-secret'


def test_configurations_are_returned_by_reference_so_expansion_is_seen(mcp_oauth):
    reference = {'elitea_title': 'teams_creds', 'private': True}
    settings = {'teams_configuration': reference}

    configs = mcp_oauth.get_oauth_configurations(settings)
    configs[0]['client_secret'] = 'expanded'

    assert settings['teams_configuration']['client_secret'] == 'expanded'


def test_toolkit_level_settings_take_priority_over_configurations(mcp_oauth):
    settings = {
        'client_secret': 'toolkit-level',
        'teams_configuration': {'client_secret': 'credential-level'},
    }
    sources = [settings, *mcp_oauth.get_oauth_configurations(settings)]

    assert mcp_oauth.pick_oauth_setting(sources, 'client_secret') == 'toolkit-level'


def test_non_dict_and_empty_configurations_are_ignored(mcp_oauth):
    settings = {
        'sharepoint_configuration': None,
        'openapi_configuration': 'not-a-dict',
        'teams_configuration': {},
        'outlook_configuration': {'client_secret': 's'},
    }

    assert mcp_oauth.get_oauth_configurations(settings) == [{'client_secret': 's'}]


def test_nothing_found_returns_none(mcp_oauth):
    assert mcp_oauth.pick_oauth_setting([{}, {'client_secret': ''}], 'client_secret') is None


def test_dataverse_token_endpoint_is_built_from_the_credential_tenant(mcp_oauth):
    settings = {'dataverse_configuration': {'tenant_id': ' contoso.onmicrosoft.com ', 'client_secret': 's'}}

    assert mcp_oauth.dataverse_token_endpoint(settings) == (
        'https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token')


@pytest.mark.parametrize('settings', [{}, {'teams_configuration': {'tenant_id': 't'}},
                                      {'dataverse_configuration': None}, {'dataverse_configuration': {}}])
def test_a_toolkit_without_a_dataverse_credential_has_no_pinned_endpoint(mcp_oauth, settings):
    assert mcp_oauth.dataverse_token_endpoint(settings) is None


@pytest.mark.parametrize('tenant', [None, '', '   ', '../x', 'a/b', 'evil.example/', '-lead', 'a b', 't' * 101])
def test_a_dataverse_credential_with_a_bad_tenant_pins_no_acceptable_endpoint(mcp_oauth, tenant):
    endpoint = mcp_oauth.dataverse_token_endpoint({'dataverse_configuration': {'tenant_id': tenant, 'url': 'u'}})

    assert endpoint == ''
    assert not mcp_oauth.is_same_endpoint(endpoint, endpoint)


@pytest.mark.parametrize('given, same', [
    ('https://login.microsoftonline.com/t/oauth2/v2.0/token', True),
    ('https://login.microsoftonline.com/t/oauth2/v2.0/token/', True),
    ('https://login.microsoftonline.com/T/oauth2/v2.0/token', False),
    ('https://login.microsoftonline.com/other/oauth2/v2.0/token', False),
    ('https://login.microsoftonline.com/t/oauth2/v2.0/token?x=1', False),
    ('', False),
    (None, False),
])
def test_an_endpoint_is_the_same_only_when_it_is_the_pinned_one(mcp_oauth, given, same):
    assert mcp_oauth.is_same_endpoint('https://login.microsoftonline.com/t/oauth2/v2.0/token', given) is same


SOME_GUID = '11111111-2222-3333-4444-555555555555'
MS_TOKEN = f'https://login.microsoftonline.com/{SOME_GUID}/oauth2/v2.0/token'


@pytest.mark.parametrize('url', [
    MS_TOKEN,
    'https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0/oauth2/token',
    'https://LOGIN.MICROSOFTONLINE.COM/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com/common/oauth2/v2.0/token/',
    'https://login.microsoftonline.com:443/common/oauth2/v2.0/token',
])
def test_the_microsoft_login_host_is_accepted_for_any_tenant_form(mcp_oauth, url):
    assert mcp_oauth.is_microsoft_login_url(url) is True


@pytest.mark.parametrize('url', [
    'http://login.microsoftonline.com/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com.attacker.example/common/oauth2/v2.0/token',
    'https://attacker.example/login.microsoftonline.com/token',
    'https://user@login.microsoftonline.com/common/oauth2/v2.0/token',
    'https://user:pw@login.microsoftonline.com/common/oauth2/v2.0/token',
    'https://attacker.example\\@login.microsoftonline.com/token',
    'https://login.microsoftonline.com:8443/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com:notaport/token',
    'https://login.microsoftonline.us/common/oauth2/v2.0/token',
    'https://login.chinacloudapi.cn/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com\x00.attacker.example/token',
    ' https://login.microsoftonline.com/common/oauth2/v2.0/token',
    'https://[::1/token',
    '', None, 5, ['https://login.microsoftonline.com/token'],
])
def test_any_other_login_address_is_refused(mcp_oauth, url):
    assert mcp_oauth.is_microsoft_login_url(url) is False


OPENAPI_SAVED = 'https://idp.example/oauth/token'
CREDENTIALS = {
    'teams': {'teams_configuration': {'client_id': 'cid', 'oauth_discovery_endpoint': 'https://x'}},
    'outlook': {'outlook_configuration': {'client_id': 'cid', 'oauth_discovery_endpoint': 'https://x'}},
    'ms_graph': {'ms_graph_configuration': {'client_id': 'cid', 'oauth_discovery_endpoint': 'https://x'}},
    'sharepoint discovery': {'sharepoint_configuration': {'oauth_discovery_endpoint': 'https://x'}},
    'sharepoint app': {'sharepoint_configuration': {'oauth_app_id': 'app-1', 'client_id': 'cid'}},
}


@pytest.mark.parametrize('settings', CREDENTIALS.values(), ids=list(CREDENTIALS))
def test_a_microsoft_credential_accepts_only_the_microsoft_login_host(mcp_oauth, settings):
    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is True
    assert mcp_oauth.token_endpoint_allowed(settings, 'https://attacker.example/token') is False
    assert mcp_oauth.token_endpoint_allowed(settings, None) is False


def test_a_sharepoint_app_only_credential_accepts_no_endpoint(mcp_oauth):
    settings = {'sharepoint_configuration': {'client_id': 'cid', 'client_secret': 's', 'site_url': 'https://x'}}

    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is False


@pytest.mark.parametrize('key', ['token_url', 'token_endpoint'])
def test_an_openapi_credential_accepts_only_its_saved_endpoint(mcp_oauth, key):
    settings = {'openapi_configuration': {key: OPENAPI_SAVED}}

    assert mcp_oauth.token_endpoint_allowed(settings, OPENAPI_SAVED) is True
    assert mcp_oauth.token_endpoint_allowed(settings, OPENAPI_SAVED + '/') is True
    assert mcp_oauth.token_endpoint_allowed(settings, 'https://attacker.example/token') is False
    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is False


@pytest.mark.parametrize('saved', [None, '', ['x'], 5])
def test_an_openapi_credential_with_no_usable_saved_endpoint_is_refused(mcp_oauth, saved):
    settings = {'openapi_configuration': {'oauth_discovery_endpoint': 'https://x', 'token_url': saved}}

    assert mcp_oauth.token_endpoint_allowed(settings, OPENAPI_SAVED) is False
    assert mcp_oauth.token_endpoint_allowed(settings, '') is False


def test_a_dataverse_credential_accepts_only_the_endpoint_of_its_tenant(mcp_oauth):
    settings = {'dataverse_configuration': {'tenant_id': 'tenant-1', 'client_id': 'cid'}}

    assert mcp_oauth.token_endpoint_allowed(
        settings, 'https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token') is True
    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is False


def test_a_key_with_no_rule_of_its_own_accepts_only_its_saved_endpoint(mcp_oauth, monkeypatch):
    monkeypatch.setattr(mcp_oauth, 'OAUTH_CONFIGURATION_KEYS', ('new_configuration',))
    settings = {'new_configuration': {'token_endpoint': OPENAPI_SAVED}}

    assert mcp_oauth.token_endpoint_allowed(settings, OPENAPI_SAVED) is True
    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is False
    assert mcp_oauth.token_endpoint_allowed({'new_configuration': {'client_id': 'c'}}, MS_TOKEN) is False


@pytest.mark.parametrize('settings', [{}, {'server_name': 'x'}, {'teams_configuration': None},
                                      {'teams_configuration': {}}, {'openapi_configuration': 'x'}])
def test_a_toolkit_without_an_oauth_credential_has_no_rule(mcp_oauth, settings):
    assert mcp_oauth.token_endpoint_allowed(settings, 'https://attacker.example/token') is None


def test_every_credential_of_a_toolkit_must_accept_the_endpoint(mcp_oauth):
    settings = {'teams_configuration': {'client_id': 'c'},
                'openapi_configuration': {'token_url': OPENAPI_SAVED}}

    assert mcp_oauth.token_endpoint_allowed(settings, OPENAPI_SAVED) is False
    assert mcp_oauth.token_endpoint_allowed(settings, MS_TOKEN) is False
