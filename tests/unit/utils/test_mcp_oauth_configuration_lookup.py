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
    spec = importlib.util.spec_from_file_location(
        'mcp_oauth_under_test', PLUGIN_ROOT / 'utils' / 'mcp_oauth.py'
    )
    module = importlib.util.module_from_spec(spec)
    # The helpers under test never touch the network; `requests` is only needed by the
    # token functions and is not in tests/requirements-dev.txt, so stub it while loading.
    stub_requests = 'requests' not in sys.modules and importlib.util.find_spec('requests') is None
    if stub_requests:
        sys.modules['requests'] = types.ModuleType('requests')
    try:
        spec.loader.exec_module(module)
    finally:
        if stub_requests:
            sys.modules.pop('requests', None)
    return module


@pytest.mark.parametrize('key', [
    'sharepoint_configuration',
    'openapi_configuration',
    'teams_configuration',
    'outlook_configuration',
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


TENANT_ENDPOINT = 'https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token'


def _dataverse(tenant_id='tenant-1'):
    return {'dataverse_configuration': {'tenant_id': tenant_id, 'client_secret': 's'}}


@pytest.mark.parametrize('endpoint', [TENANT_ENDPOINT, f'{TENANT_ENDPOINT}/'])
def test_a_dataverse_credential_accepts_the_endpoint_of_its_tenant(mcp_oauth, endpoint):
    assert mcp_oauth.is_foreign_token_endpoint(_dataverse(), endpoint) is False


@pytest.mark.parametrize('endpoint', [
    'https://attacker.example/token',
    'https://login.microsoftonline.com/other-tenant/oauth2/v2.0/token',
    'https://login.microsoftonline.com.attacker.example/tenant-1/oauth2/v2.0/token',
    'http://login.microsoftonline.com/tenant-1/oauth2/v2.0/token',
    'https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token?x=1',
    '',
    None,
])
def test_a_dataverse_credential_refuses_any_other_endpoint(mcp_oauth, endpoint):
    assert mcp_oauth.is_foreign_token_endpoint(_dataverse(), endpoint) is True


@pytest.mark.parametrize('tenant', [None, '', '  ', 'a/../b', 'tenant-1/x', 'ten ant', 7])
def test_a_dataverse_credential_without_a_usable_tenant_matches_no_endpoint(mcp_oauth, tenant):
    settings = _dataverse(tenant_id=tenant)

    assert mcp_oauth.is_foreign_token_endpoint(settings, TENANT_ENDPOINT) is True


def test_the_endpoint_of_a_dataverse_credential_is_built_from_its_tenant(mcp_oauth):
    assert mcp_oauth.dataverse_token_endpoint(_dataverse()) == TENANT_ENDPOINT
    assert mcp_oauth.dataverse_token_endpoint(_dataverse(tenant_id='')) is None
    assert mcp_oauth.dataverse_token_endpoint({'teams_configuration': {'tenant_id': 'tenant-1'}}) is None


def test_the_tenant_is_trimmed_before_the_endpoint_is_built(mcp_oauth):
    assert mcp_oauth.is_foreign_token_endpoint(_dataverse(tenant_id=' tenant-1 '), TENANT_ENDPOINT) is False


@pytest.mark.parametrize('settings', [
    {},
    {'dataverse_configuration': None},
    {'dataverse_configuration': {}},
    {'sharepoint_configuration': {'client_secret': 's'}},
    {'openapi_configuration': {'client_secret': 's', 'token_endpoint': 'https://idp.example/token'}},
    {'teams_configuration': {'client_secret': 's'}},
    {'outlook_configuration': {'client_secret': 's'}},
])
def test_other_sign_in_types_keep_any_endpoint(mcp_oauth, settings):
    assert mcp_oauth.is_foreign_token_endpoint(settings, 'https://attacker.example/token') is False
