"""MCP OAuth proxy must resolve the OAuth client from every delegated-OAuth credential type.

Teams/Outlook toolkits keep client_id/client_secret in ``teams_configuration`` /
``outlook_configuration``. The chat auth card strips the secret, so the proxy is the only
place it can come from; missing those keys sent no secret and Entra answered AADSTS7000218.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def mcp_oauth():
    spec = importlib.util.spec_from_file_location(
        'mcp_oauth_under_test', PLUGIN_ROOT / 'utils' / 'mcp_oauth.py'
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('key', [
    'sharepoint_configuration',
    'openapi_configuration',
    'teams_configuration',
    'outlook_configuration',
])
def test_every_delegated_oauth_configuration_supplies_the_secret(mcp_oauth, key):
    settings = {key: {'client_id': 'cid', 'client_secret': 'real-secret', 'scopes': ['User.Read']}}
    sources = [settings, *mcp_oauth.get_oauth_configurations(settings)]

    assert mcp_oauth.pick_oauth_setting(sources, 'client_secret') == 'real-secret'
    assert mcp_oauth.pick_oauth_setting(sources, 'client_id') == 'cid'


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
