"""The one rule for the Microsoft login host, used by the on-behalf-of exchange and the MCP token proxy."""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
SOME_GUID = '11111111-2222-3333-4444-555555555555'


@pytest.fixture(scope='module')
def entra_host():
    # the module has no imports of the plugin, so it loads on its own
    spec = importlib.util.spec_from_file_location('entra_host_under_test', PLUGIN_ROOT / 'utils' / 'entra_host.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('url', [
    f'https://login.microsoftonline.com/{SOME_GUID}/oauth2/v2.0/token',
    'https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0/oauth2/token',
    'https://LOGIN.MICROSOFTONLINE.COM/common/oauth2/v2.0/token',
    'https://login.microsoftonline.com/common/oauth2/v2.0/token/',
    'https://login.microsoftonline.com:443/common/oauth2/v2.0/token',
])
def test_the_microsoft_login_host_is_accepted_for_any_tenant_form(entra_host, url):
    assert entra_host.is_entra_authority_url(url) is True


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
def test_any_other_login_address_is_refused(entra_host, url):
    assert entra_host.is_entra_authority_url(url) is False
