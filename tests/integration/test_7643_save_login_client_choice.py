"""#7643: which OAuth client the save-login exchange sends (DCR vs a client typed into the credential).

Found in review: after a DCR login the credential holds the registered client_id, and a Re-login (which
registers a fresh DCR client in the browser) had the stored DCR client override the new one, so the code
issued to the new client was exchanged as the old one (invalid_grant). A client typed into the credential
must still win over anything the browser sends.
"""
import types

import pytest

from .test_6688_oauth_proxy_resource_indicator import PKG, _module, proxy  # noqa: F401  (fixture reuse)

SERVER = 'https://mcp.example.test/mcp'


@pytest.fixture
def saved(proxy, monkeypatch):
    state = types.SimpleNamespace(credential_data={'url': SERVER}, saved=[])

    class Refused(Exception):
        def __init__(self, code, message, status):
            super().__init__(message)
            self.code, self.status = code, status

    def save(project_id, credential, token_data, request_data, client_id, client_secret, scope):
        state.saved.append({'client_id': client_id, 'client_secret': client_secret, 'used_dcr': request_data.used_dcr})
        return {'access_token': 'x', 'saved': True}

    _module(monkeypatch, f'{PKG}.utils.mcp_saved_login', SavedLoginRefused=Refused,
            check_login_target=lambda *a: None,
            load_login_credential=lambda *a: {'uuid': 'u', 'data': state.credential_data},
            save_exchanged_login=save)
    proxy.handler.auth = types.SimpleNamespace(current_user=lambda: {'id': 3},
                                               decorators=proxy.handler.auth.decorators)
    state.proxy = proxy
    return state


def _login(state, **body):
    state.proxy.request.json = {'token_endpoint': 'https://auth.example.test/oauth/token', 'code': 'c',
                                'redirect_uri': 'http://localhost/cb', 'save_login': True,
                                'configuration_uuid': 'u', 'resource': SERVER, **body}
    return state.proxy.api.post(2)


def test_relogin_with_a_fresh_dcr_client_sends_the_new_client(saved):
    saved.credential_data.update(client_id='dcr-old', used_dcr=True)

    _login(saved, client_id='dcr-new', used_dcr=True)

    assert saved.proxy.posted[0][1]['client_id'] == 'dcr-new'
    assert saved.saved == [{'client_id': 'dcr-new', 'client_secret': None, 'used_dcr': True}]


def test_a_stored_dcr_client_never_overrides_a_non_dcr_request(saved):
    saved.credential_data.update(client_id='dcr-old', used_dcr=True)

    _login(saved, client_id='typed-in-ui')

    assert saved.proxy.posted[0][1]['client_id'] == 'typed-in-ui'


def test_a_client_typed_into_the_credential_wins_over_the_browser(saved):
    saved.credential_data.update(client_id='configured', client_secret='s3cret')

    _login(saved, client_id='browser-sent', client_secret='other')

    sent = saved.proxy.posted[0][1]
    assert (sent['client_id'], sent.get('client_secret')) == ('configured', 's3cret')
