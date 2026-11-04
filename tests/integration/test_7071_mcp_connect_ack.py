"""Regression tests for issue #7071: ``mcp_connect`` answers the local MCP client instead of dropping it silently.

The handler's return value is the Socket.IO ACK a client gets from ``sio.call("mcp_connect", ...)``.
``sio/mcp.py`` is loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``) with the real
``ServersStorage``, so the duplicate-name decline comes from the storage itself.
"""
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from fixtures.sio_harness import SIO_PKG

SID = 'sid-1'
OTHER_SID = 'sid-2'
PROJECT_ID = 13


@pytest.fixture
def mcp(sio_all):  # pylint: disable=W0613
    module = sys.modules[f'{SIO_PKG}.sio.mcp']
    module.auth.is_sio_user_in_project.reset_mock()
    module.auth.is_sio_user_in_project.return_value = True
    module.mcp_notification_to_sid.reset_mock()
    return module


@pytest.fixture
def platform():
    storage = sys.modules[f'{SIO_PKG}.utils.mcp_servers_storage'].ServersStorage()
    return SimpleNamespace(servers_storage=storage, context=SimpleNamespace(sio=MagicMock()))


def _server(name):
    return {'name': name, 'tools': []}


def _payload(*names, project_id=PROJECT_ID):
    return {
        'project_id': project_id,
        'timeout_tools_list': 30,
        'timeout_tools_call': 60,
        'toolkit_configs': [_server(name) for name in names],
    }


def _connect(mcp, platform, data, sid=SID):
    """The ACK as the client receives it: Socket.IO sends it JSON-encoded."""
    return json.loads(json.dumps(mcp.SIO.mcp_connect(platform, sid, data)))


def _notifications(mcp):
    return [call.args for call in mcp.mcp_notification_to_sid.call_args_list]


# ---- refusals ------------------------------------------------------------------------------------------------

def test_a_client_outside_the_project_is_told_so(mcp, platform):
    mcp.auth.is_sio_user_in_project.return_value = False

    ack = _connect(mcp, platform, _payload('jira'))

    assert ack == {
        'ok': False,
        'error': 'not_in_project',
        'message': f'User has no access to project {PROJECT_ID} or the project does not exist.',
        'project_id': PROJECT_ID,
    }
    mcp.auth.is_sio_user_in_project.assert_called_once_with(SID, PROJECT_ID)
    assert platform.servers_storage.get_servers_dict(PROJECT_ID) == {}
    platform.context.sio.enter_room.assert_not_called()


def test_an_invalid_payload_is_answered_with_the_failing_fields(mcp, platform):
    ack = _connect(mcp, platform, {'project_id': 'abc', 'toolkit_configs': [{'tools': []}]})

    assert ack['ok'] is False
    assert ack['error'] == 'validation_error'
    assert ack['message']
    assert {tuple(detail['loc']) for detail in ack['details']} == {
        ('project_id',), ('timeout_tools_list',), ('timeout_tools_call',), ('toolkit_configs', 0, 'name'),
    }
    assert all('input' not in detail for detail in ack['details'])
    mcp.auth.is_sio_user_in_project.assert_not_called()


def test_a_payload_that_is_not_an_object_is_a_validation_error(mcp, platform):
    ack = _connect(mcp, platform, 'jira')

    assert (ack['ok'], ack['error']) == (False, 'validation_error')


# ---- registration ------------------------------------------------------------------------------------------

def test_registered_servers_are_listed_by_their_sanitized_name(mcp, platform):
    ack = _connect(mcp, platform, _payload('jira', 'my-server'))

    assert ack == {'ok': True, 'registered': ['jira', 'my_server'], 'declined': []}
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'jira', 'my_server'}


def test_a_name_another_client_holds_is_declined_with_its_reason(mcp, platform):
    _connect(mcp, platform, _payload('github'), sid=OTHER_SID)

    ack = _connect(mcp, platform, _payload('jira', 'github'))

    assert ack == {
        'ok': True,
        'registered': ['jira'],
        'declined': [{'name': 'github', 'reason': 'name_already_registered'}],
    }
    assert platform.servers_storage.get_server(PROJECT_ID, 'github').sio_sid == OTHER_SID


def test_a_name_with_no_letters_is_declined_and_not_registered(mcp, platform):
    ack = _connect(mcp, platform, _payload('jira', '123'))

    assert ack == {'ok': True, 'registered': ['jira'], 'declined': [{'name': '123', 'reason': 'invalid_name'}]}
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'jira'}


# ---- older clients ---------------------------------------------------------------------------------------------

def test_older_clients_still_get_the_notifications_and_the_room(mcp, platform):
    """Clients before #7071 use ``emit``, ignore the ACK and read these notifications instead."""
    _connect(mcp, platform, _payload('github'), sid=OTHER_SID)
    mcp.mcp_notification_to_sid.reset_mock()

    _connect(mcp, platform, _payload('jira', 'github'))

    assert _notifications(mcp) == [(SID, 'jira - registered.'), (SID, 'github - declined.')]
    platform.context.sio.enter_room.assert_called_with(SID, f'room_mcp_connect_{PROJECT_ID}')
