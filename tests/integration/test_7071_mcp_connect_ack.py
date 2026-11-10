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


# ---- #7076: status only for real changes on the additive path -------------------------------------------------

def _statuses(platform):
    return [call.kwargs['data'] for call in platform.context.sio.emit.call_args_list]


def test_a_declined_server_is_not_announced_as_connected(mcp, platform):
    _connect(mcp, platform, _payload('github'), sid=OTHER_SID)
    platform.context.sio.emit.reset_mock()

    _connect(mcp, platform, _payload('jira', 'github'))

    assert _statuses(platform) == [{'connected': True, 'project_id': PROJECT_ID, 'type': 'jira'}]


# ---- #7076: snapshot mode -------------------------------------------------------------------------------------

CLIENT = 'host-client-1'
EPOCH = '0190a000-0000-7000-8000-000000000001'
NEWER_EPOCH = '0190a000-0000-7000-8000-000000000002'
THIRD_SID = 'sid-3'
USERS = {SID: 1, OTHER_SID: 1, THIRD_SID: 2}


@pytest.fixture
def users(mcp):
    mcp.auth.sio_users = {sid: {'user_id': user_id} for sid, user_id in USERS.items()}
    mcp.auth.current_user.side_effect = lambda auth_data: {'id': auth_data['user_id']}


def _snapshot(*names, failed=(), generation=1, epoch=EPOCH, client_id=CLIENT, tools=None):
    return {
        **_payload(*names),
        'toolkit_configs': [{'name': name, 'tools': tools or []} for name in names],
        'protocol': 2, 'client_id': client_id, 'epoch': epoch, 'generation': generation,
        'failed_servers': [{'name': name, 'transport': 'stdio', 'kind': 'exited', 'message': f'Server "{name}": exited during start.'}
                           for name in failed],
    }


def _registry_version(mcp):
    return sys.modules[f'{SIO_PKG}.utils.schema_registry_version'].current()


def _rpc():
    return sys.modules[f'{SIO_PKG}.rpc.mcp_all'].RPC


def test_a_snapshot_registers_healthy_servers_and_keeps_failed_ones_offline(mcp, platform, users):
    ack = _connect(mcp, platform, _snapshot('github', failed=['jira']))

    assert ack == {'ok': True, 'protocol': 2, 'epoch': EPOCH, 'generation': 1, 'stale': False,
                   'registered': ['github'], 'declined': [], 'failed': ['jira']}
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github'}
    assert platform.servers_storage.get_failed_server(PROJECT_ID, 'jira').message == 'Server "jira": exited during start.'
    assert _statuses(platform) == [{'connected': True, 'project_id': PROJECT_ID, 'type': 'github'}]
    assert _notifications(mcp) == []


def test_a_later_snapshot_removes_what_it_no_longer_lists_and_announces_it(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', 'jira'))
    platform.context.sio.emit.reset_mock()

    ack = _connect(mcp, platform, _snapshot('github', failed=['jira'], generation=2))

    assert ack['registered'] == ['github']
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github'}
    assert _statuses(platform) == [{'connected': False, 'project_id': PROJECT_ID, 'type': 'jira'}]


def test_a_recovered_server_leaves_the_failed_store(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', failed=['jira']))

    _connect(mcp, platform, _snapshot('github', 'jira', generation=2))

    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github', 'jira'}
    assert platform.servers_storage.get_failed_server(PROJECT_ID, 'jira') is None


def test_an_old_or_repeated_generation_is_stale_and_changes_nothing(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', 'jira', generation=5))
    platform.context.sio.emit.reset_mock()

    for generation in (5, 4):
        ack = _connect(mcp, platform, _snapshot('github', generation=generation))
        assert (ack['stale'], ack['registered']) == (True, [])

    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github', 'jira'}
    platform.context.sio.emit.assert_not_called()


def test_a_restarted_client_starts_a_newer_epoch_from_generation_1(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', 'jira', generation=9))

    ack = _connect(mcp, platform, _snapshot('github', generation=1, epoch=NEWER_EPOCH), sid=OTHER_SID)

    assert (ack['stale'], ack['registered']) == (False, ['github'])
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github'}
    assert platform.servers_storage.get_server(PROJECT_ID, 'github').sio_sid == OTHER_SID


def test_a_message_from_an_older_epoch_is_stale(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', epoch=NEWER_EPOCH))

    ack = _connect(mcp, platform, _snapshot('jira', generation=7, epoch=EPOCH))

    assert ack['stale'] is True
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github'}


def test_names_another_client_holds_are_declined(mcp, platform, users):
    _connect(mcp, platform, _payload('confluence'), sid=THIRD_SID)
    _connect(mcp, platform, _snapshot('github'), sid=OTHER_SID)

    ack = _connect(mcp, platform, _snapshot('jira', 'github', 'confluence', client_id='other-client'))

    assert ack['registered'] == ['jira']
    assert ack['declined'] == [{'name': 'github', 'reason': 'name_already_registered'},
                               {'name': 'confluence', 'reason': 'name_already_registered'}]
    assert platform.servers_storage.get_server(PROJECT_ID, 'github').sio_sid == OTHER_SID


def test_a_new_socket_of_the_same_client_and_user_takes_over_its_names(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github'))

    ack = _connect(mcp, platform, _snapshot('github', generation=2), sid=OTHER_SID)
    platform.servers_storage.remove_servers(SID)

    assert ack['registered'] == ['github']
    assert platform.servers_storage.get_server(PROJECT_ID, 'github').sio_sid == OTHER_SID


def test_the_same_client_id_under_another_user_cannot_take_over(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github'))

    ack = _connect(mcp, platform, _snapshot('github', generation=2), sid=THIRD_SID)

    assert ack['declined'] == [{'name': 'github', 'reason': 'name_already_registered'}]
    assert platform.servers_storage.get_server(PROJECT_ID, 'github').sio_sid == SID


def test_an_unchanged_snapshot_does_not_invalidate_the_schema_cache(mcp, platform, users):
    tools = [{'name': 'echo', 'description': '', 'inputSchema': {'type': 'object', 'properties': {}, 'required': []}}]
    _connect(mcp, platform, _snapshot('github', tools=tools))
    version = _registry_version(mcp)
    platform.context.sio.emit.reset_mock()

    _connect(mcp, platform, _snapshot('github', tools=tools, generation=2))

    assert _registry_version(mcp) == version
    platform.context.sio.emit.assert_not_called()


def test_changed_tools_invalidate_the_schema_cache_and_are_announced(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github'))
    version = _registry_version(mcp)
    platform.context.sio.emit.reset_mock()
    tools = [{'name': 'echo', 'description': '', 'inputSchema': {'type': 'object', 'properties': {}, 'required': []}}]

    _connect(mcp, platform, _snapshot('github', tools=tools, generation=2))

    assert _registry_version(mcp) != version
    assert _statuses(platform) == [{'connected': True, 'project_id': PROJECT_ID, 'type': 'github'}]


def test_a_failed_server_is_never_online_and_never_shadows_another_clients_server(mcp, platform, users):
    _connect(mcp, platform, _snapshot('jira'), sid=OTHER_SID)

    _connect(mcp, platform, _snapshot('github', failed=['jira', 'figma'], client_id='other-client'))

    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github', 'jira'}
    assert platform.servers_storage.get_failed_server(PROJECT_ID, 'jira') is None
    assert platform.servers_storage.get_failed_server(PROJECT_ID, 'figma') is not None


def test_client_text_is_cut_short_and_stripped_of_control_characters(mcp, platform, users):
    data = _snapshot('github')
    data['failed_servers'] = [{'name': 'jira', 'kind': 'exited', 'message': 'bad\x1b[31m\nline' + 'x' * 2000}]

    _connect(mcp, platform, data)

    message = platform.servers_storage.get_failed_server(PROJECT_ID, 'jira').message
    assert message.startswith('bad [31m line')
    assert len(message) == 500


def test_an_unknown_failure_kind_is_a_validation_error(mcp, platform, users):
    data = _snapshot('github')
    data['failed_servers'] = [{'name': 'jira', 'kind': 'exploded', 'message': ''}]

    assert _connect(mcp, platform, data)['error'] == 'validation_error'


def test_a_disconnect_forgets_the_clients_failed_servers(mcp, platform, users):
    _connect(mcp, platform, _snapshot('github', failed=['jira']))

    platform.servers_storage.remove_servers(SID)

    assert platform.servers_storage.get_failed_server(PROJECT_ID, 'jira') is None
    assert platform.servers_storage.get_servers_dict(PROJECT_ID) == {}


def test_a_socket_that_registers_too_often_is_refused(mcp, platform, users):
    storage = sys.modules[f'{SIO_PKG}.utils.mcp_servers_storage']
    for generation in range(1, storage.CONNECTS_PER_WINDOW + 1):
        assert _connect(mcp, platform, _snapshot('github', generation=generation))['ok'] is True

    ack = _connect(mcp, platform, _snapshot('github', generation=99))

    assert (ack['ok'], ack['error']) == (False, 'rate_limited')
    assert _connect(mcp, platform, _snapshot('github', generation=100), sid=OTHER_SID)['ok'] is True


def test_the_rate_limit_window_moves_on(mcp, users):
    clock = MagicMock(return_value=0.0)
    storage = sys.modules[f'{SIO_PKG}.utils.mcp_servers_storage']
    servers = storage.ServersStorage(clock=clock)
    for _ in range(storage.CONNECTS_PER_WINDOW):
        servers.allow_connect(SID)
    assert servers.allow_connect(SID) is False

    clock.return_value = float(storage.CONNECT_WINDOW_SECONDS)

    assert servers.allow_connect(SID) is True


def test_the_ping_sweep_announces_the_servers_it_removes(mcp, platform, users, monkeypatch):
    _connect(mcp, platform, _snapshot('github'))
    _connect(mcp, platform, _payload('jira'), sid=THIRD_SID)
    platform.context.sio.emit.reset_mock()
    monkeypatch.setattr(sys.modules[f'{SIO_PKG}.rpc.mcp_all'], 'is_client_connected', lambda sid: sid == THIRD_SID)

    _rpc().mcp_servers_handler(platform)

    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'jira'}
    assert _statuses(platform) == [{'connected': False, 'project_id': PROJECT_ID, 'type': 'github'}]


def test_a_payload_without_generation_still_takes_the_additive_path(mcp, platform, users):
    _connect(mcp, platform, _payload('github'))

    ack = _connect(mcp, platform, _payload('jira'))

    assert ack == {'ok': True, 'registered': ['jira'], 'declined': []}
    assert set(platform.servers_storage.get_servers_dict(PROJECT_ID)) == {'github', 'jira'}


@pytest.mark.parametrize('payload', [_payload, _snapshot])
def test_a_call_timeout_set_per_server_is_kept_and_the_payload_value_fills_the_rest(mcp, platform, users, payload):
    data = payload('github', 'jira')
    data['toolkit_configs'][0]['timeout_tools_call'] = 65

    _connect(mcp, platform, data)

    assert platform.servers_storage.get_server(PROJECT_ID, 'github').timeout_tools_call == 65
    assert platform.servers_storage.get_server(PROJECT_ID, 'jira').timeout_tools_call == data['timeout_tools_call']


def test_a_server_the_client_added_between_snapshots_is_still_its_own_after_a_reconnect(mcp, platform, users):
    _connect(mcp, platform, _snapshot('jira'))
    _connect(mcp, platform, _payload('github'))

    ack = _connect(mcp, platform, _snapshot('jira', 'github', generation=2), sid=OTHER_SID)
    platform.servers_storage.remove_servers(SID)

    assert (ack['registered'], ack['declined']) == (['jira', 'github'], [])
    assert {name: server.sio_sid for name, server in platform.servers_storage.get_servers_dict(PROJECT_ID).items()} == {
        'jira': OTHER_SID, 'github': OTHER_SID}
