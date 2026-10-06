"""Regression tests for issue #6906.

``test_toolkit_enter_room`` used to join ``room_{event_name}_{stream_id}`` with both values
taken from the client and no identity check, so any socket (even unauthenticated) could
reach chat_predict / eval_run_progress rooms or another user's toolkit test stream.

The fix: event_name is pinned to ``test_toolkit_tool``, stream_id must be a UUID, the sid must
carry a real user id, and the stream_id is bound to the first user that claims it (join or
task start) in Redis. Both sides fail closed when Redis is unavailable.
``sio/all.py`` is loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``).
"""
import sys
import types
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fixtures.sio_harness import SIO_PKG


class FakeRedis:
    """Emulates the one script claim_stream sends, on the same key/value/TTL contract."""

    def __init__(self):
        self.store = {}
        self.ttl = {}
        self.calls = 0

    def eval(self, script, numkeys, key, user, ttl):  # noqa: ARG002
        self.calls += 1
        if key not in self.store:
            self.store[key], self.ttl[key] = user, ttl
            return 1
        if self.store[key] == user:
            self.ttl[key] = ttl
            return 1
        return 0


class BrokenRedis:
    def eval(self, *a, **k):
        raise ConnectionError('redis down')


@pytest.fixture
def rooms(sio_all):  # noqa: ARG001
    return sys.modules[f'{SIO_PKG}.utils.toolkit_test_rooms']


class _Handler:
    def __init__(self, redis_client):
        self.context = types.SimpleNamespace(sio=MagicMock())
        self._redis = redis_client

    def get_redis_client(self):
        return self._redis


def _setup_users(sio_all, users: dict):
    """users: sid -> user id (None = public/unauthenticated visitor)."""
    sio_all.auth.sio_users = {sid: uid for sid, uid in users.items()}
    sio_all.auth.current_user = lambda auth_data=None: {'id': auth_data}


def _join(sio_all, handler, sid, data):
    sio_all.SIO.test_toolkit_enter_room(handler, sid, data)
    return handler.context.sio.enter_room.call_args_list


# ---- claim_stream ----

def test_first_claimer_owns_stream_and_others_are_refused(rooms):
    r, s = FakeRedis(), uuid4()
    assert rooms.claim_stream(r, s, 3) is True
    assert rooms.claim_stream(r, s, 6) is False
    # Owner re-claims (page reload) and refreshes the TTL
    r.ttl[rooms._key(s)] = 5
    assert rooms.claim_stream(r, str(s), 3) is True
    assert r.ttl[rooms._key(s)] == rooms.STREAM_OWNER_TTL


def test_claim_is_a_single_redis_round_trip(rooms):
    r, s = FakeRedis(), uuid4()
    rooms.claim_stream(r, s, 3)
    rooms.claim_stream(r, s, 3)
    rooms.claim_stream(r, s, 6)
    assert r.calls == 3


def test_claim_without_user_id_never_binds(rooms):
    r = FakeRedis()
    assert rooms.claim_stream(r, uuid4(), None) is False
    assert r.store == {}


@pytest.mark.parametrize('stream_id', ['not-a-uuid', 'x' * 5000, '', '1'])
def test_non_uuid_stream_ids_are_never_claimed(rooms, stream_id):
    # Bounds the Redis key size and stops pre-claiming of guessable strings
    r = FakeRedis()
    assert rooms.claim_stream(r, stream_id, 3) is False
    assert r.store == {}


def test_uuid_spellings_map_to_one_key(rooms):
    r, s = FakeRedis(), uuid4()
    assert rooms.claim_stream(r, str(s).upper(), 3) is True
    assert rooms.claim_stream(r, str(s), 6) is False


def test_chat_events_are_not_owner_bound(rooms):
    # Chat streams use the conversation uuid shared by every participant, so binding them
    # would lock out all but the first user.
    for event in ('chat_predict', 'chat_predict_attachment', 'application_predict'):
        assert event not in rooms.OWNED_EVENTS
    assert rooms.OWNED_EVENTS == {'test_toolkit_tool', 'test_mcp_connection'}


# ---- guard_test_stream (task start side) ----

def _start(rooms, sio_all, redis_client, user_id, stream_id, event='test_toolkit_tool'):
    handler = _Handler(redis_client)
    data = {'stream_id': str(stream_id), 'message_id': 'm1', 'user_id': user_id}
    rooms.guard_test_stream(handler, 'sid-1', event, data)
    return handler


def test_start_by_owner_passes_and_by_other_user_is_refused(rooms, sio_all):
    r, s = FakeRedis(), uuid4()
    _start(rooms, sio_all, r, 3, s)
    _start(rooms, sio_all, r, 3, s)
    with pytest.raises(sio_all.SioValidationError):
        _start(rooms, sio_all, r, 6, s, event='test_mcp_connection')


def test_start_fails_closed_when_redis_is_down(rooms, sio_all):
    # Failing open here would leave the stream unbound, so whoever joined first after Redis
    # recovered would own the victim's stream.
    with pytest.raises(sio_all.SioValidationError) as exc:
        _start(rooms, sio_all, BrokenRedis(), 3, uuid4())
    assert 'try again' in str(exc.value.error)


def test_start_with_non_uuid_stream_id_is_refused(rooms, sio_all):
    with pytest.raises(sio_all.SioValidationError):
        _start(rooms, sio_all, FakeRedis(), 3, 'not-a-uuid')


def test_start_of_unbound_events_skips_redis(rooms, sio_all):
    r = FakeRedis()
    _start(rooms, sio_all, r, 3, 'conv-shared-id', event='chat_predict_attachment')
    assert r.calls == 0


# ---- test_toolkit_enter_room handler ----

@pytest.mark.parametrize('event_name', [
    'chat_predict', 'eval_run_progress', 'application_predict', 'test_mcp_connection', 'anything',
])
def test_non_toolkit_event_names_are_rejected(sio_all, event_name):
    _setup_users(sio_all, {'sid-a': 3})
    handler = _Handler(FakeRedis())
    with pytest.raises(sio_all.SioValidationError):
        sio_all.SIO.test_toolkit_enter_room(
            handler, 'sid-a', {'stream_id': str(uuid4()), 'event_name': event_name})
    handler.context.sio.enter_room.assert_not_called()


@pytest.mark.parametrize('stream_id', ['not-a-uuid', '1', 'x' * 5000])
def test_non_uuid_stream_id_is_rejected(sio_all, stream_id):
    _setup_users(sio_all, {'sid-a': 3})
    r = FakeRedis()
    handler = _Handler(r)
    with pytest.raises(sio_all.SioValidationError):
        sio_all.SIO.test_toolkit_enter_room(handler, 'sid-a', {'stream_id': stream_id})
    handler.context.sio.enter_room.assert_not_called()
    assert r.store == {}


@pytest.mark.parametrize('users, sid', [
    ({'sid-pub': None}, 'sid-pub'),  # connected as public visitor, no user id
    ({}, 'sid-gone'),                # sid unknown to auth
])
def test_unauthenticated_socket_is_silently_denied(sio_all, users, sid):
    _setup_users(sio_all, users)
    r = FakeRedis()
    handler = _Handler(r)
    assert _join(sio_all, handler, sid, {'stream_id': str(uuid4())}) == []
    handler.context.sio.emit.assert_not_called()
    assert r.store == {}  # must not claim the stream either


def test_owner_joins_other_user_denied_owner_rejoins_after_reload(sio_all):
    # DeepWiki flow: join before start (claims), another user tries the same id, owner reloads.
    _setup_users(sio_all, {'sid-owner': 3, 'sid-other': 6, 'sid-owner-reload': 3})
    r, s = FakeRedis(), uuid4()
    payload = {'stream_id': str(s), 'event_name': 'test_toolkit_tool'}

    calls = _join(sio_all, _Handler(r), 'sid-owner', payload)
    assert [c.args for c in calls] == [('sid-owner', f'room_test_toolkit_tool_{s}')]

    h2 = _Handler(r)
    assert _join(sio_all, h2, 'sid-other', {'stream_id': str(s)}) == []
    h2.context.sio.emit.assert_not_called()

    calls = _join(sio_all, _Handler(r), 'sid-owner-reload', {'stream_id': str(s)})
    assert [c.args for c in calls] == [('sid-owner-reload', f'room_test_toolkit_tool_{s}')]


def test_stream_claimed_at_task_start_blocks_other_users_join(sio_all, rooms):
    _setup_users(sio_all, {'sid-other': 6})
    r, s = FakeRedis(), uuid4()
    assert rooms.claim_stream(r, s, 3)
    assert _join(sio_all, _Handler(r), 'sid-other', {'stream_id': str(s)}) == []


def test_redis_failure_denies_join(sio_all):
    _setup_users(sio_all, {'sid-a': 3})
    handler = _Handler(BrokenRedis())
    assert _join(sio_all, handler, 'sid-a', {'stream_id': str(uuid4())}) == []
