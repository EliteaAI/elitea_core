"""Regression tests for issue #6906.

``test_toolkit_enter_room`` used to join ``room_{event_name}_{stream_id}`` with both values
taken from the client and no identity check, so any socket (even unauthenticated) could
reach chat_predict / eval_run_progress rooms or another user's toolkit test stream.

The fix: event_name is pinned to ``test_toolkit_tool``, the sid must carry a real user id,
and the stream_id is bound to the first user that claims it (join or task start) in Redis.
Same loading harness as ``test_sio_validation_error_stream_id.py``.
"""
import importlib.abc
import importlib.util
import pathlib
import sys
import types
from unittest.mock import MagicMock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'ttkroomguardpkg_6906'

_STUBBED = ('redis', 'tools', 'sqlalchemy', 'pylon', f'{PKG}.models.conversation', f'{PKG}.models.message_group',
            f'{PKG}.models.enums', f'{PKG}.models.message_items', f'{PKG}.models.pd.participant',
            f'{PKG}.models.pd.predict', f'{PKG}.utils.continue_message',
            f'{PKG}.utils.participant_utils', f'{PKG}.utils.canvas_utils',
            f'{PKG}.utils.chat_constants', f'{PKG}.utils.conversation_access')


class _MockFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):  # noqa: ARG002
        if any(fullname == root or fullname.startswith(root + '.') for root in _STUBBED):
            return importlib.util.spec_from_loader(fullname, self)
        return None

    def create_module(self, spec):
        mock = MagicMock()
        mock.__name__ = spec.name
        mock.__spec__ = spec
        mock.__path__ = []
        if spec.name == 'pylon.core.tools':
            mock.web.sio = lambda *a, **k: (lambda f: f)
        return mock

    def exec_module(self, module):
        pass


def _load_sio_module():
    finder = _MockFinder()
    # Stubs left in sys.modules by earlier tests would bypass the finder in a full-suite run
    shadowed = {k: sys.modules.pop(k) for k in list(sys.modules)
                if any(k == root or k.startswith(root + '.') for root in _STUBBED)}
    sys.meta_path.insert(0, finder)
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    for name in (f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils', f'{PKG}.sio'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    sys.modules[PKG] = pkg
    try:
        for full, relpath in (
            (f'{PKG}.utils.sio_utils', 'utils/sio_utils.py'),
            (f'{PKG}.utils.toolkit_test_rooms', 'utils/toolkit_test_rooms.py'),
            (f'{PKG}.models.pd.sio', 'models/pd/sio.py'),
            (f'{PKG}.sio.all', 'sio/all.py'),
        ):
            spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / relpath)
            module = importlib.util.module_from_spec(spec)
            sys.modules[full] = module
            spec.loader.exec_module(module)
    finally:
        sys.meta_path.remove(finder)
        sys.modules.update(shadowed)
    return sys.modules[f'{PKG}.sio.all'], sys.modules[f'{PKG}.utils.toolkit_test_rooms']


class FakeRedis:
    """Just enough of redis-py for SET NX EX / GET / EXPIRE, storing bytes like the real one."""

    def __init__(self):
        self.store = {}
        self.ttl = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = str(value).encode()
        self.ttl[key] = ex
        return True

    def get(self, key):
        return self.store.get(key)

    def expire(self, key, seconds):
        self.ttl[key] = seconds
        return key in self.store


class BrokenRedis:
    def set(self, *a, **k):
        raise ConnectionError('redis down')


@pytest.fixture
def loaded():
    sio_all, rooms = _load_sio_module()
    yield sio_all, rooms
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


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


# ---- claim_stream (pure helper) ----

def test_first_claimer_owns_stream_and_others_are_refused(loaded):
    _, rooms = loaded
    r = FakeRedis()
    assert rooms.claim_stream(r, 's1', 3) is True
    assert rooms.claim_stream(r, 's1', 6) is False
    # Owner re-claims (page reload) and refreshes the TTL
    r.ttl[rooms._key('s1')] = 5
    assert rooms.claim_stream(r, 's1', 3) is True
    assert r.ttl[rooms._key('s1')] == rooms.STREAM_OWNER_TTL


def test_claim_without_user_id_never_binds(loaded):
    _, rooms = loaded
    r = FakeRedis()
    assert rooms.claim_stream(r, 's1', None) is False
    assert r.store == {}


def test_chat_events_are_not_owner_bound(loaded):
    # Chat streams use the conversation uuid shared by every participant, so binding them
    # would lock out all but the first user.
    _, rooms = loaded
    for event in ('chat_predict', 'chat_predict_attachment', 'application_predict'):
        assert event not in rooms.OWNED_EVENTS
    assert rooms.OWNED_EVENTS == {'test_toolkit_tool', 'test_mcp_connection'}


# ---- test_toolkit_enter_room handler ----

@pytest.mark.parametrize('event_name', [
    'chat_predict', 'eval_run_progress', 'application_predict', 'test_mcp_connection', 'anything',
])
def test_non_toolkit_event_names_are_rejected(loaded, event_name):
    sio_all, _ = loaded
    _setup_users(sio_all, {'sid-a': 3})
    handler = _Handler(FakeRedis())
    with pytest.raises(sio_all.SioValidationError):
        sio_all.SIO.test_toolkit_enter_room(handler, 'sid-a', {'stream_id': 'x', 'event_name': event_name})
    handler.context.sio.enter_room.assert_not_called()


@pytest.mark.parametrize('users, sid', [
    ({'sid-pub': None}, 'sid-pub'),  # connected as public visitor, no user id
    ({}, 'sid-gone'),                # sid unknown to auth
])
def test_unauthenticated_socket_is_silently_denied(loaded, users, sid):
    sio_all, _ = loaded
    _setup_users(sio_all, users)
    r = FakeRedis()
    handler = _Handler(r)
    assert _join(sio_all, handler, sid, {'stream_id': 's1'}) == []
    handler.context.sio.emit.assert_not_called()
    assert r.store == {}  # must not claim the stream either


def test_owner_joins_other_user_denied_owner_rejoins_after_reload(loaded):
    # DeepWiki flow: join before start (claims), another user tries the same id, owner reloads.
    sio_all, _ = loaded
    _setup_users(sio_all, {'sid-owner': 3, 'sid-other': 6, 'sid-owner-reload': 3})
    r = FakeRedis()

    h1 = _Handler(r)
    calls = _join(sio_all, h1, 'sid-owner', {'stream_id': 's1', 'event_name': 'test_toolkit_tool'})
    assert [c.args for c in calls] == [('sid-owner', 'room_test_toolkit_tool_s1')]

    h2 = _Handler(r)
    assert _join(sio_all, h2, 'sid-other', {'stream_id': 's1'}) == []
    h2.context.sio.emit.assert_not_called()

    h3 = _Handler(r)
    calls = _join(sio_all, h3, 'sid-owner-reload', {'stream_id': 's1'})
    assert [c.args for c in calls] == [('sid-owner-reload', 'room_test_toolkit_tool_s1')]


def test_stream_claimed_at_task_start_blocks_other_users_join(loaded):
    # Start side claims via the same helper; a later join by someone else must fail.
    sio_all, rooms = loaded
    _setup_users(sio_all, {'sid-other': 6})
    r = FakeRedis()
    assert rooms.claim_stream(r, 's1', 3)
    handler = _Handler(r)
    assert _join(sio_all, handler, 'sid-other', {'stream_id': 's1'}) == []


def test_redis_failure_denies_join(loaded):
    sio_all, _ = loaded
    _setup_users(sio_all, {'sid-a': 3})
    handler = _Handler(BrokenRedis())
    assert _join(sio_all, handler, 'sid-a', {'stream_id': 's1'}) == []
