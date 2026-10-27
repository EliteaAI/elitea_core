"""Regression tests for issue #7024: the RPC entry points answer a send they refuse.

``rpc/application.py`` (application predict, MCP connection test) used to ``return`` without a reply
when the socket was not in the project, and ``rpc/chat_all.py`` sent its early refusals to the chat
room, which a socket that reconnected has not rejoined. Either way the client's reply placeholder
spun forever. The sio handlers are covered in test_7024_predict_rejection_reply.py.

The methods are cut out of their modules by ``fixtures/rpc_method_harness.py``: only the names these
paths really depend on are real, every other name is a mock.
"""
import types
from unittest.mock import MagicMock

import pytest

from fixtures.rpc_method_harness import load_real_module, load_rpc_method

SID = 'sid-1'
CONVERSATION_UUID = 'conv-uuid-1'
QUESTION_ID = 'question-1'
MESSAGE_ID = 'message-1'
STREAM_ID = 'stream-1'


@pytest.fixture(scope='module')
def sio_utils():
    return load_real_module('utils/sio_utils.py', 'rpc_harness_sio_utils')


@pytest.fixture
def auth():
    return MagicMock(name='auth')


def _fake_self():
    return types.SimpleNamespace(context=types.SimpleNamespace(sio=MagicMock(name='sio')))


def _rejection_frames(sio):
    """Every ``socket_validation_error`` the fake socket server was asked to emit."""
    return [
        call.kwargs for call in sio.emit.call_args_list
        if call.kwargs.get('event') == 'socket_validation_error'
    ]


# ---------------------------------------------------------------------------------------------
# rpc/application.py
# ---------------------------------------------------------------------------------------------

@pytest.fixture
def application_predict(sio_utils, auth):
    parsed = types.SimpleNamespace(project_id=1, stream_id=STREAM_ID, message_id=MESSAGE_ID)
    request_model = MagicMock(name='ApplicationChatRequest')
    request_model.model_validate.return_value = parsed
    return load_rpc_method('rpc/application.py', 'RPC', 'predict_sio', {
        'SioEvents': sio_utils.SioEvents,
        'SioValidationError': sio_utils.SioValidationError,
        'SOCKET_UNAUTHENTICATED_CODE': sio_utils.SOCKET_UNAUTHENTICATED_CODE,
        'SOCKET_UNAUTHENTICATED_MESSAGE': sio_utils.SOCKET_UNAUTHENTICATED_MESSAGE,
        'ApplicationChatRequest': request_model,
        'auth': auth,
    })


def test_application_predict_from_an_anonymous_socket_is_answered(sio_utils, auth, application_predict):
    auth.is_sio_user_in_project.return_value = False
    handler = _fake_self()

    with pytest.raises(sio_utils.SioValidationError) as raised:
        application_predict(handler, SID, {'stream_id': STREAM_ID, 'message_id': MESSAGE_ID}, sio_event='chat_predict')

    (frame,) = _rejection_frames(handler.context.sio)
    assert frame['to'] == SID
    assert frame['data']['code'] == 'socket_unauthenticated'
    assert frame['data']['event'] == 'chat_predict'
    assert frame['data']['stream_id'] == STREAM_ID
    assert frame['data']['message_id'] == MESSAGE_ID
    assert frame['data']['content'] == sio_utils.SOCKET_UNAUTHENTICATED_MESSAGE
    assert raised.value.code == 'socket_unauthenticated'


def test_application_predict_checks_membership_in_the_run_project(sio_utils, auth, application_predict):
    auth.is_sio_user_in_project.return_value = False

    with pytest.raises(sio_utils.SioValidationError):
        application_predict(_fake_self(), SID, {'stream_id': STREAM_ID}, sio_event='chat_predict')

    auth.is_sio_user_in_project.assert_called_once_with(SID, 1)


def test_application_predict_names_the_event_the_caller_sent_it_for(sio_utils, auth, application_predict):
    auth.is_sio_user_in_project.return_value = False
    handler = _fake_self()

    with pytest.raises(sio_utils.SioValidationError):
        application_predict(handler, SID, {'stream_id': STREAM_ID}, sio_event='application_predict')

    (frame,) = _rejection_frames(handler.context.sio)
    assert frame['data']['event'] == 'application_predict'


@pytest.fixture
def mcp_connection_test(sio_utils, auth):
    return load_rpc_method('rpc/application.py', 'RPC', 'test_mcp_connection_sio', {
        'SioValidationError': sio_utils.SioValidationError,
        'SOCKET_UNAUTHENTICATED_CODE': sio_utils.SOCKET_UNAUTHENTICATED_CODE,
        'SOCKET_UNAUTHENTICATED_MESSAGE': sio_utils.SOCKET_UNAUTHENTICATED_MESSAGE,
        'auth': auth,
    })


def test_mcp_connection_test_from_an_anonymous_socket_is_answered(sio_utils, auth, mcp_connection_test):
    auth.is_sio_user_in_project.return_value = False
    handler = _fake_self()
    data = {
        'toolkit_config': {'type': 'mcp'},
        'project_id': 1,
        'stream_id': STREAM_ID,
        'message_id': MESSAGE_ID,
    }

    with pytest.raises(sio_utils.SioValidationError):
        mcp_connection_test(handler, SID, data)

    (frame,) = _rejection_frames(handler.context.sio)
    assert frame['to'] == SID
    assert frame['data']['code'] == 'socket_unauthenticated'
    assert frame['data']['event'] == 'test_mcp_connection'
    assert frame['data']['stream_id'] == STREAM_ID
    assert frame['data']['message_id'] == MESSAGE_ID
    auth.is_sio_user_in_project.assert_called_once_with(SID, 1)


def test_mcp_connection_test_still_validates_the_payload_first(sio_utils, mcp_connection_test):
    handler = _fake_self()

    with pytest.raises(ValueError, match='toolkit_config is required'):
        mcp_connection_test(handler, SID, {'project_id': 1})

    assert not _rejection_frames(handler.context.sio)


# ---------------------------------------------------------------------------------------------
# rpc/chat_all.py: refusals made before the turn is stored
# ---------------------------------------------------------------------------------------------

class _RedisConnectionError(Exception):
    """Stands in for ``redis.exceptions.ConnectionError`` in the ``except`` clause of the lock check."""


@pytest.fixture
def chat_session():
    """The database session ``predict_sio`` opens, with the duplicate-question lookup found nothing."""
    session = MagicMock(name='session')
    session.query.return_value.filter.return_value.first.return_value = None
    # The conversation lookup; its uuid names the chat room
    session.query.return_value.where.return_value.first.return_value.uuid = CONVERSATION_UUID
    return session


@pytest.fixture
def chat_predict(sio_utils, auth, chat_session):
    parsed = types.SimpleNamespace(
        project_id=1, conversation_uuid=CONVERSATION_UUID, question_id=QUESTION_ID,
        participant_id=5, user_ids=[], user_input='hello',
    )
    predict_model = MagicMock(name='SioPredictModel')
    predict_model.model_validate.return_value = parsed
    db = MagicMock(name='db')
    db.get_session.return_value.__enter__.return_value = chat_session
    return load_rpc_method('rpc/chat_all.py', 'RPC', 'predict_sio', {
        'SioEvents': sio_utils.SioEvents,
        'SioValidationError': sio_utils.SioValidationError,
        'SioPredictModel': predict_model,
        'auth': auth,
        'db': db,
        'redis': types.SimpleNamespace(exceptions=types.SimpleNamespace(ConnectionError=_RedisConnectionError)),
        # a refusal of the sender itself is another path; these tests start from an allowed sender
        'check_post_access': lambda *args, **kwargs: None,
        'get_or_create_one': lambda **kwargs: (MagicMock(name='author_participant'), True),
        'check_chat_input': lambda user_input, *args, **kwargs: (user_input, None),
        'get_chat_room': lambda conversation_uuid: f'room-{conversation_uuid}',
    })


def _locked(handler):
    handler.get_redis_client.return_value.get.return_value = 'true'


def _redis_down(handler):
    handler.get_redis_client.return_value.get.side_effect = _RedisConnectionError()


def _duplicate(handler, chat_session):
    chat_session.query.return_value.filter.return_value.first.return_value = MagicMock(name='stored_turn')


@pytest.mark.parametrize('arrange, content', [
    (lambda handler, session: _duplicate(handler, session), 'This message has already been submitted'),
    (lambda handler, session: _locked(handler), 'Conversation is locked while summarization is in progress'),
    (lambda handler, session: _redis_down(handler), 'Redis connection error'),
], ids=['duplicate question', 'locked conversation', 'redis down'])
def test_early_refusal_goes_to_the_sender_not_the_room(chat_predict, chat_session, arrange, content):
    handler = MagicMock(name='handler')
    arrange(handler, chat_session)

    result = chat_predict(handler, SID, {})

    assert result == {'error': content}
    (frame,) = _rejection_frames(handler.context.sio)
    # A socket that reconnected has not rejoined the room, so the reply must be addressed to it
    assert frame['to'] == SID
    assert 'room' not in frame
    assert frame['data'] == {
        'event': 'chat_predict',
        'content': content,
        'type': 'error',
        'stream_id': CONVERSATION_UUID,
        'message_id': QUESTION_ID,
    }


@pytest.mark.parametrize('arrange', [
    lambda handler, session: _duplicate(handler, session),
    lambda handler, session: _locked(handler),
    lambda handler, session: _redis_down(handler),
], ids=['duplicate question', 'locked conversation', 'redis down'])
def test_early_refusal_without_a_socket_still_goes_to_the_room(chat_predict, chat_session, arrange):
    """REST and RPC callers have no sid; their refusal keeps reaching whoever listens on the chat room."""
    handler = MagicMock(name='handler')
    arrange(handler, chat_session)

    chat_predict(handler, None, {})

    (frame,) = _rejection_frames(handler.context.sio)
    assert frame['room'] == f'room-{CONVERSATION_UUID}'
    assert 'to' not in frame


def test_a_conversation_that_is_not_locked_is_not_refused(chat_predict, chat_session):
    """Control: the refusals above are not unconditional, so they do prove the check ran."""
    handler = MagicMock(name='handler')
    handler.get_redis_client.return_value.get.return_value = 'false'
    chat_session.query.return_value.filter.return_value.all.return_value = []

    try:
        chat_predict(handler, SID, {})
    except Exception:  # pylint: disable=W0703
        # Past the checks the method needs the real ORM; only the refusals matter here
        pass

    assert not _rejection_frames(handler.context.sio)
