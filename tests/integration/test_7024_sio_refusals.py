"""Regression tests for issue #7024: a send the server refuses is answered, never dropped silently.

The refusal logic lives in two small helpers that are imported and tested directly:

* ``utils/sio_guards.ensure_sio_user_in_project``: the project-membership guard used by the chat predict
  and continue handlers and by the application predict and MCP connection RPCs.
* ``utils/sio_utils.emit_refusal``: the reply for a send refused before it starts a run.

Both are loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``).
"""
from unittest.mock import MagicMock

import pytest

from fixtures.sio_harness import sio_helper

SID = 'sid-1'
ROOM = 'room_chat_predict_conv-1'


@pytest.fixture
def guards(sio_all):  # pylint: disable=W0613
    return sio_helper('sio_guards')


@pytest.fixture
def utils(sio_all):  # pylint: disable=W0613
    return sio_helper('sio_utils')


def _frames(sio):
    return [call.kwargs for call in sio.emit.call_args_list if call.kwargs.get('event') == 'socket_validation_error']


# ---- ensure_sio_user_in_project ------------------------------------------------------------------------------

def test_a_member_is_let_through(guards):
    guards.auth.is_sio_user_in_project.return_value = True
    sio = MagicMock()

    guards.ensure_sio_user_in_project(sio, SID, 1, 'chat_predict', 'conv-1', 'q-1')

    sio.emit.assert_not_called()
    guards.auth.is_sio_user_in_project.assert_called_once_with(SID, 1)


def test_a_non_member_is_refused_with_the_code_the_client_acts_on(guards, utils):
    guards.auth.is_sio_user_in_project.return_value = False
    sio = MagicMock()

    with pytest.raises(utils.SioValidationError) as raised:
        guards.ensure_sio_user_in_project(sio, SID, 1, 'chat_predict', 'conv-1', 'q-1')

    (frame,) = _frames(sio)
    assert frame['to'] == SID
    assert frame['data'] == {
        'event': 'chat_predict',
        'content': utils.SOCKET_UNAUTHENTICATED_MESSAGE,
        'type': 'error',
        'stream_id': 'conv-1',
        'message_id': 'q-1',
        'code': 'socket_unauthenticated',
    }
    assert raised.value.code == 'socket_unauthenticated'


@pytest.mark.parametrize('event', ['chat_predict', 'chat_continue_predict', 'application_predict',
                                    'application_continue_message', 'test_mcp_connection'])
def test_the_refusal_names_the_event_it_answers(guards, utils, event):
    guards.auth.is_sio_user_in_project.return_value = False
    sio = MagicMock()

    with pytest.raises(utils.SioValidationError):
        guards.ensure_sio_user_in_project(sio, SID, 1, event, 's', 'm')

    assert _frames(sio)[0]['data']['event'] == event


def test_a_caller_without_a_socket_is_not_checked(guards):
    """REST and RPC callers have no sid; they are authorised elsewhere and must not be refused here."""
    guards.auth.is_sio_user_in_project.return_value = False
    sio = MagicMock()

    guards.ensure_sio_user_in_project(sio, None, 1, 'chat_predict', 'conv-1', 'q-1')

    guards.auth.is_sio_user_in_project.assert_not_called()
    sio.emit.assert_not_called()


def test_the_refusal_is_logged(guards, utils):
    guards.auth.is_sio_user_in_project.return_value = False

    with pytest.raises(utils.SioValidationError):
        guards.ensure_sio_user_in_project(MagicMock(), SID, 7, 'chat_predict', 's', 'm')

    guards.log.warning.assert_called_once()
    assert 'not in project' in guards.log.warning.call_args.args[0]


# ---- emit_refusal -------------------------------------------------------------------------------------------

def test_a_refusal_goes_to_the_sender_when_there_is_a_socket(utils):
    """A socket that reconnected has not rejoined the room, so the room would never deliver it."""
    sio = MagicMock()

    utils.emit_refusal(sio, SID, ROOM, 'chat_predict', 'Redis connection error', 'conv-1', 'q-1')

    (frame,) = _frames(sio)
    assert frame['to'] == SID
    assert 'room' not in frame
    assert frame['data'] == {
        'event': 'chat_predict',
        'content': 'Redis connection error',
        'type': 'error',
        'stream_id': 'conv-1',
        'message_id': 'q-1',
    }


def test_a_refusal_without_a_socket_still_goes_to_the_room(utils):
    sio = MagicMock()

    utils.emit_refusal(sio, None, ROOM, 'chat_predict', 'Redis connection error', 'conv-1', 'q-1')

    (frame,) = _frames(sio)
    assert frame['room'] == ROOM
    assert 'to' not in frame


def test_a_refusal_has_no_code(utils):
    """The code is only for the unauthenticated case; other refusals keep the frame they always had."""
    sio = MagicMock()

    utils.emit_refusal(sio, SID, ROOM, 'chat_predict', 'x', 's', 'm')

    assert 'code' not in _frames(sio)[0]['data']


def test_refusal_frame_and_validation_error_build_the_same_frame(utils):
    sio = MagicMock()

    utils.SioValidationError(sio=sio, sid=SID, event='e', error='x', stream_id='s', message_id='m', code='c')

    assert _frames(sio)[0]['data'] == utils.refusal_frame('e', 'x', 's', 'm', 'c')


# ---- the refusals that reach the sender through emit_refusal ---------------------------------------------------

def test_an_error_while_preparing_attachments_reaches_the_sender_not_the_room(utils):
    """Both preparation errors in chat predict are sent with ``emit_refusal``: a socket that reconnected has not
    rejoined the chat room, so a frame addressed to the room would never reach the sender (#7024)."""
    sio = MagicMock()

    utils.emit_refusal(sio, SID, ROOM, 'chat_predict', 'could not process the attachment', 'conv-1', 'q-1')

    (frame,) = _frames(sio)
    assert frame['to'] == SID
    assert 'room' not in frame


def test_the_content_of_a_payload_error_is_sent_as_it_is(utils):
    """The text of a PayloadGenerationError is already written for the user and must not be reworded."""
    sio = MagicMock()
    message = 'You can mention at most 3 skills in a message.'

    utils.emit_refusal(sio, SID, ROOM, 'chat_predict', message, 'conv-1', 'q-1')

    assert _frames(sio)[0]['data']['content'] == message
