"""Regression tests for issue #7024.

A socket that reconnects with an expired session is stored by the server as an anonymous user.
``chat_predict`` / ``chat_continue_predict`` then hit the project-membership check and used to
``return`` without a reply, so the client's reply placeholder spun forever. Any unexpected error
raised while starting the run was only logged by pylon, with the same result.

``sio/all.py`` is loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``).
"""
import types
from unittest.mock import MagicMock

import pytest

CONVERSATION_UUID = 'conv-uuid-1'
QUESTION_ID = 'question-1'
MESSAGE_ID = 'message-1'


class _Handler:
    """Minimal stand-in for the SIO mixin's `self`."""

    def __init__(self, sio_all):
        self.context = types.SimpleNamespace(sio=MagicMock())
        self.chat_predict_sio = MagicMock()
        self.chat_continue_predict_sio = MagicMock()
        # The real helper is a plain method of the SIO class; bind it to this stand-in
        self._predict_failure = types.MethodType(sio_all.SIO._predict_failure, self)


@pytest.fixture
def handler(sio_all):
    return _Handler(sio_all)


@pytest.fixture
def parsed_payload(sio_all):
    """Both payload models are stubbed in the harness, so hand the handlers a ready parse result."""
    parsed = types.SimpleNamespace(
        project_id=1, conversation_uuid=CONVERSATION_UUID, question_id=QUESTION_ID, message_id=MESSAGE_ID,
        model_dump=lambda: {'project_id': 1},
    )
    sio_all.SioPredictModel.model_validate.return_value = parsed
    sio_all.SioContinuePredictModel.model_validate.return_value = parsed
    return parsed


def _emitted(handler):
    (call,) = handler.context.sio.emit.call_args_list
    return call.kwargs


@pytest.mark.parametrize('handler_name, event, message_id', [
    ('predict', 'chat_predict', QUESTION_ID),
    ('continue_predict', 'chat_continue_predict', MESSAGE_ID),
])
def test_anonymous_socket_gets_an_unauthenticated_reply(
    sio_all, handler, parsed_payload, handler_name, event, message_id,
):
    sio_all.auth.is_sio_user_in_project.return_value = False

    with pytest.raises(sio_all.SioValidationError):
        getattr(sio_all.SIO, handler_name)(handler, 'sid-1', {})

    emitted = _emitted(handler)
    assert emitted['to'] == 'sid-1'
    assert emitted['event'] == 'socket_validation_error'
    assert emitted['data']['event'] == event
    assert emitted['data']['code'] == 'socket_unauthenticated'
    assert emitted['data']['stream_id'] == CONVERSATION_UUID
    assert emitted['data']['message_id'] == message_id
    handler.chat_predict_sio.assert_not_called()
    handler.chat_continue_predict_sio.assert_not_called()


@pytest.mark.parametrize('handler_name, runner', [
    ('predict', 'chat_predict_sio'),
    ('continue_predict', 'chat_continue_predict_sio'),
])
def test_unexpected_failure_while_starting_a_run_is_reported_to_the_sender(
    sio_all, handler, parsed_payload, handler_name, runner,
):
    sio_all.auth.is_sio_user_in_project.return_value = True
    getattr(handler, runner).side_effect = RuntimeError('connection to rpc timed out')

    with pytest.raises(sio_all.SioValidationError):
        getattr(sio_all.SIO, handler_name)(handler, 'sid-1', {})

    data = _emitted(handler)['data']
    assert data['content'] == sio_all.SEND_FAILED_MESSAGE
    # The exception text names internals and must stay in the log
    assert 'rpc' not in str(data['content'])
    assert 'code' not in data


def test_pool_saturation_gets_a_retry_hint(sio_all, handler, parsed_payload):
    sio_all.auth.is_sio_user_in_project.return_value = True
    handler.chat_predict_sio.side_effect = sio_all.PoolSaturationError(pool='agents')

    with pytest.raises(sio_all.SioValidationError):
        sio_all.SIO.predict(handler, 'sid-1', {})

    assert 'busy' in _emitted(handler)['data']['content']


def test_validation_error_from_the_runner_is_not_reported_twice(sio_all, handler, parsed_payload):
    sio_all.auth.is_sio_user_in_project.return_value = True
    already_reported = sio_all.SioValidationError(
        sio=MagicMock(), sid=None, event='chat_predict', error='denied', stream_id='s',
    )
    handler.chat_predict_sio.side_effect = already_reported

    with pytest.raises(sio_all.SioValidationError) as exc_info:
        sio_all.SIO.predict(handler, 'sid-1', {})

    assert exc_info.value is already_reported
    handler.context.sio.emit.assert_not_called()


def test_code_is_only_sent_when_set(sio_all):
    sio = MagicMock()
    sio_all.SioValidationError(sio=sio, sid='sid-1', event='x', error='e', stream_id='s')
    assert 'code' not in sio.emit.call_args.kwargs['data']
