try:
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        ...

from uuid import UUID


class SioEvents(StrEnum):
    socket_validation_error = 'socket_validation_error'

    promptlib_predict = 'promptlib_predict'
    promptlib_leave_rooms = 'promptlib_leave_rooms'


    application_predict = 'application_predict'
    application_continue_message = 'application_continue_message'
    application_status = 'application_status'
    application_leave_rooms = 'application_leave_rooms'

    chat_predict = 'chat_predict'
    chat_continue_predict = 'chat_continue_predict'
    chat_enter_room = 'chat_enter_room'
    chat_leave_rooms = 'chat_leave_rooms'
    chat_participant_update = 'chat_participant_update'
    chat_participant_delete = 'chat_participant_delete'
    chat_message_delete = 'chat_message_delete'
    chat_message_delete_all = 'chat_message_delete_all'
    chat_message_sync = 'chat_message_sync'
    notifications_notify = 'notifications_notify'
    schedule_run_finished = 'schedule_run_finished'
    # chat_conversation_create = 'chat_conversation_create'
    # chat_conversation_delete = 'chat_conversation_delete'

    mcp_connect = 'mcp_connect'
    mcp_tools_call = 'mcp_tools_call'
    mcp_notification = 'mcp_notification'
    mcp_tools_list = 'mcp_tools_list'
    mcp_ping = 'mcp_ping'
    mcp_status = 'mcp_status'

    test_tool = 'test_tool'
    test_toolkit_enter_room = 'test_toolkit_enter_room'

    chat_canvas_join = 'chat_canvas_join'
    chat_canvas_edit = 'chat_canvas_edit'
    chat_canvas_leave_rooms = 'chat_canvas_leave_rooms'
    chat_canvas_detail = 'chat_canvas_detail'
    chat_canvas_sync = 'chat_canvas_sync'
    chat_canvas_error = 'chat_canvas_error'
    chat_canvas_editors_change = 'chat_canvas_editors_change'
    chat_canvas_content_change = 'chat_canvas_content_change'

    asr_start = 'asr_start'
    asr_audio_chunk = 'asr_audio_chunk'
    asr_stop = 'asr_stop'
    asr_transcript_delta = 'asr_transcript_delta'
    asr_transcript_done = 'asr_transcript_done'
    asr_error = 'asr_error'
    asr_speech_started = 'asr_speech_started'
    asr_vad_flush = 'asr_vad_flush'

    tts_start = 'tts_start'
    tts_stop = 'tts_stop'
    tts_audio_chunk = 'tts_audio_chunk'
    tts_done = 'tts_done'
    tts_error = 'tts_error'
    tts_word_timings = 'tts_word_timings'

    next_input_suggestion_ready = 'next_input_suggestion_ready'

    eval_run_progress = 'eval_run_progress'
    eval_run_enter_room = 'eval_run_enter_room'
    eval_run_leave_room = 'eval_run_leave_room'
    # Emitted back to the joining sid so the client can tell a working push transport from a
    # merely connected one. Without it a server that never joins the room — old build, or a
    # project-membership rejection — looks identical to a live feed and the client's polling
    # fallback stays switched off.
    eval_run_room_joined = 'eval_run_room_joined'


# Machine-readable reason sent with a rejection of a socket whose handshake did not yield a user
# that belongs to the project (anonymous after an expired session or a failed authorize call).
# The client reacts by checking the session and re-handshaking instead of just showing the text.
SOCKET_UNAUTHENTICATED_CODE = 'socket_unauthenticated'
SOCKET_UNAUTHENTICATED_MESSAGE = (
    'Your connection is not authorized for this project. '
    'Reconnecting; please resend your message.'
)
# Shown when a send fails for a reason the user cannot act on, so the reply never hangs
SEND_FAILED_MESSAGE = 'The request could not be started. Please try again.'


def refusal_frame(event, error, stream_id, message_id, code=None) -> dict:
    """The ``socket_validation_error`` frame the client settles a reply placeholder with."""
    frame = {
        'event': event,
        'content': error,
        'type': 'error',
        'stream_id': stream_id,
        'message_id': message_id,
    }
    if code:
        frame['code'] = code
    return frame


def emit_refusal(sio, sid, room, event, error, stream_id, message_id) -> None:
    """Tell the sender that a send was refused before it started a run.

    Addressed to the sender's socket when there is one. A socket that reconnected has not rejoined the chat
    room yet, so a refusal sent to the room would never reach it (#7024). Callers without a socket keep
    using the room, which is where their listeners are.
    """
    target = {'to': sid} if sid else {'room': room}
    sio.emit(
        event=SioEvents.socket_validation_error.value,
        data=refusal_frame(event, error, stream_id, message_id),
        **target,
    )


class SioValidationError(Exception):
    def __init__(
        self, sio, sid: str | None, event: str, error, stream_id: str = '',
        message_id: str | None = None, code: str | None = None,
    ):
        self.sio = sio
        self.type = 'error'
        self.event = event
        self.error = error
        self.stream_id = stream_id
        self.sid = sid
        self.message_id = message_id
        self.code = code
        # self.room = get_event_room(
        #     event_name=event,
        #     room_id=stream_id
        # )
        if self.sid:
            # self.enter_room()
            self.emit_error()
        super().__init__(error)

    # def enter_room(self) -> None:
    #     self.sio.enter_room(self.sid, self.room)

    def emit_error(self) -> None:
        self.sio.emit(
            event=SioEvents.socket_validation_error.value,
            data=refusal_frame(self.event, self.error, self.stream_id, self.message_id, self.code),
            to=self.sid,
        )


def get_event_room(event_name: str, room_id: UUID | str) -> str:
    return f'room_{event_name}_{room_id}'


def get_chat_room(room_uuid: str, **kwargs):
    return get_event_room(
        event_name=SioEvents.chat_predict.value,
        room_id=str(room_uuid)
    )


def get_eval_run_room(run_id: int | str, **kwargs):
    return get_event_room(
        event_name=SioEvents.eval_run_progress.value,
        room_id=str(run_id)
    )


def get_canvas_room(canvas_uuid: str, **kwargs):
    return get_event_room(
        event_name='canvas',
        room_id=str(canvas_uuid)
    )
