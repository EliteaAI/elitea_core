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

    datasource_predict = 'datasource_predict'
    datasource_dataset_status = 'datasource_dataset_status'
    datasource_leave_rooms = 'datasource_leave_rooms'

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


class SioValidationError(Exception):
    def __init__(self, sio, sid: str | None, event: str, error, stream_id: str, message_id: str | None = None):
        self.sio = sio
        self.type = 'error'
        self.event = event
        self.error = error
        self.stream_id = stream_id
        self.sid = sid
        self.message_id = message_id
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
            data={
                'event': self.event,
                'content': self.error,
                'type': self.type,
                'stream_id': self.stream_id,
                'message_id': self.message_id
            },
            to=self.sid,
        )


def get_event_room(event_name: str, room_id: UUID | str) -> str:
    return f'room_{event_name}_{room_id}'


def get_chat_room(room_uuid: str, **kwargs):
    return get_event_room(
        event_name=SioEvents.chat_predict.value,
        room_id=str(room_uuid)
    )


def get_canvas_room(canvas_uuid: str, **kwargs):
    return get_event_room(
        event_name='canvas',
        room_id=str(canvas_uuid)
    )
