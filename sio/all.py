#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" SIO """
from datetime import timedelta

import redis
from tools import db, config as c, auth
from pylon.core.tools import log, web
from sqlalchemy import desc

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_items.canvas import CanvasVersionItem, CanvasMessageItem
from ..models.pd.participant import ParticipantEntityUser
from ..models.pd.predict import SioPredictModel, SioContinuePredictModel
from ..models.pd.sio import (
    EnterRoomPayload,
    LeaveRoomPayload,
    JoinCanvasPayload,
    EditCanvasPayload,
    CanvasLeavePayload,
    TestToolkitEnterRoomPayload,
)
from ..utils.continue_message import continue_message
from ..utils.participant_utils import get_entity_details, get_or_create_one
from ..utils.canvas_utils import get_canvas_key, get_canvas_authors_key, get_shadow_key
from ..utils.chat_constants import CANVAS_CONTENT_TTL, CANVAS_SHADOW_KEY_OFFSET_TTL
from ..utils.sio_utils import get_chat_room, get_canvas_room, get_event_room
from ..utils.sio_utils import SioEvents, SioValidationError
from pydantic import ValidationError


class SIO:
    @web.sio(SioEvents.chat_enter_room)
    def enter_room(self, sid: str, data: dict) -> None:
        try:
            parsed = EnterRoomPayload.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_enter_room.value,
                error=e.errors(include_url=False, include_context=False),
            )
        #
        if not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: return valid error or raise SioValidationError
        #
        with db.get_session(parsed.project_id) as session:
            conversation = session.query(
                Conversation
            ).filter(Conversation.id == parsed.conversation_id).first()
            if conversation:
                room = get_chat_room(conversation.uuid)
                self.context.sio.enter_room(sid, room)

    @web.sio(SioEvents.test_toolkit_enter_room)
    def test_toolkit_enter_room(self, sid: str, data: dict) -> None:
        """
        Allow clients to (re)join a test_toolkit_tool room by stream_id.
        This enables reconnection after page reload while a task is running.
        """
        try:
            parsed = TestToolkitEnterRoomPayload.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.test_toolkit_enter_room.value,
                error=e.errors(include_url=False, include_context=False),
            )
        # Build the room name using the same pattern as test_toolkit_tool_sio
        room = get_event_room(
            event_name=parsed.event_name,
            room_id=str(parsed.stream_id)
        )
        self.context.sio.enter_room(sid, room)
        log.info(f"Socket {sid} joined room {room} for test_toolkit reconnection")

    @web.sio(SioEvents.chat_leave_rooms)
    def leave_rooms(self, sid: str, data: dict | list) -> None:
        assert isinstance(data, (dict, list)), 'Expected dict or list, got {}'.format(type(data))
        if isinstance(data, dict):
            data = [data]
        try:
            parsed_list = [LeaveRoomPayload.model_validate(i) for i in data]
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_leave_rooms.value,
                error=e.errors(include_url=False, include_context=False),
            )
        for item in parsed_list:
            room = get_chat_room(item.conversation_uuid)
            self.context.sio.leave_room(sid, room)

    @web.sio(SioEvents.chat_predict)
    def predict(self, sid: str, data: dict) -> None:
        try:
            parsed = SioPredictModel.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_predict.value,
                error=e.errors(include_url=False, include_context=False),
                stream_id=data.get("conversation_uuid"),
                message_id=data.get("payload", {}).get("message_id"),
            )
        #
        if not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: return valid error or raise SioValidationError
        #
        self.chat_predict_sio(sid=sid, data=parsed.model_dump())

    @web.sio(SioEvents.chat_continue_predict)
    def continue_predict(self, sid: str, data: dict) -> None:
        """
        Handle "Continue" requests for paused chat predictions (e.g., after MCP OAuth interruption).
        Routes to a separate RPC endpoint with simpler payload requirements.
        """
        try:
            parsed = SioContinuePredictModel.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_continue_predict.value,
                error=e.errors(include_url=False, include_context=False),
                stream_id=data.get("conversation_uuid"),
                message_id=data.get("message_id"),
            )
        #
        if not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: return valid error or raise SioValidationError
        #
        self.chat_continue_predict_sio(sid=sid, data=parsed.model_dump())

    @web.sio(SioEvents.chat_canvas_join)
    def join_canvas(self, sid: str, data: dict):
        try:
            parsed = JoinCanvasPayload.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_canvas_join.value,
                error=e.errors(include_url=False, include_context=False),
            )
        current_user = auth.current_user(
            auth_data=auth.sio_users[sid]
        )
        if not current_user.get('id'):
            log.error(f"Current_user is not valid: {current_user}")
            self.context.sio.emit(
                event=SioEvents.chat_canvas_error,
                data={'error': f"Current_user is not valid: {current_user}"},
                room=sid
            )
            return

        if not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: return valid error or raise SioValidationError

        client = self.get_redis_client()

        with db.get_session(parsed.project_id) as session:
            canvas_uuid = parsed.canvas_uuid
            room = get_canvas_room(canvas_uuid)
            project_id = parsed.project_id
            self.context.sio.enter_room(sid, room)
            try:
                canvas_key: str = get_canvas_key(project_id, canvas_uuid)
                content = client.get(canvas_key)
            except redis.exceptions.ConnectionError as e:
                log.error(f"Redis connection error: {e}")
                self.context.sio.emit(
                    event=SioEvents.chat_canvas_error,
                    data={'error': str(e)},
                    room=sid
                )
            else:
                if content is None:
                    canvas_latest_version: CanvasVersionItem = session.query(
                        CanvasVersionItem
                    ).join(
                        CanvasMessageItem,
                        CanvasVersionItem.canvas_item_id == CanvasMessageItem.id
                    ).filter(
                        CanvasMessageItem.uuid == canvas_uuid
                    ).order_by(
                        desc(CanvasVersionItem.created_at)
                    ).first()

                    if canvas_latest_version:
                        content = canvas_latest_version.canvas_content
                        client.set(canvas_key, content)
                        client.expire(canvas_key, timedelta(seconds=CANVAS_CONTENT_TTL))

                        canvas_shadow_key: str = get_shadow_key(canvas_key)
                        client.set(canvas_shadow_key, "")
                        client.expire(
                            canvas_shadow_key, timedelta(seconds=CANVAS_CONTENT_TTL - CANVAS_SHADOW_KEY_OFFSET_TTL)
                        )

                        canvas_authors_key: str = get_canvas_authors_key(project_id, canvas_uuid)
                        current_editors = client.smembers(canvas_authors_key)

                        author_participant, _ = get_or_create_one(
                            session=session,
                            entity_name=ParticipantTypes.user,
                            entity_meta=ParticipantEntityUser(id=current_user['id'])
                        )

                        if not current_editors or author_participant.id not in current_editors:
                            client.sadd(canvas_authors_key, author_participant.id)
                            client.expire(canvas_authors_key, timedelta(seconds=CANVAS_CONTENT_TTL))

                            canvas_authors_shadow_key: str = get_shadow_key(canvas_authors_key)
                            client.sadd(canvas_authors_shadow_key, "")
                            client.expire(
                                canvas_authors_shadow_key, timedelta(seconds=CANVAS_CONTENT_TTL - CANVAS_SHADOW_KEY_OFFSET_TTL)
                            )

                            user_participant = get_entity_details(
                                entity_name=ParticipantTypes.user,
                                entity_meta=current_user
                            )

                        self.context.sio.emit(
                            event=SioEvents.chat_canvas_editors_change.value,
                            data={
                                'editors': [user_participant],
                                'message_group_uuid': str(canvas_latest_version.canvas_item.message_group.uuid),
                                'canvas_uuid': str(canvas_latest_version.canvas_item.uuid)
                            },
                            room=get_chat_room(canvas_latest_version.canvas_item.message_group.conversation.uuid),
                            skip_sid=sid
                        )
                    else:
                        self.context.sio.emit(
                            event=SioEvents.chat_canvas_error,
                            data={'error': 'No such canvas was found'},
                            room=sid
                        )
                        return

                self.context.sio.emit(
                    event=SioEvents.chat_canvas_detail,
                    data={'content': content},
                    room=sid
                )

    @web.sio(SioEvents.chat_canvas_edit)
    def edit_canvas(self, sid: str, data: dict):
        try:
            parsed = EditCanvasPayload.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_canvas_edit.value,
                error=e.errors(include_url=False, include_context=False),
            )
        current_user = auth.current_user(
            auth_data=auth.sio_users[sid]
        )
        if not current_user.get('id'):
            log.error(f"Current_user is not valid: {current_user}")
            self.context.sio.emit(
                event=SioEvents.chat_canvas_error,
                data={'error': f"Current_user is not valid: {current_user}"},
                room=sid
            )
            return

        if not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: return valid error or raise SioValidationError

        canvas_uuid = parsed.canvas_uuid
        project_id = parsed.project_id
        room = get_canvas_room(canvas_uuid)
        content = parsed.content

        try:
            client = self.get_redis_client()

            canvas_authors_key: str = get_canvas_authors_key(project_id, canvas_uuid)
            current_editors = client.smembers(canvas_authors_key)
            # log.debug(f'{current_editors=}')
            # remove when done with collaborative feature
            with db.get_session(project_id) as session:
                author_participant, _ = get_or_create_one(
                    session=session,
                    entity_name=ParticipantTypes.user,
                    entity_meta=ParticipantEntityUser(id=current_user['id'])
                )
                if current_editors and str(author_participant.id) not in current_editors:
                    log.error(f"Canvas is locked by: {list(current_editors)=}")
                    self.context.sio.emit(
                        event=SioEvents.chat_canvas_error,
                        data={'error': 'Canvas is locked', 'current_editors': list(current_editors)},
                        room=sid
                    )
                    return
                if not current_editors or str(author_participant.id)not in current_editors:
                    client.sadd(canvas_authors_key, author_participant.id)
                    client.expire(canvas_authors_key, timedelta(seconds=CANVAS_CONTENT_TTL))

                    canvas_authors_shadow_key: str = get_shadow_key(canvas_authors_key)
                    client.sadd(canvas_authors_shadow_key, "")
                    client.expire(
                        canvas_authors_shadow_key, timedelta(seconds=CANVAS_CONTENT_TTL - CANVAS_SHADOW_KEY_OFFSET_TTL)
                    )
                    user_participant = get_entity_details(
                        entity_name=ParticipantTypes.user,
                        entity_meta=current_user
                    )
                    conversation_uuid, _ = session.query(Conversation.uuid, CanvasMessageItem.uuid).where(
                        CanvasMessageItem.uuid == canvas_uuid
                    ).first()
                    self.context.sio.emit(
                        event=SioEvents.chat_canvas_editors_change.value,
                        data={'editors': [user_participant]},
                        room=get_chat_room(conversation_uuid),
                        skip_sid=sid
                    )

            canvas_key: str = get_canvas_key(project_id, canvas_uuid)
            client.set(canvas_key, content)
            client.expire(canvas_key, timedelta(seconds=CANVAS_CONTENT_TTL))

            canvas_shadow_key: str = get_shadow_key(canvas_key)
            client.set(canvas_shadow_key, "")
            client.expire(canvas_shadow_key, timedelta(seconds=CANVAS_CONTENT_TTL - CANVAS_SHADOW_KEY_OFFSET_TTL))

        except redis.exceptions.ConnectionError as e:
            log.error(f"Redis connection error: {e}")
            self.context.sio.emit(
                event=SioEvents.chat_canvas_error,
                data={'error': str(e)},
                room=sid
            )
        else:
            self.context.sio.emit(
                event=SioEvents.chat_canvas_sync,
                data={'content': content},
                room=room,
                skip_sid=sid
            )

    @web.sio(SioEvents.chat_canvas_leave_rooms)
    def canvas_leave_room(self, sid: str, data: dict):
        assert isinstance(data, (dict, list)), 'Expected dict or list, got {}'.format(type(data))
        if isinstance(data, dict):
            data = [data]
        try:
            parsed_list = [CanvasLeavePayload.model_validate(i) for i in data]
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=SioEvents.chat_canvas_leave_rooms.value,
                error=e.errors(include_url=False, include_context=False),
            )
        for item in parsed_list:
            if not auth.is_sio_user_in_project(sid, item.project_id):
                log.warning("Sid %s is not in project %s", sid, item.project_id)
                continue
            project_id = item.project_id
            canvas_uuid = item.canvas_uuid
            canvas_content = item.canvas_content
            code_language = item.code_language
            if canvas_uuid:
                redis_client = self.get_redis_client()

                canvas_authors_key: str = get_canvas_authors_key(project_id, canvas_uuid)
                current_editors = redis_client.smembers(canvas_authors_key)

                current_user = auth.current_user(
                    auth_data=auth.sio_users[sid]
                )
                with db.get_session(project_id) as session:
                    author_participant, _ = get_or_create_one(
                        session=session,
                        entity_name=ParticipantTypes.user,
                        entity_meta=ParticipantEntityUser(id=current_user['id'])
                    )

                # log.debug(f'{current_editors=}')
                # log.debug(f'{author_participant.id=}')

                last_editor_leaves = len(current_editors) == 1 and current_editors.pop() == str(author_participant.id)
                # log.debug(f'{last_editor_leaves=}')

                if last_editor_leaves:
                    with db.get_session(project_id) as session:
                        canvas_item = session.query(CanvasMessageItem).where(
                            CanvasMessageItem.uuid == canvas_uuid
                        ).first()
                        if not canvas_item.latest_version or canvas_item.latest_version.canvas_content != canvas_content:
                            canvas_new_version = CanvasVersionItem(
                                canvas_item=canvas_item,
                                canvas_content=canvas_content,
                                code_language=code_language,
                            )
                            session.add(canvas_new_version)
                            session.commit()

                        self.canvas_emit_to_conversation(
                            conversation_uuid=str(canvas_item.message_group.conversation.uuid),
                            canvas_uuid=str(canvas_item.uuid),
                            message_group_uuid=str(canvas_item.message_group.uuid),
                            current_editors=[],
                            content=canvas_content
                        )

                    # clear redis content
                    canvas_key: str = get_canvas_key(project_id, canvas_uuid)
                    canvas_shadow_key = get_shadow_key(canvas_key)
                    redis_client.delete(canvas_shadow_key)
                    redis_client.delete(canvas_key)

                    # clear redis editors
                    canvas_authors_key: str = get_canvas_authors_key(project_id, canvas_uuid)
                    canvas_authors_shadow_key: str = get_shadow_key(canvas_authors_key)
                    redis_client.delete(canvas_authors_shadow_key)
                    redis_client.delete(canvas_authors_key)

                room = get_canvas_room(canvas_uuid)
                self.context.sio.leave_room(sid, room)

    # ========================================
    # Application SocketIO Handlers (from applications plugin)
    # ========================================

    @web.sio(SioEvents.application_predict)
    def application_predict(self, sid: str, data: dict):
        """ Event handler for application predictions """
        self.predict_sio(
            sid, data, SioEvents.application_predict
        )

    @web.sio(SioEvents.application_continue_message)
    def application_continue_message(self, sid: str, data: dict):
        """ Event handler for application continue prediction """
        return continue_message(sid, data)

    @web.sio("test_toolkit_tool")
    def test_toolkit_tool(self, sid: str, data: dict):
        """ Event handler for testing toolkit tools """
        self.test_toolkit_tool_sio(
            sid, data, "test_toolkit_tool"
        )

    @web.sio("test_mcp_connection")
    def test_mcp_connection(self, sid: str, data: dict):
        """ Event handler for testing MCP server connection """
        self.test_mcp_connection_sio(
            sid, data, "test_mcp_connection"
        )

    @web.sio(SioEvents.application_leave_rooms)
    def application_leave_rooms(self, sid, data):
        """ Leave application prediction rooms """
        for room_id in data:
            room = get_event_room(
                event_name=SioEvents.application_predict,
                room_id=room_id
            )
            self.context.sio.leave_room(sid, room)
