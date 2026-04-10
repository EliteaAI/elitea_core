from collections import defaultdict

import redis
from tools import db, config as c
from pylon.core.tools import web, log

from ..models.enums.all import ParticipantTypes
from ..models.message_items.canvas import CanvasMessageItem, CanvasVersionItem
from ..utils.canvas_utils import (get_list_canvas_details, get_canvas_details,
                                  get_origin_key_by_shadow, get_canvas_authors_key)
from ..utils.participant_utils import get_entity_details
from ..utils.sio_utils import get_chat_room
from ..utils.sio_utils import SioEvents


class RPC:
    @web.rpc("chat_canvas_emit_to_conversation", "canvas_emit_to_conversation")
    def canvas_emit_to_conversation(
            self, conversation_uuid: str, canvas_uuid: str,
            message_group_uuid: str, current_editors: list[str], content: str
    ):
        self.context.sio.emit(
            event=SioEvents.chat_canvas_editors_change.value,
            data={
                'editors': [
                    get_entity_details(
                        entity_name=ParticipantTypes.user,
                        entity_meta={'id': editor_id}
                    ) for editor_id in current_editors
                ],
                'message_group_uuid': message_group_uuid,
                'canvas_uuid': canvas_uuid
            },
            room=get_chat_room(conversation_uuid)
        )
        self.context.sio.emit(
            event=SioEvents.chat_canvas_content_change.value,
            data={
                'content': content,
                'message_group_uuid': message_group_uuid,
                'canvas_uuid': canvas_uuid
            },
            room=get_chat_room(conversation_uuid)
        )

    @web.rpc("chat_canvas_save_versions")
    def chat_canvas_save_versions(self, **kwargs):
        # TODO keep the last N versions instead of all?
        redis_client = self.get_redis_client()
        in_memory_canvas_keys: list[str] = redis_client.keys('canvas:*')
        parsed_in_memory_canvas_keys: list[dict] = get_list_canvas_details(in_memory_canvas_keys, shadow=False)
        grouped_canvases = defaultdict(list)

        for canvas_key_parsed in parsed_in_memory_canvas_keys:
            grouped_canvases[canvas_key_parsed['project_id']].append({
                'canvas_uuid': canvas_key_parsed['canvas_uuid'],
                'content': redis_client.get(canvas_key_parsed['key'])
            })

        for project_id, canvas_details_list in grouped_canvases.items():
            with db.get_session(project_id) as session:
                new_canvas_versions: list[CanvasVersionItem] = []

                for canvas_details in canvas_details_list:
                    canvas: CanvasMessageItem = session.query(CanvasMessageItem).filter(
                        CanvasMessageItem.uuid == canvas_details["canvas_uuid"]
                    ).first()
                    if not canvas:
                        log.error(f'Can not find the canvas with uuid {canvas_details["canvas_uuid"]}')
                        continue

                    if not canvas.latest_version or canvas.latest_version.canvas_content != canvas_details["content"]:
                        canvas_version: CanvasVersionItem = CanvasVersionItem(
                            canvas_content=canvas_details['content'],
                            canvas_item_id=canvas.id,
                            code_language=canvas.latest_version.code_language
                        )
                        new_canvas_versions.append(canvas_version)
                    else:
                        log.debug(f'Skipping the canvas content with uuid {canvas_details["canvas_uuid"]}')

                    canvas_authors_key: str = get_canvas_authors_key(canvas_details["project_id"], str(canvas.uuid))
                    current_editors = redis_client.smembers(canvas_authors_key)
                    self.canvas_emit_to_conversation(
                        conversation_uuid=str(canvas.message_group.conversation.uuid),
                        canvas_uuid=str(canvas.uuid),
                        message_group_uuid=str(canvas.message_group.uuid),
                        current_editors=list(current_editors),
                        content=canvas_details['content']
                    )
                session.add_all(new_canvas_versions)
                session.commit()

    @web.rpc("chat_canvas_save_expired_version", "canvas_save_expired_version")
    def canvas_save_expired_version(self, canvas_key: str):
        redis_client = self.get_redis_client()
        canvas_details: dict = get_canvas_details(canvas_key)
        if canvas_details and canvas_details['is_shadow']:
            origin_canvas_key: str = get_origin_key_by_shadow(canvas_key)
            canvas_content: str = redis_client.get(origin_canvas_key)

            with db.get_session(canvas_details['project_id']) as session:
                canvas: CanvasMessageItem = session.query(CanvasMessageItem).filter(
                    CanvasMessageItem.uuid == canvas_details["canvas_uuid"]
                ).first()
                if not canvas:
                    log.error(f'Can not find the canvas with uuid {canvas_details["canvas_uuid"]}')
                    return

                if not canvas.latest_version or canvas.latest_version.canvas_content != canvas_content:
                    canvas_version: CanvasVersionItem = CanvasVersionItem(
                        canvas_content=canvas_content,
                        canvas_item_id=canvas.id,
                        code_language=canvas.latest_version.code_language
                    )
                    session.add(canvas_version)
                    session.commit()
                else:
                    log.debug(f'Skipping the canvas expired content with uuid {canvas_details["canvas_uuid"]}')

                canvas_authors_key: str = get_canvas_authors_key(canvas_details["project_id"], str(canvas.uuid))
                current_editors = redis_client.smembers(canvas_authors_key)
                self.canvas_emit_to_conversation(
                    conversation_uuid=str(canvas.message_group.conversation.uuid),
                    canvas_uuid=str(canvas.uuid),
                    message_group_uuid=str(canvas.message_group.uuid),
                    current_editors=list(current_editors),
                    content=canvas_content
                )
