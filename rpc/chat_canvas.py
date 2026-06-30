from collections import defaultdict

import redis
from tools import db, config as c
from pylon.core.tools import web, log

try:
    import gevent  # pylint: disable=C0413
except ImportError:  # pragma: no cover - gevent absent in non-gevent deploys
    gevent = None

from ..models.enums.all import ParticipantTypes
from ..models.message_items.canvas import CanvasMessageItem, CanvasVersionItem
from ..utils.canvas_utils import (get_list_canvas_details, get_canvas_details,
                                  get_origin_key_by_shadow, get_canvas_authors_key,
                                  get_canvas_key)
from ..utils.canvas_autosave import CanvasAutosave
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
        # Cooperative yield only when gevent is the actual web runtime;
        # under flask/waitress/hypercorn this is a no-op.
        yield_to_hub = (
            (lambda: gevent.sleep(0))
            if (gevent is not None and self.context.web_runtime == "gevent")
            else (lambda: None)
        )

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
            # Yield between projects so a heavy canvas-save tick does not
            # starve the gevent hub and stall other greenlets.
            yield_to_hub()
            with db.get_session(project_id) as session:
                new_canvas_versions: list[CanvasVersionItem] = []

                for canvas_details in canvas_details_list:
                    # Cooperative yield per canvas; emit + content compare are
                    # CPU-bound between DB lookups.
                    yield_to_hub()
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

    @web.rpc("chat_canvas_autosave", "canvas_autosave")
    def canvas_autosave(self, **kwargs):
        """Periodic auto-save: persists dirty canvases that haven't been saved in 5+ minutes.

        Only saves canvases where:
        1. The dirty flag is set (content changed since last save)
        2. At least AUTOSAVE_INTERVAL_SECONDS (300s) have elapsed since last save

        This is complementary to the existing per-minute save_versions cron —
        autosave focuses on ensuring no edit goes unpersisted for more than 5 minutes.
        """
        redis_client = self.get_redis_client()
        autosave = CanvasAutosave(redis_client)

        dirty_canvases = autosave.get_dirty_canvases()
        if not dirty_canvases:
            return

        saved_count = 0
        skipped_count = 0

        for canvas_info in dirty_canvases:
            project_id = canvas_info["project_id"]
            canvas_uuid = canvas_info["canvas_uuid"]

            if not autosave.should_save(project_id, canvas_uuid):
                skipped_count += 1
                continue

            canvas_key = get_canvas_key(int(project_id), canvas_uuid)
            content = redis_client.get(canvas_key)
            if content is None:
                autosave.mark_saved(project_id, canvas_uuid)
                continue

            try:
                with db.get_session(project_id) as session:
                    canvas: CanvasMessageItem = session.query(CanvasMessageItem).filter(
                        CanvasMessageItem.uuid == canvas_uuid
                    ).first()
                    if not canvas:
                        log.warning(f"Canvas autosave: canvas {canvas_uuid} not found in DB")
                        autosave.delete_state(project_id, canvas_uuid)
                        continue

                    if not canvas.latest_version or canvas.latest_version.canvas_content != content:
                        canvas_version = CanvasVersionItem(
                            canvas_content=content,
                            canvas_item_id=canvas.id,
                            code_language=(
                                canvas.latest_version.code_language
                                if canvas.latest_version else None
                            ),
                        )
                        session.add(canvas_version)
                        session.commit()
                        saved_count += 1

                    autosave.mark_saved(project_id, canvas_uuid)

            except Exception as exc:
                log.error(f"Canvas autosave failed for {canvas_uuid}: {exc}")

        if saved_count or skipped_count:
            log.info(
                f"Canvas autosave: saved={saved_count}, skipped={skipped_count}, "
                f"total_dirty={len(dirty_canvases)}"
            )
