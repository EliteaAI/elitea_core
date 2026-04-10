from flask import request
from sqlalchemy import desc, asc
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
import re

from pylon.core.tools import log
from sqlalchemy.orm import joinedload
from tools import api_tools, auth, db, config as c, serialize

from ...models.message_items.canvas import CanvasMessageItem, CanvasVersionItem
from ...models.message_items.text import TextMessageItem
from ...models.pd.message import MessageGroupDetail
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...models.pd.canvas import CanvasItemDetail, CanvasItemCreatePayload
from ...utils.sio_utils import SioEvents

markdown_pattern = re.compile(r"```(\w+)?\n(.*?)```\s*$", re.DOTALL)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.canvas.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int):
        limit = request.args.get('limit', default=10, type=int)
        offset = request.args.get('offset', default=0, type=int)
        sort_by = request.args.get('sort_by', default='created_at')
        sorting_by = getattr(CanvasMessageItem, sort_by)
        sort_order = request.args.get('sort_order', default='desc')
        sorting = desc if sort_order == 'desc' else asc

        with db.get_session(project_id) as session:
            canvas_q: CanvasMessageItem = session.query(CanvasMessageItem)
            canvas_total = canvas_q.count()
            canvas_result = canvas_q.order_by(sorting(sorting_by)).limit(limit).offset(offset).all()

            rows = [
                {
                    **serialize(CanvasItemDetail.from_orm(c)),
                } for c in canvas_result
            ]

            return {
                'total': canvas_total,
                'rows': rows
            }, 200

    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.canvas.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        raw = dict(request.json)
        try:
            parsed = CanvasItemCreatePayload.model_validate(raw)
        except ValidationError as e:
            log.error(f'Validation error on canvas: {e.errors()}')
            return serialize({"error": f"Validation failed {e.errors()}"}), 400

        with db.get_session(project_id) as session:
            old_message_item: TextMessageItem = session.query(TextMessageItem).options(
                joinedload(TextMessageItem.message_group)
            ).filter(
                TextMessageItem.id == parsed.message_item_id,
                TextMessageItem.message_group_id == parsed.message_group_id,
            ).first()

            if old_message_item is None:
                return {"error": "No such message in message group"}, 400

            old_content = old_message_item.content

            start = parsed.canvas_content_starts_at
            end = parsed.canvas_content_ends_at

            order_index = old_message_item.order_index
            pre_canvas_content = old_content[:start]
            canvas_content = old_content[start:end]

            if canvas_content.startswith("```"):
                match = re.match(markdown_pattern, canvas_content)
                if match:
                    code_language = match.group(1) or None
                    if code_language:
                        parsed.code_language = code_language
                    canvas_content = match.group(2)
            post_canvas_content = old_content[end:]

            if pre_canvas_content:
                new_pre_message_item = TextMessageItem(
                    content=pre_canvas_content,
                    message_group=old_message_item.message_group,
                    order_index=order_index,
                )
                session.add(new_pre_message_item)
                order_index += 1

            try:
                new_canvas: CanvasMessageItem = CanvasMessageItem(
                    name=parsed.name,
                    canvas_type=parsed.canvas_type,
                    meta=parsed.meta,
                    message_group=old_message_item.message_group,
                    order_index=order_index,
                    versions=[
                        CanvasVersionItem(
                            code_language=parsed.code_language,
                            canvas_content=canvas_content,
                        )
                    ]
                )
                session.add(new_canvas)
                order_index += 1

                if post_canvas_content:
                    new_post_message_item = TextMessageItem(
                        content=post_canvas_content,
                        message_group=old_message_item.message_group,
                        order_index=order_index
                    )
                    session.add(new_post_message_item)
                    order_index += 1

                for i in old_message_item.message_group.message_items:
                    if i.order_index < old_message_item.order_index:
                        i.order_index = order_index
                        order_index += 1

                old_message_group = old_message_item.message_group

                session.delete(old_message_item)
                session.commit()
                session.refresh(old_message_group)
            except SQLAlchemyError as e:
                session.rollback()
                log.error(f"Database error while create canvas: {e}")
                return {
                    "error": f"Failed to create canvas"
                }, 400

            room = get_chat_room(old_message_group.conversation.uuid)
            msg_group = MessageGroupDetail.model_validate(old_message_group)

            self.module.context.sio.emit(
                event=SioEvents.chat_message_sync,
                data=serialize(msg_group),
                room=room,
            )
            return serialize(CanvasItemDetail.model_validate(new_canvas)), 200


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
