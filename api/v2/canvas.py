from flask import request
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from pylon.core.tools import log
from tools import api_tools, auth, db, config as c
from tools import serialize

from ...models.message_items.canvas import CanvasMessageItem
from ...utils.sio_utils import get_canvas_room
from ...utils.sio_utils import SioEvents
from ...utils.constants import PROMPT_LIB_MODE
from ...models.pd.canvas import CanvasItemEditPayload, CanvasItemDetail



class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.canvas.details"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, canvas_uuid: str):
        with db.get_session(project_id) as session:
            canvas_q: CanvasMessageItem = session.query(
                CanvasMessageItem
            ).filter(
                CanvasMessageItem.uuid == canvas_uuid,
            )
            canvas = canvas_q.first()

            if not canvas:
                return serialize({
                    "error": f"Canvas with uuid {canvas_uuid} was not found"
                }), 400

            return serialize(CanvasItemDetail.from_orm(canvas)), 200

    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.canvas.update"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, canvas_uuid: str):
        raw = dict(request.json)
        try:
            parsed = CanvasItemEditPayload.model_validate(raw)
        except ValidationError as e:
            log.error(f'Validation error on canvas {canvas_uuid}: {e.errors()}')
            return serialize({"error": f"Validation failed {e.errors()}"}), 400

        with db.get_session(project_id) as session:
            try:
                canvas: CanvasMessageItem = session.query(CanvasMessageItem).filter(
                    CanvasMessageItem.uuid==canvas_uuid
                ).first()

                if not canvas:
                    return serialize({
                        "error": f"Canvas with uuid '{canvas_uuid}' was not found"
                    }), 400

                # Update the canvas name if provided
                dumped = parsed.model_dump(exclude_unset=True)
                if "name" in dumped:
                    canvas.name = parsed.name

                # Update the code_language in the latest version if provided
                if 'code_language' in dumped:
                    if not canvas.latest_version:
                        log.warning("No latest_version found to set code_language.")
                    else:
                        canvas.latest_version.code_language = parsed.code_language

                session.commit()

            except IntegrityError as e:
                session.rollback()
                log.error(f"Integrity error while updating canvas {canvas_uuid}: {str(e)}")
                return serialize({
                    "error": f"Failed to update canvas with uuid '{canvas_uuid}'"
                }), 400

            except SQLAlchemyError as e:
                session.rollback()
                log.error(f"Database error while updating canvas {canvas_uuid}: {str(e)}")
                return serialize({
                    "error": f"Failed to update canvas with uuid '{canvas_uuid}'"
                }), 400

            # Emit socket event
            room = get_canvas_room(canvas_uuid)
            self.module.context.sio.emit(
                event=SioEvents.chat_canvas_sync,
                data={**serialize(parsed), 'content': canvas.latest_version.canvas_content},
                room=room,
            )

            # Optionally refresh if you need the fully updated object
            session.refresh(canvas)
            return serialize(CanvasItemDetail.model_validate(canvas)), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:canvas_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
