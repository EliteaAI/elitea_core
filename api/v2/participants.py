from typing import List

from flask import request
from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize

from pydantic import parse_obj_as, ValidationError

from ...models.conversation import Conversation
from ...models.pd.participant import ParticipantBase, ParticipantCreate
from ...utils.participant_utils import add_participant_to_conversation
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Add Participants",
        description="Add participants (users, agents, toolkits) to a conversation.",
        mcp_tool=True
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.participants.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int, **kwargs):
        """
        Add some participants to the conversation
        """
        log.debug(f'Add participants to conversation {conversation_id} in project {project_id}')
        data = request.json
        if isinstance(data, dict):
            data = [request.json]
        elif not isinstance(request.json, list):
            return f'Unsupported data type: {type(request.json)}', 400

        # todo: remove that - this is tmp fix for old entities which did not have project_id
        for i in data:
            i['entity_meta']['project_id'] = i['entity_meta'].get('project_id', project_id)
        try:
            participants = parse_obj_as(List[ParticipantCreate], data)
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:

            conversation = session.query(
                Conversation
            ).filter(Conversation.id == conversation_id).first()

            if not conversation:
                return {'error': f'No such conversation with id {conversation_id}'}, 400

            current_user_id = auth.current_user().get("id")
            room = get_chat_room(conversation.uuid)

            result_details = list()
            for parsed_participant in participants:
                try:
                    result: ParticipantBase = add_participant_to_conversation(
                        participant=parsed_participant,
                        conversation=conversation,
                        session=session,
                        project_id=project_id,
                        initiator_id=current_user_id
                    )
                    result_details.append(result)
                except ValueError as e:
                    return {'error': str(e)}, 400

                self._emit_participant_update(result, room)

            return [serialize(p) for p in result_details], 200

    def _emit_participant_update(self, participant: ParticipantBase, room: str) -> None:
        """Helper method to emit participant update event."""
        self.module.context.sio.emit(
            event=SioEvents.chat_participant_update,
            data=serialize(participant),
            room=room,
        )

class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
