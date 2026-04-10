from typing import Optional

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, db, config as c, register_openapi

from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.participants import Participant, ParticipantMapping
from ...models.pd.participant import ParticipantBase, ParticipantEntityUser
from ...models.pd.participant_settings import EntitySettingsLlm
from ...utils.participant_utils import make_query_filter_for_entity
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents


class PromptLibAPI(api_tools.APIModeHandler):
    def _put(self, project_id: int, conversation_id: int, participant_id: int, **kwargs):
        """
        Update entity_settings for a participant in a conversation.

        Note: llm_settings cannot be updated for application/pipeline participants.
        They must be changed at the version level.
        """
        with db.get_session(project_id) as session:
            data = dict(request.json)

            participant = session.query(Participant).filter(
                Participant.id == participant_id
            ).first()
            if participant is None:
                return {"error": "Participant was not found"}, 400

            # Validate llm_settings for non-application participants
            if participant.entity_name != ParticipantTypes.application:
                if llm_settings_data := data.get('llm_settings'):
                    try:
                        validated_settings = EntitySettingsLlm.model_validate(llm_settings_data)
                        data['llm_settings'] = validated_settings.model_dump()
                    except ValidationError as e:
                        return {"error": f"Invalid LLM settings: {str(e)}"}, 400

            session.query(ParticipantMapping).filter(
                ParticipantMapping.conversation_id == conversation_id,
                ParticipantMapping.participant_id == participant.id
            ).update({'entity_settings': data})
            session.commit()

            update_result = ParticipantBase.model_validate(participant).model_dump()
            update_result.update({'entity_settings': data})

            conversation = session.query(
                Conversation
            ).filter(Conversation.id == conversation_id).first()

            room = get_chat_room(conversation.uuid)

            self.module.context.sio.emit(
                event=SioEvents.chat_participant_update,
                data=update_result,
                room=room,
            )

            return update_result, 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.entity_settings.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    def put(self, project_id: int, conversation_id: int, participant_id: Optional[int] = None, **kwargs):
        return self._put(
            project_id=project_id,
            conversation_id=conversation_id,
            participant_id=participant_id,
            **kwargs
        )

    @register_openapi(
        name="Configure Participant",
        description="Configure participant settings (LLM settings, etc.) in a conversation.",
        mcp_tool=True
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.entity_settings.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    def patch(self, project_id: int, conversation_id: int, participant_id: Optional[int] = None, **kwargs):
        if participant_id is None:
            with db.get_session(project_id) as session:
                flt = make_query_filter_for_entity(
                    entity_name=ParticipantTypes.user,
                    entity_meta=ParticipantEntityUser(
                        id=auth.current_user().get('id')
                    ))
                p: Participant = session.query(Participant).where(
                    Participant.entity_name == ParticipantTypes.user.value,
                    *flt
                ).first()
                if not p:
                    return {"error": "Participant was not found"}, 400
                participant_id = p.id
        return self._put(project_id, conversation_id, participant_id, **kwargs)


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>/<int:participant_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
