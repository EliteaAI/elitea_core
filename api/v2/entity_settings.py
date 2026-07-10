from typing import Optional

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, db, config as c, register_openapi

from ...models.all import ApplicationVersion
from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.participants import Participant, ParticipantMapping
from ...models.pd.participant import ParticipantBase, ParticipantEntityUser
from ...models.pd.participant_settings import EntitySettingsLlm, EntitySettingsApplication
from ...utils.entity_settings_utils import coerce_version_id
from ...utils.participant_utils import make_query_filter_for_entity
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents
from ...utils.utils import get_public_project_id


class PromptLibAPI(api_tools.APIModeHandler):
    def _put(self, project_id: int, conversation_id: int, participant_id: int, **kwargs):
        """Update entity_settings for a participant in a conversation."""
        with db.get_session(project_id) as session:
            data = dict(request.json)

            # Stored verbatim in entity_settings JSONB; coerce so it matches the
            # integer ApplicationVersion.id during version resolution.
            try:
                coerce_version_id(data)
            except (TypeError, ValueError):
                return {"error": "version_id must be an integer"}, 400

            participant = session.query(Participant).filter(
                Participant.id == participant_id
            ).first()
            if participant is None:
                return {"error": "Participant was not found"}, 400

            # Validate llm_settings based on participant type
            if llm_settings_data := data.get('llm_settings'):
                if participant.entity_name == ParticipantTypes.application:
                    agent_project_id = (participant.entity_meta or {}).get('project_id')
                    public_project_id = get_public_project_id()
                    if agent_project_id != public_project_id:
                        # Non-published agent: only reject if llm_settings actually
                        # differs from the target version baseline.
                        try:
                            validated_request = EntitySettingsLlm.model_validate(llm_settings_data)
                            request_llm = validated_request.model_dump(exclude_none=True)
                        except ValidationError as e:
                            return {"error": f"Invalid LLM settings: {str(e)}"}, 400

                        version_id = data.get('version_id') or data.get('id')
                        version_llm_raw = {}
                        if version_id:
                            version = session.query(ApplicationVersion).filter(
                                ApplicationVersion.id == version_id
                            ).first()
                            if version:
                                version_llm_raw = version.llm_settings or {}

                        baseline = (
                            EntitySettingsLlm.model_validate(version_llm_raw).model_dump(exclude_none=True)
                            if version_llm_raw else {}
                        )

                        if request_llm and request_llm != baseline:
                            return {
                                "error": "LLM settings override is only allowed for published agents from agent studio"
                            }, 400
                        # Same as baseline or empty -> strip (not a real override)
                        data.pop('llm_settings', None)
                    else:
                        # Published agent from public project -> validate and store
                        try:
                            validated_settings = EntitySettingsLlm.model_validate(llm_settings_data)
                            data['llm_settings'] = validated_settings.model_dump()
                        except ValidationError as e:
                            return {"error": f"Invalid LLM settings: {str(e)}"}, 400
                else:
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
        description="Configure per-conversation settings for a participant — LLM model override, agent version, variables, and chat history mode",
        mcp_description="""
        USE to change which version of an agent is active in a conversation, override LLM model parameters for a
        participant, or switch chat history mode.

        DO NOT USE to add a new participant → use add_participants.
        DO NOT USE to change application-level agent configuration → use update_version.

        Critical restriction: LLM settings override only works for published agents from agent studio. Attempting
        to override LLM settings for a private project agent returns HTTP 400.

        Examples:
        1. Switch agent to version 202: { 'version_id': 202 }
        2. Override LLM temperature for an LLM participant: { 'llm_settings': { 'model_name': 'gpt-4o', 'temperature': 0.1 } }
        3. Set chat history to context-managed mode: { 'chat_history_template': 'context_managed' }
        4. Override agent variables: { 'version_id': 101, 'variables': [{ 'name': 'lang', 'value': 'en' }] }
        5. Error: overriding LLM on private agent → HTTP 400 'LLM settings override is only allowed for published agents from agent studio'
        """,
        request_body=EntitySettingsApplication,
        tags=["elitea_core/chat"],
        mcp_tool=True,
        available_to_users=True,
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
