from tools import api_tools, auth, db, config as c, serialize, register_openapi

from ...models.participants import Participant
from ...models.pd.participant import ParticipantDetails
from ...utils.participant_utils import delete_participant_from_conversation
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Participant",
        description="Get details of a specific participant.",
        mcp_description="""
        USE to inspect a participant's current configuration in a conversation — including which agent version is 
        active, LLM settings overrides, and entity metadata.
        
        DO NOT USE to list all participants in a conversation → that information is included in get_conversation. 
        DO NOT USE to update participant settings → use configure_participant.
        
        Participant type guide (entity_name):
        - 'application' → an agent or pipeline; entity_meta.id = application_id
        - 'llm' → a bare LLM; entity_meta.model_name = model identifier
        - 'user' → a human user; entity_meta.id = user_id
        - 'toolkit' → a toolkit; entity_meta.id = toolkit_id
        Examples:
        1. Get agent participant details: GET .../participant/prompt_lib/42/7/15 → check entity_settings.version_id 
        to see which version is active.
        2. Check LLM override in use: inspect entity_settings.llm_settings in the response.
        """,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.participant.get"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, participant_id: int, **kwargs):
        with db.get_session(project_id) as session:
            participant: Participant = session.query(Participant).where(
                Participant.id == participant_id
            ).first()
            if not participant:
                return {"error": f"Participant with id {participant_id} was not found"}, 400
            return serialize(ParticipantDetails.model_validate(participant))

    @register_openapi(
        name="Remove Participant",
        description="Remove a participant from a conversation.",
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.participant.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int, participant_id: int, **kwargs):
        ret, code = delete_participant_from_conversation(project_id, conversation_id, participant_id)
        return ret, code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:participant_id>',
        '<int:project_id>/<int:conversation_id>/<int:participant_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
