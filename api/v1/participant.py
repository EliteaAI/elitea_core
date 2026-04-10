from tools import api_tools, auth, db, config as c, serialize

from ...models.participants import Participant
from ...models.pd.participant import ParticipantDetails
from ...utils.participant_utils import delete_participant_from_conversation
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
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
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:participant_id>',
        '<int:project_id>/<int:conversation_id>/<int:participant_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
