from tools import api_tools, auth, db, config as c
from tools import serialize, this
from pylon.core.tools import log

from flask import request
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import flag_modified

from ...models.enums.all import ParticipantTypes
from ...models.conversation import Conversation
from ...models.participants import ParticipantMapping, Participant
from ...models.pd.conversation import ConversationUpdate, ConversationDetails
from ...utils.conversation_utils import get_conversation_details
from ...utils.constants import PROMPT_LIB_MODE


def check_attachment_participant_id(session, conversation_id: int, attachment_participant_id: int) -> bool:
    """
    Check if the attachment participant ID is valid for the given conversation.
    """
    participant_mapping = session.query(ParticipantMapping).filter(
        ParticipantMapping.conversation_id == conversation_id,
        ParticipantMapping.participant_id == attachment_participant_id
    ).first()
    if not participant_mapping:
        raise ValueError(f"Attachment participant {attachment_participant_id} is not added to conversation {conversation_id}")

    participant = session.query(Participant).filter(
        Participant.id == attachment_participant_id,
    ).first()
    if participant is None:
        raise ValueError(f"Participant with ID {attachment_participant_id} does not exist")
    if participant.entity_name != ParticipantTypes.toolkit.value:
        raise ValueError(f"Participant with ID {attachment_participant_id} is not a toolkit participant")

    toolkit_details = this.module.get_toolkit_by_id(
        project_id=participant.entity_meta['project_id'],
        toolkit_id=participant.entity_meta['id']
    )
    if toolkit_details.get('type') != 'artifact':
        raise ValueError(f"Participant with ID {attachment_participant_id} is not an artifact participant")

    return True


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int = None, **kwargs):
        with db.get_session(project_id) as session:
            user_id = auth.current_user().get("id")
            conversation: ConversationDetails = get_conversation_details(session, conversation_id, project_id, user_id)

            if not conversation:
                return {'error': f'No such conversation with id {conversation_id}'}, 400

            result = serialize(conversation)

            return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, conversation_id: int):
        data = dict(request.json)
        log.debug(f"Update conversation {conversation_id} with data: {data}")
        try:
            parsed = ConversationUpdate.parse_obj(data)
            with db.get_session(project_id) as session:
                conversation_q = session.query(Conversation).filter(Conversation.id == conversation_id)

                conversation = conversation_q.first()
                if conversation is None:
                    return {"error": "Conversation not found"}, 404

                if parsed.is_private and not conversation.is_private:
                    return {"error": "Public conversation can not be changed to private"}, 400

                from ...utils.utils import get_public_project_id  # pylint: disable=C0415
                public_project_id = get_public_project_id()
                if not parsed.is_private and conversation.is_private and public_project_id == project_id:
                    return {"error": "Public conversation can not exist in public project"}, 400

                if parsed.attachment_participant_id is not None:
                    check_attachment_participant_id(
                        session, conversation_id, parsed.attachment_participant_id
                    )

                update_data = parsed.model_dump(exclude_unset=True, exclude={'is_hidden'})
                if update_data:
                    conversation_q.update(update_data)

                if parsed.is_hidden is not None:
                    conversation.meta = conversation.meta or {}
                    conversation.meta['is_hidden'] = parsed.is_hidden
                    flag_modified(conversation, 'meta')

                session.commit()
                session.refresh(conversation)
        except ValidationError as e:
            return e.errors(), 400
        except IntegrityError as e:
            log.error(e)
            return {"error": "Can not update conversation: invalid data provided"}, 400
        except ValueError as e:
            return {"error": str(e)}, 400
        except Exception as e:
            log.error(f"Error updating conversation: {e}")
            return {"error": "Can not update conversation"}, 400
        return serialize(conversation), 200


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
