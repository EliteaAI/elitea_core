from pylon.core.tools import log
from tools import api_tools, auth, db, config as c
from tools import serialize

from ...models.all import SelectedConversations
from ...models.conversation import Conversation
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int):
        user_id = auth.current_user()['id']

        with db.get_session(project_id) as session:
            conversation_exists = session.query(Conversation.id).filter(
                Conversation.id == conversation_id
            ).scalar()
            if not conversation_exists:
                return serialize({"error": f"No such conversation with id {conversation_id}"}), 400

            selection = (
                session.query(SelectedConversations)
                .filter(SelectedConversations.user_id == user_id)
                .with_for_update()
                .first()
            )
            if selection is None:
                selection = SelectedConversations(user_id=user_id, conversation_id=conversation_id)
                session.add(selection)
            else:
                selection.conversation_id = conversation_id
            session.commit()
            return serialize(selection), 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int):
        user_id = auth.current_user()['id']
        with db.get_session(project_id) as session:
            existing_selection = session.query(SelectedConversations).filter(
                SelectedConversations.user_id == user_id
            ).first()

            if existing_selection:
                session.delete(existing_selection)
                session.commit()
            return serialize({"ok": True}), 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
