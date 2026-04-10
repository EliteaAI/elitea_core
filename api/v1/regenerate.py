from flask import request
from tools import api_tools, auth, db, config as c, serialize

from sqlalchemy.orm import joinedload

from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...models.pd.predict import SioRegenerateModel
from ...rpc.chat_all import CHAT_PREDICT_MAPPER, generate_toolkit_payload
from ...utils.chat_history import generate_chat_history
from ...models.enums.all import ChatHistoryRole
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents

from pylon.core.tools import log

class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.regenerate"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, message_group_uuid: str, **kwargs):
        raw = dict(request.json)
        parsed = SioRegenerateModel.model_validate(raw)

        with db.get_session(project_id) as session:
            msg_group = session.query(ConversationMessageGroup).options(
                joinedload(ConversationMessageGroup.reply_to)
            ).filter(
                ConversationMessageGroup.uuid == message_group_uuid
            ).first()

            message_groups = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == msg_group.conversation_id,
                ConversationMessageGroup.created_at < msg_group.created_at,
            ).order_by(
                ConversationMessageGroup.created_at.asc()
            ).all()

            reply_msg = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id == msg_group.reply_to_id
            ).first()

            msg_entity_meta = msg_group.author_participant.entity_meta

            parsed.payload['chat_history'] = generate_chat_history(message_groups)
            parsed.payload['message_id'] = str(msg_group.uuid)
            parsed.payload['project_id'] = msg_entity_meta.get('project_id') or project_id

            if not parsed.payload.get('user_input'):
                parsed.payload['user_input'] = reply_msg.message_items[-1].content
            conversation_author_id = reply_msg.conversation.author_id

            if not msg_group and not msg_group.author_participant:
                return {'error': f'No such message group with id {message_group_uuid}'}, 400

            reply_msg_entity_meta = reply_msg.author_participant.entity_meta

            if not reply_msg.author_participant.entity_name == ChatHistoryRole.user:
                return {'error': f'You can not regenerate message group with uuid {message_group_uuid}'}, 400

            if auth.current_user().get("id") not in (conversation_author_id, reply_msg_entity_meta.get("id")):
                return {'error': f'You can not regenerate message group with uuid {message_group_uuid}'}, 400

            # remove outdated message items
            for message_item in msg_group.message_items:
                session.delete(message_item)

            msg_group.is_streaming = True
            session.commit()
            session.refresh(msg_group)

            parsed.payload['tools'] = generate_toolkit_payload(
                session=session,
                conversation_uuid=reply_msg.conversation.uuid,
                user_id=reply_msg.author_participant.entity_meta['id'],
                conversation_project_id=project_id
            )

            rpc_func = CHAT_PREDICT_MAPPER.get(msg_group.author_participant.entity_name)
            if rpc_func:
                getattr(self.module.context.rpc_manager.call, rpc_func)(
                    parsed.sid, parsed.payload, SioEvents.chat_predict.value,
                    start_event_content={
                        'participant_id': msg_group.author_participant_id,
                        'question_id': parsed.question_id,
                    },
                    chat_project_id=project_id
                )
                # load new regenerated message items
                session.refresh(msg_group)
                return serialize(MessageGroupDetail.model_validate(msg_group)), 200
            return None


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
