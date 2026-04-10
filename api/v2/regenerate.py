from flask import request
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified
from pydantic import ValidationError

from tools import api_tools, auth, db, config as c, serialize

from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...models.pd.predict import SioRegenerateModel, SioPredictModel
from ...rpc.chat_all import CHAT_PREDICT_MAPPER, prepare_conversation_history, generate_payload
from ...utils.chat_history import generate_chat_history
from ...models.enums.all import ChatHistoryRole
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents


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
        try:
            parsed = SioRegenerateModel.model_validate(raw)
        except ValidationError as e:
            return {'error': 'Invalid request payload', 'details': e.errors()}, 400

        with db.get_session(project_id) as session:
            msg_group = session.query(ConversationMessageGroup).options(
                joinedload(ConversationMessageGroup.reply_to)
            ).filter(
                ConversationMessageGroup.uuid == message_group_uuid
            ).first()

            reply_msg = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id == msg_group.reply_to_id
            ).first()

            msg_entity_meta = msg_group.author_participant.entity_meta

            raw_predict_payload = {**parsed.model_dump(), **parsed.payload}
            try:
                predict_payload = SioPredictModel.model_validate(raw_predict_payload)
            except ValidationError as e:
                return {'error': 'Invalid prediction payload', 'details': e.errors()}, 400
            regenerate_payload: dict = generate_payload(session, msg_group=reply_msg, predict_payload=predict_payload)
            regenerate_payload['is_regenerate'] = True

            chat_history_groups, summaries, preserve_instructions = prepare_conversation_history(
                session, self.module.context.sio,
                reply_msg.conversation, reply_msg,
            )

            regenerate_payload['chat_history'] = generate_chat_history(
                message_groups=chat_history_groups, summaries=summaries
            )
            if not preserve_instructions:
                regenerate_payload['instructions'] = None

            regenerate_payload['message_id'] = str(msg_group.uuid)
            regenerate_payload['project_id'] = msg_entity_meta.get('project_id') or project_id

            if not parsed.payload.get('user_input'):
                regenerate_payload['user_input'] = reply_msg.message_items[-1].content
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

            msg_group.meta['thinking_steps'] = []
            msg_group.meta['tool_calls'] = {}
            msg_group.is_streaming = True
            flag_modified(msg_group, 'meta')
            session.commit()
            session.refresh(msg_group)

            rpc_func = CHAT_PREDICT_MAPPER.get(msg_group.author_participant.entity_name)
            if rpc_func:
                getattr(self.module.context.rpc_manager.call, rpc_func)(
                    parsed.sid, regenerate_payload, SioEvents.chat_predict.value,
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
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
