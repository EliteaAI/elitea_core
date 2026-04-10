from sqlalchemy.orm import joinedload

from .sio_utils import SioEvents
from ..models.message_group import ConversationMessageGroup
from ..models.pd.message import MessageGroupDetail

from tools import db, context, serialize, auth
from pylon.core.tools import log

from ..rpc.chat_all import CHAT_PREDICT_MAPPER


def continue_message(sid: str, payload: dict):
    message_group_uuid = payload['message_id']
    project_id = payload['project_id']

    if not auth.is_sio_user_in_project(sid, project_id):
        log.warning("Sid %s is not in project %s", sid, project_id)
        return  # FIXME: need some proper error?

    with db.get_session(project_id) as session:
        msg_group = session.query(ConversationMessageGroup).options(
            joinedload(ConversationMessageGroup.reply_to)
        ).filter(
            ConversationMessageGroup.uuid == message_group_uuid
        ).first()

        if not msg_group:
            return {'error': f'No such message group with id {message_group_uuid}'}, 400

        msg_entity_meta = msg_group.author_participant.entity_meta

        payload['project_id'] = msg_entity_meta.get('project_id') or project_id
        payload['should_continue'] = True

        msg_group.is_streaming = True
        session.commit()
        session.refresh(msg_group)

        rpc_func = CHAT_PREDICT_MAPPER.get(msg_group.author_participant.entity_name)
        if rpc_func:
            getattr(context.rpc_manager.call, rpc_func)(
                sid, payload, SioEvents.chat_predict.value,
                start_event_content={
                    'participant_id': msg_group.author_participant_id,
                    'question_id': payload['question_id'],
                },
                chat_project_id=project_id
            )
            session.refresh(msg_group)
            return serialize(MessageGroupDetail.model_validate(msg_group)), 200
        return None
