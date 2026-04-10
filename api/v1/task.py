from datetime import datetime

from tools import serialize
from tools import api_tools, auth, db, config as c

from ...models.enums.all import ParticipantTypes
from ...models.pd.message import MessageGroupDetail
from ...models.message_items.text import TextMessageItem
from ...models.message_group import ConversationMessageGroup

from ...utils.sio_utils import get_chat_room
from ...utils.sio_utils import SioEvents
from ...utils.constants import PROMPT_LIB_MODE


from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.task.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, message_group_uuid: str, **kwargs):
        with db.get_session(project_id) as session:
            msg_group = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == message_group_uuid
            ).first()

            user_id = auth.current_user().get('id')
            if user_id != msg_group.conversation.author_id:
                author = msg_group.author_participant
                if author.entity_name != ParticipantTypes.user or \
                        user_id != author.entity_meta.get('id'):
                    return {
                        "error": "Message can be stopped only by "
                                 "message or conversation author"
                    }, 400

            self.module.stop_task(msg_group.task_id)

            msg_group.is_streaming = False
            msg_group_deleted = False
            thinking_steps = msg_group.meta.get('thinking_steps', [])

            room = get_chat_room(msg_group.conversation.uuid)

            if not msg_group.message_items:
                filtered_steps = [
                    s for s in thinking_steps
                    if (txt := s.get('text')) and str(txt).strip()
                ]
                latest_step = max(
                    filtered_steps,
                    key=lambda step: datetime.fromisoformat(
                        step['timestamp_finish'].replace('Z', '+00:00')
                    )
                ) if filtered_steps else None

                if latest_step:
                    msg: TextMessageItem = TextMessageItem(
                        content=str(latest_step.get('text')),
                        message_group=msg_group,
                        order_index=0,
                    )
                    session.add(msg)
                else:
                    reply_to_record = session.query(ConversationMessageGroup).filter(
                        ConversationMessageGroup.id == msg_group.reply_to_id
                    ).first()
                    session.delete(reply_to_record)
                    session.delete(msg_group)
                    msg_group_deleted = True
                    self.module.context.sio.emit(
                        event=SioEvents.chat_message_delete,
                        data={
                            'message_group_id': msg_group.id,
                            'message_group_uid': str(msg_group.uuid),
                        },
                        room=room,
                    )
                    self.module.context.sio.emit(
                        event=SioEvents.chat_message_delete,
                        data={
                            'message_group_id': reply_to_record.id,
                            'message_group_uid': str(reply_to_record.uuid),
                        },
                        room=room,
                    )

            session.commit()

            if not msg_group_deleted:
                session.refresh(msg_group)
                msg_group = MessageGroupDetail.model_validate(msg_group)

                self.module.context.sio.emit(
                     event=SioEvents.chat_message_sync,
                     data=serialize(msg_group),
                     room=room,
                )

            return None, 204


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
