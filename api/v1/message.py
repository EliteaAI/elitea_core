from flask import request

from tools import api_tools, auth, db, config as c, MinioClient, serialize
from pylon.core.tools import log

from ...models.enums.all import ParticipantTypes
from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents
from ...models.message_items.attachment import AttachmentMessageItem


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, message_group_uid: str, **kwargs):
        with db.get_session(project_id) as session:
            try:
                message_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                    ConversationMessageGroup.uuid == message_group_uid,
                ).first()
                if message_group is None:
                    return {"error": "Message group was not found"}, 400
                result = serialize(MessageGroupDetail.from_orm(message_group))
            except Exception as ex:
                log.debug(ex)
                return {
                    "error": f"Can not get details for {message_group_uid=}",
                }, 400
            return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, message_group_uid: str, **kwargs):
        with db.get_session(project_id) as session:
            message_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == message_group_uid,
            ).first()
            if message_group is None:
                return {"error": "Message group was not found"}, 400

            # message.is_streaming ? stop streaming task

            current_user = auth.current_user()
            # logging.info(f'{current_user=}')

            user_id = current_user.get('id')
            if user_id != message_group.conversation.author_id:
                author = message_group.author_participant
                if author.entity_name != ParticipantTypes.user or \
                        user_id != author.entity_meta.get('id'):
                    return {
                        "error": "Message can be deleted only by "
                                 "message or conversation author"
                    }, 400

            if 'delete_attachment' in request.args:
                for message_item in message_group.message_items:
                    if message_item.item_type == AttachmentMessageItem.__mapper_args__['polymorphic_identity']:
                        mc = MinioClient.from_project_id(project_id)
                        mc.remove_file(message_item.bucket, message_item.name)

            if message_group.meta and message_group.meta.get('context', {}).get('included') is False:
                return {
                    "error": "Summarized message can not be deleted"
                }, 400

            session.delete(message_group)
            session.commit()

            room = get_chat_room(message_group.conversation.uuid)

            self.module.context.sio.emit(
                event=SioEvents.chat_message_delete,
                data={
                    'message_group_id': message_group.id,
                    'message_group_uid': str(message_group.uuid),
                },
                room=room,
            )
            return None, 204


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
