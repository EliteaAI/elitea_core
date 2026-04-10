from flask import request

from tools import api_tools, auth, db, config as c, MinioClient, serialize
from pylon.core.tools import log

from ...models.enums.all import ParticipantTypes
from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...utils.context_analytics import update_context_analytics_after_message_delete
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

            # Check if this is the last message in the conversation
            last_message = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == message_group.conversation_id
            ).order_by(ConversationMessageGroup.created_at.desc()).first()

            if last_message and last_message.id != message_group.id:
                return {
                    "error": "Only the last message in the conversation can be deleted"
                }, 400

            conversation_id = message_group.conversation_id
            conversation_uuid = message_group.conversation.uuid
            response_id = message_group.id
            response_uid = str(message_group.uuid)

            user_input = (
                session.query(ConversationMessageGroup)
                .filter(ConversationMessageGroup.id == message_group.reply_to_id)
                .first()
            ) if message_group.reply_to_id else None

            if user_input is not None and user_input.meta and \
                    user_input.meta.get('context', {}).get('included') is False:
                return {
                    "error": "Summarized message can not be deleted"
                }, 400

            user_input_id = user_input.id if user_input is not None else None
            user_input_uid = str(user_input.uuid) if user_input is not None else None

            session.delete(message_group)
            if user_input is not None:
                session.delete(user_input)
            session.commit()

            try:
                update_context_analytics_after_message_delete(project_id, conversation_id, session)
            except Exception as e:
                log.error(f"Failed to update context analytics after message deletion: {e}")

            room = get_chat_room(conversation_uuid)

            self.module.context.sio.emit(
                event=SioEvents.chat_message_delete,
                data={
                    'message_group_id': response_id,
                    'message_group_uid': response_uid,
                },
                room=room,
            )
            if user_input_uid is not None:
                self.module.context.sio.emit(
                    event=SioEvents.chat_message_delete,
                    data={
                        'message_group_id': user_input_id,
                        'message_group_uid': user_input_uid,
                    },
                    room=room,
                )
            return None, 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
