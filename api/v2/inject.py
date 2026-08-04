from flask import request
from sqlalchemy import desc
from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize
from pylon.core.tools import log

from ...models.conversation import Conversation
from ...models.message_group import ConversationMessageGroup
from ...models.message_items.base import MessageItem
from ...models.message_items.text import TextMessageItem
from ...models.pd.message import MessageGroupDetail
from ...models.enums.all import ParticipantTypes
from ...utils.midturn_injection_utils import is_midturn_injection_blocked_for_project
from ...utils.sio_utils import get_chat_room, SioEvents
from ...utils.constants import PROMPT_LIB_MODE

import uuid as _uuid

# Injected text shares the turn's context window with tool results, and
# summarization cannot relieve pressure from inside the tool loop.
MAX_INJECTION_CHARS = 4000


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Inject Mid-Turn Message",
        description="Inject a user message into a conversation while an agent turn is still running.",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.messages.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_uuid: str):
        if is_midturn_injection_blocked_for_project(project_id):
            return {"error": "Mid-turn input is not enabled for this project"}, 403

        raw = dict(request.json or {})
        user_input = (raw.get("user_input") or "").strip()
        if not user_input:
            return {"error": "user_input is required"}, 400
        if len(user_input) > MAX_INJECTION_CHARS:
            return {
                "error": f"Injected message too long ({len(user_input)} chars, "
                         f"max {MAX_INJECTION_CHARS}). Send it as a new message instead."
            }, 400

        injection_id = raw.get("injection_id") or str(_uuid.uuid4())

        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.uuid == conversation_uuid
            ).first()
            if conversation is None:
                return {"error": f"No conversation found with uuid {conversation_uuid}"}, 404

            # A turn is in flight iff its assistant group is still streaming. That
            # group's reply_to_id points at the user request group we append to.
            streaming_group = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == conversation.id,
                ConversationMessageGroup.is_streaming.is_(True),
                ConversationMessageGroup.reply_to_id.isnot(None),
            ).order_by(desc(ConversationMessageGroup.created_at)).first()
            if streaming_group is None:
                return {"error": "No agent turn is currently running in this conversation"}, 409

            request_group = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id == streaming_group.reply_to_id
            ).first()
            if request_group is None:
                return {"error": "Running turn has no user request group to append to"}, 409
            if request_group.author_participant.entity_name != ParticipantTypes.user:
                return {"error": "Running turn was not initiated by a user message"}, 409

            thread_id = raw.get("thread_id") or str(conversation.uuid)

            # Append to the user's own request group: role is resolved per group,
            # so this replays as a multi-part user turn with no author ambiguity.
            max_index = session.query(MessageItem.order_index).filter(
                MessageItem.message_group_id == request_group.id
            ).order_by(desc(MessageItem.order_index)).limit(1).scalar()
            next_index = (max_index or 0) + 1

            msg = TextMessageItem(
                message_group=request_group,
                item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
                content=user_input,
                order_index=next_index,
                meta={'injection_id': injection_id},
            )
            session.add(msg)
            session.commit()
            session.refresh(request_group)

            group_payload = serialize(MessageGroupDetail.model_validate(request_group))
            item_uuid = str(msg.uuid)
            group_uuid = str(request_group.uuid)

        # Notify the running agent turn (indexer) to fold this input in.
        try:
            self.module.event_node.emit('predict_events', {
                'type': 'inject',
                'thread_id': thread_id,
                'injection_id': injection_id,
                'text': user_input,
            })
        except Exception as e:
            log.error(f"Failed to emit inject event for thread {thread_id}: {e}")

        # Reflect the injected chunk to all connected clients.
        try:
            self.module.context.sio.emit(
                event=SioEvents.chat_predict.value,
                data={'type': 'chat_user_message', **group_payload},
                room=get_chat_room(conversation_uuid),
            )
        except Exception as e:
            log.warning(f"Failed to sio-emit injected message: {e}")

        return {
            'injection_id': injection_id,
            'thread_id': thread_id,
            'message_group_uuid': group_uuid,
            'message_item_uuid': item_uuid,
            'order_index': next_index,
            'message_group': group_payload,
        }, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:conversation_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
