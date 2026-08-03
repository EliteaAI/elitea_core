from flask import request
from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize
from pylon.core.tools import log

from ...models.conversation import Conversation
from ...models.message_group import ConversationMessageGroup
from ...models.message_items.text import TextMessageItem
from ...models.pd.message import MessageGroupDetail
from ...models.pd.participant import ParticipantEntityUser
from ...models.enums.all import ParticipantTypes
from ...utils.participant_utils import get_or_create_one
from ...utils.sio_utils import get_chat_room, SioEvents
from ...utils.constants import PROMPT_LIB_MODE

import uuid as _uuid


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Inject Mid-Turn Message",
        description="Inject a user message into a conversation while an agent turn is still running (Phase 0 POC).",
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
        raw = dict(request.json or {})
        user_input = (raw.get("user_input") or "").strip()
        if not user_input:
            return {"error": "user_input is required"}, 400

        injection_id = raw.get("injection_id") or str(_uuid.uuid4())
        current_user = auth.current_user()

        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.uuid == conversation_uuid
            ).first()
            if conversation is None:
                return {"error": f"No conversation found with uuid {conversation_uuid}"}, 404

            # Effective thread_id must match what the running turn registered
            # (ensure_thread_id falls back to the conversation uuid when unset).
            thread_id = raw.get("thread_id") or str(conversation.uuid)

            author_participant, _ = get_or_create_one(
                session=session,
                entity_name=ParticipantTypes.user,
                entity_meta=ParticipantEntityUser(id=current_user['id']),
            )

            msg_group = ConversationMessageGroup(
                uuid=str(_uuid.uuid4()),
                conversation=conversation,
                author_participant=author_participant,
                meta={'injection_id': injection_id},
            )
            msg = TextMessageItem(
                message_group=msg_group,
                item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
                content=user_input,
                order_index=0,
            )
            session.add(msg_group)
            session.add(msg)
            session.commit()
            session.refresh(msg_group)

            group_payload = serialize(MessageGroupDetail.model_validate(msg_group))
            group_uuid = str(msg_group.uuid)

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

        # Reflect the injected user message to all connected clients.
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
            'message_group': group_payload,
        }, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:conversation_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
