from flask import request
from tools import api_tools, auth, db, config as c, MinioClient, rpc_tools, register_openapi
from tools import serialize

from pydantic import ValidationError

from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.pd.conversation import ConversationCreate, ConversationDetails
from ...models.pd.participant import ParticipantCreate, ParticipantEntityUser
from ...utils.conversation_utils import get_conversation_details
from ...utils.participant_utils import add_participant_to_conversation
from ...utils.chat_feature_flags import get_context_manager_feature_flag
from ...utils.context_analytics import set_context_strategy
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Conversations",
        description="Get list of conversations with filtering, sorting, and pagination.",
        mcp_tool=True,
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": [
            "models.chat.conversations.list",
            "models.chat.conversations.list_custom",
        ],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        user_id = auth.current_user().get("id")
        rpc = rpc_tools.RpcMixin().rpc

        user_is_admin: bool = rpc.timeout(3).admin_check_user_is_admin(project_id, user_id)

        entity_meta_id = request.args.get('entity_meta_id', type=int) or request.args.get('participant_id', type=int)

        result = rpc.timeout(10).chat_list_conversations_rpc(
            project_id=project_id,
            user_id=user_id,
            source=request.args.get('source', default='elitea'),
            query=request.args.get('query'),
            limit=request.args.get('limit', default=10, type=int),
            offset=request.args.get('offset', default=0, type=int),
            sort_by=request.args.get('sort_by', default='created_at'),
            sort_order=request.args.get('sort_order', default='desc'),
            include_hidden=False,
            is_admin=user_is_admin,
            participant_id=entity_meta_id,
            entity_name=request.args.get('entity_name'),
        )

        return result, 200

    @register_openapi(
        name="Create Conversation",
        description="Create a new conversation for chat interactions.",
        mcp_tool=True,
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        raw = dict(request.json)
        user_id = auth.current_user().get("id")
        raw['author_id'] = user_id

        try:
            parsed = ConversationCreate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        from ...utils.utils import get_public_project_id  # pylint: disable=C0415
        public_project_id = get_public_project_id()
        if not parsed.is_private and public_project_id == project_id:
            return {"error": "Public conversation can not exist in public project"}, 400

        # Fetch user's personalization settings
        user_personalization = None
        user_context_defaults = None
        user_summarization_defaults = None
        try:
            social_user = rpc_tools.RpcMixin().rpc.timeout(2).social_get_user(user_id)
            if social_user:
                user_personalization = social_user.get('personalization')
                user_context_defaults = social_user.get('default_context_management')
                user_summarization_defaults = social_user.get('default_summarization')
        except Exception:
            pass  # Continue with defaults if fetching user settings fails

        # Apply user's personalization defaults to conversation meta
        if parsed.meta is None:
            parsed.meta = {}
        if user_personalization:
            # Set persona directly (not default_persona) - this is what the chat system expects
            if user_personalization.get('persona'):
                parsed.meta['persona'] = user_personalization['persona']
            if user_personalization.get('default_instructions'):
                parsed.meta['default_instructions'] = user_personalization['default_instructions']
                # Initialize instructions from default_instructions when not explicitly provided
                if not parsed.instructions:
                    parsed.instructions = user_personalization['default_instructions']

        user_participant_data = ParticipantCreate(
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=user_id)
        )
        dummy_participant_data = ParticipantCreate(
            entity_name=ParticipantTypes.dummy,
            entity_meta={}
        )
        parsed.participants.append(user_participant_data)
        parsed.participants.append(dummy_participant_data)

        with db.get_session(project_id) as session:
            conversation_dict = parsed.model_dump(exclude={'participants'})
            new_conversation = Conversation(**conversation_dict)
            session.add(new_conversation)
            session.flush()
            for p_data in parsed.participants:
                add_participant_to_conversation(
                    project_id=project_id,
                    session=session,
                    participant=p_data,
                    conversation=new_conversation,
                    initiator_id=user_id
                )
                session.flush()

            context_strategy = None
            if get_context_manager_feature_flag(
                project_id,
            ):
                context_strategy = set_context_strategy(
                    project_id=project_id,
                    conversation_id=new_conversation.id,
                    user_context_defaults=user_context_defaults,
                    user_summarization_defaults=user_summarization_defaults,
                )

            session.commit()
            # Expire all cached objects to ensure we get fresh data from DB
            session.expire_all()
            conversation: ConversationDetails = get_conversation_details(
                session, new_conversation.id, project_id, user_id
            )
            serialized = serialize(conversation)
            # Ensure context_strategy is included in response (may have been set by RPC after session cache)
            if context_strategy and 'meta' in serialized:
                serialized['meta']['context_strategy'] = context_strategy

            # room = get_chat_room(new_conversation.uuid)
            # self.module.context.sio.emit(
            #     event=SioEvents.chat_conversation_create,
            #     data=serialized,
            #     room=room,
            # )
            return serialized, 201

    @register_openapi(
        name="Delete Conversation",
        description="Delete a conversation by ID.",
        mcp_tool=True,
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int):
        rpc = rpc_tools.RpcMixin().rpc

        result = rpc.timeout(5).chat_delete_conversation_rpc(
            project_id=project_id,
            conversation_id=conversation_id,
        )

        if not result.get('success'):
            return {"error": result.get('error', 'Conversation not found')}, 404

        return {}, 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
