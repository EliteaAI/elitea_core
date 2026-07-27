from flask import request
from tools import api_tools, auth, db, config as c, MinioClient, rpc_tools, register_openapi
from tools import serialize

from pydantic import ValidationError

from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.pd.conversation import ConversationCreate, ConversationDetails
from ...models.pd.participant import ParticipantCreate, ParticipantEntityUser
from ...utils.conversation_utils import get_conversation_details, resolve_persona_instructions
from ...utils.participant_utils import add_participant_to_conversation
from ...utils.chat_feature_flags import get_context_manager_feature_flag
from ...utils.context_analytics import set_context_strategy
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Conversations",
        description="List conversations in a project with filtering by source, participant, entity name, and free-text search — paginated",
        mcp_description="""
        USE to browse or search conversations in a project, and to find a conversation_id or conversation_uuid
        before calling other endpoints.

        DO NOT USE for folder-organized conversation browsing → use list_folders_and_conversations instead.
        DO NOT USE to get a single conversation with its messages → use get_conversation.

        Examples:
        1. List user's recent conversations: GET .../conversations/prompt_lib/42?limit=20
        2. Search by name: GET ...?query=sprint+review
        3. Filter by participant (agent): GET ...?participant_id=15
        4. Support conversations: GET ...?source=support
        """,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        parameters=[
            {"name": "source", "in": "query", "required": False, "schema": {"type": "string", "default": "elitea"},
             "description": "Filter by conversation source (e.g. 'elitea', 'support')."},
            {"name": "query", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Free-text search filter on conversation name."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 10},
             "description": "Maximum number of results to return."},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer", "default": 0},
             "description": "Pagination offset."},
            {"name": "sort_by", "in": "query", "required": False, "schema": {"type": "string", "default": "created_at"},
             "description": "Field to sort by."},
            {"name": "sort_order", "in": "query", "required": False, "schema": {"type": "string", "default": "desc"},
             "description": "Sort order (asc or desc)."},
            {"name": "participant_id", "in": "query", "required": False, "schema": {"type": "integer"},
             "description": "Filter by participant entity meta ID (agent/toolkit ID)."},
            {"name": "entity_name", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Filter by participant entity name (e.g. 'application', 'llm')."},
        ],
        available_to_users=True,
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
        description="Create a new chat conversation with optional initial participants, instructions, and metadata",
        mcp_description="""
        USE to start a new chat session — either a blank conversation or one pre-configured with specific agents,
        toolkits, or LLMs.

        DO NOT USE if a conversation already exists and you want to add a participant → use add_participants.
        DO NOT USE to send a message → use send_message after creating the conversation.

        Participant types you can add:
        - Agent/pipeline: { 'entity_name': 'application', 'entity_meta': { 'id': 7, 'project_id': 42 } }
        - LLM: { 'entity_name': 'llm', 'entity_meta': { 'model_name': 'gpt-4o' } }
        - Toolkit: { 'entity_name': 'toolkit', 'entity_meta': { 'id': 5, 'project_id': 42 } }

        Examples:
        1. Blank conversation: { 'name': 'My Chat', 'is_private': true }
        2. With agent pre-added: { 'name': 'Agent Chat', 'is_private': true, 'participants': [{ 'entity_name': 'application', 'entity_meta': { 'id': 7, 'project_id': 42 } }] }
        """,
        request_body=ConversationCreate,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
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
            persona = user_personalization.get('persona')
            if persona:
                parsed.meta['persona'] = persona
            # Resolve the instructions for the selected persona (#5392); '' means no override.
            selected_instructions = resolve_persona_instructions(user_personalization, persona)
            if selected_instructions:
                parsed.meta['default_instructions'] = selected_instructions
                # Initialize instructions from default_instructions when not explicitly provided
                if not parsed.instructions:
                    parsed.instructions = selected_instructions

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

class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }

