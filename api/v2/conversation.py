from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi
from pylon.core.tools import log


from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Conversation",
        description="Retrieve full details of a specific conversation including participants and paginated message history",
        mcp_description="""
        USE to retrieve the full conversation record including its participants and recent messages in a single call.

        DO NOT USE when you only need the message list without conversation metadata → use get_messages.
        DO NOT USE to list all conversations → use list_conversations.

        Key distinction: this returns both the conversation record AND messages. get_messages returns only the
        message list with richer filtering.

        Examples:
        1. Get conversation 42 with last 20 messages: GET .../conversation/prompt_lib/1/42?messages_limit=20&sort_order=desc
        2. Get conversation with messages in chronological order: ?sort_order=acs (default).
        """,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int = None, **kwargs):
        user_id = auth.current_user().get("id")
        rpc = rpc_tools.RpcMixin().rpc

        messages_limit = request.args.get('messages_limit', 100, type=int)
        messages_offset = request.args.get('messages_offset', 0, type=int)

        sort_order = request.args.get('sort_order', 'acs')

        support_config = rpc.timeout(3).support_assistant_get_config()
        is_support_project = support_config.get('project_id') == project_id

        result = rpc.timeout(5).chat_get_conversation_details(
            project_id=project_id,
            conversation_id=conversation_id,
            user_id=user_id,
            check_ownership=not is_support_project,
            messages_limit=messages_limit,
            messages_offset=messages_offset,
            sort_order=sort_order,
        )

        if not result:
            return {'error': f'No such conversation with id {conversation_id}'}, 400

        return result, 200

    @register_openapi(
        name="Update Conversation",
        description="Update a conversation's name, instructions, privacy, folder assignment, or metadata",
        mcp_description="""
        USE to rename a conversation, add/change its system instructions, move it into a folder, or change its
        privacy setting.

        DO NOT USE to send messages → use send_message.
        DO NOT USE to change participant-level LLM settings → use configure_participant.

        Folder movement: to move a conversation into folder 5: { 'folder_id': 5 }. To remove from folder:
        { 'folder_id': null }.

        Examples:
        1. Rename: { 'name': 'Sprint 12 Review' }
        2. Set system instructions: { 'instructions': 'Always respond in bullet points.' }
        3. Make public: { 'is_private': false } (not allowed in public project).
        4. Move to folder: { 'folder_id': 3 }
        """,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, conversation_id: int):
        data = dict(request.json) if request.json else {}
        log.debug(f"Update conversation {conversation_id} with data: {data}")
        rpc = rpc_tools.RpcMixin().rpc

        kwargs = {
            'project_id': project_id,
            'conversation_id': conversation_id,
            'name': data.get('name'),
            'instructions': data.get('instructions'),
            'is_private': data.get('is_private'),
            'is_hidden': data.get('is_hidden'),
            'meta': data.get('meta'),
            'attachment_participant_id': data.get('attachment_participant_id'),
        }

        if 'folder_id' in data:
            kwargs['folder_id'] = data['folder_id']
            kwargs['update_folder'] = True

        result = rpc.timeout(5).chat_update_conversation_rpc(**kwargs)

        if not result.get('success'):
            error = result.get('error', 'Failed to update conversation')
            return {"error": error}, 400

        return result.get('conversation'), 200

    @register_openapi(
        name="Delete Conversation",
        description="Delete a conversation by ID.",
        tags=["elitea_core/chat"],
        available_to_users=True,
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
