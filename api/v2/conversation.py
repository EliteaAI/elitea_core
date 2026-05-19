from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi
from pylon.core.tools import log

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Conversation",
        description="Get detailed information about a specific conversation.",
        mcp_tool=True
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

        result = rpc.timeout(5).chat_update_conversation_rpc(
            project_id=project_id,
            conversation_id=conversation_id,
            name=data.get('name'),
            instructions=data.get('instructions'),
            is_private=data.get('is_private'),
            is_hidden=data.get('is_hidden'),
            meta=data.get('meta'),
            attachment_participant_id=data.get('attachment_participant_id'),
        )

        if not result.get('success'):
            error = result.get('error', 'Failed to update conversation')
            return {"error": error}, 400

        return result.get('conversation'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
