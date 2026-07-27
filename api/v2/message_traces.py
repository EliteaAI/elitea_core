from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Message Traces",
        description="List trace steps (tool calls / thinking steps) for a conversation — light labels and ordering for the chat trace pins",
        parameters=[
            {"name": "message_group_id", "in": "query", "required": False, "schema": {"type": "integer"},
             "description": "Scope to a single message group."},
            {"name": "message_group_ids", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Comma-separated loaded message-group ids (maximum 200)."},
            {"name": "kind", "in": "query", "required": False, "schema": {"type": "string", "enum": ["tool_call", "thinking_step"]},
             "description": "Filter by step kind."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 2000}},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer", "default": 0}},
            {"name": "include_total", "in": "query", "required": False, "schema": {"type": "boolean", "default": False}},
        ],
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int, **kwargs):
        kind = request.args.get('kind')
        if kind and kind not in {'tool_call', 'thinking_step'}:
            return {'error': f'Unsupported trace-step kind: {kind}'}, 400
        raw_group_ids = request.args.get('message_group_ids', '')
        try:
            message_group_ids = [
                int(value)
                for value in raw_group_ids.split(',')
                if value.strip()
            ]
        except ValueError:
            return {'error': 'message_group_ids must contain integers'}, 400
        if len(message_group_ids) > 200 or any(value < 1 for value in message_group_ids):
            return {'error': 'message_group_ids must contain 1-200 positive integers'}, 400
        result = rpc_tools.RpcMixin().rpc.timeout(5).chat_list_trace_steps(
            project_id=project_id,
            conversation_id=conversation_id,
            message_group_id=request.args.get('message_group_id', type=int),
            message_group_ids=message_group_ids,
            kind=kind,
            limit=request.args.get('limit', 2000, type=int),
            offset=request.args.get('offset', 0, type=int),
            include_total=request.args.get('include_total', 'false').lower() == 'true',
        )
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
