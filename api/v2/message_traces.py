from flask import request
from sqlalchemy.orm import selectinload
from tools import api_tools, auth, db, config as c, rpc_tools, register_openapi

from ...models.conversation import Conversation
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_access import check_read_access


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Message Traces",
        description="List trace steps (tool calls / thinking steps) for a conversation — light labels and ordering for the chat trace pins",
        mcp_description="""
        USE to find which tools were called in a conversation, and in which message, before reading what they returned.
        Each row has the tool name, the owning message_group_id, timings, is_error and the parent agent (empty for
        the agent's own calls, set for a sub-agent's). Rows carry no inputs or outputs.

        DO NOT USE to read a call's inputs or output → use the Get Message Trace tool with the row's id and message_group_id.
        DO NOT USE to read the message text → use the Get Messages or Get Conversation tool.

        Examples:
        1. All tool calls of conversation 42: GET .../message_traces/prompt_lib/1/42?kind=tool_call
        2. Tool calls of one message: ?kind=tool_call&message_group_id=315
        """,
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
        mcp_tool=True,
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
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).options(
                selectinload(Conversation.participants)
            ).filter(Conversation.id == conversation_id).first()
            denied = check_read_access(project_id, conversation, auth.current_user()['id'])
        if denied:
            return denied
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
