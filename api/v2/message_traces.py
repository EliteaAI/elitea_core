from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Message Traces",
        description="List trace steps (tool calls / thinking steps) for a conversation — light labels and ordering for the chat trace pins",
        mcp_description="""
        USE to list the trace-step pins (tool calls and thinking steps) of a conversation for display or
        analysis — returns lightweight rows (labels, timing, ordering) without heavy inputs/outputs.

        DO NOT USE to read a step's full inputs/outputs → use get_message_trace with the step id.

        Rows are ordered by (started_at, id) and carry message_group_id so they can be grouped per message.

        Examples:
        1. All steps in a conversation: GET .../message_traces/prompt_lib/1/42
        2. Only tool calls: ?kind=tool_call
        3. Steps of one message group: ?message_group_id=1061
        4. Paginate: ?limit=100&offset=0
        """,
        parameters=[
            {"name": "message_group_id", "in": "query", "required": False, "schema": {"type": "integer"},
             "description": "Scope to a single message group."},
            {"name": "kind", "in": "query", "required": False, "schema": {"type": "string", "enum": ["tool_call", "thinking_step"]},
             "description": "Filter by step kind."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 1000}},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer", "default": 0}},
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
        result = rpc_tools.RpcMixin().rpc.timeout(5).chat_list_trace_steps(
            project_id=project_id,
            conversation_id=conversation_id,
            message_group_id=request.args.get('message_group_id', type=int),
            kind=request.args.get('kind'),
            limit=request.args.get('limit', 1000, type=int),
            offset=request.args.get('offset', 0, type=int),
        )
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
