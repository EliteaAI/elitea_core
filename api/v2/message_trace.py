from flask import request
from sqlalchemy.orm import selectinload
from tools import api_tools, auth, db, config as c, rpc_tools, register_openapi

from ...models.conversation import Conversation
from ...models.message_group import ConversationMessageGroup
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_access import check_read_access


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Message Trace",
        description="Retrieve a single trace step (tool call or thinking step) with its full inputs/outputs",
        mcp_description="""
        USE to read what an earlier tool call was given (tool_inputs) and what it returned (tool_output), for
        example to check or reuse figures an earlier answer was built on.

        DO NOT USE to find the calls → use the List Message Traces tool first; it gives each step's id and message_group_id.

        tool_output is the raw result as the tool returned it and can be large.

        Examples:
        1. Step 901 of message 315: GET .../message_trace/prompt_lib/1/901?message_group_id=315
        """,
        parameters=[
            {"name": "message_group_id", "in": "query", "required": True, "schema": {"type": "integer"},
             "description": "Owning message group from the trace-list row."},
        ],
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, step_id: int, **kwargs):
        message_group_id = request.args.get('message_group_id', type=int)
        if not message_group_id or message_group_id < 1:
            return {'error': 'message_group_id is required'}, 400
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).options(
                selectinload(Conversation.participants)
            ).join(
                ConversationMessageGroup, ConversationMessageGroup.conversation_id == Conversation.id
            ).filter(ConversationMessageGroup.id == message_group_id).first()
            denied = check_read_access(project_id, conversation, auth.current_user()['id'])
        if denied:
            return denied
        result = rpc_tools.RpcMixin().rpc.timeout(5).chat_get_trace_step(
            project_id=project_id,
            step_id=step_id,
            message_group_id=message_group_id,
        )
        if result is None:
            return {"error": f"No such trace step with id {step_id}"}, 404
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:step_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
