from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Message Trace",
        description="Retrieve a single trace step (tool call or thinking step) with its full inputs/outputs",
        parameters=[
            {"name": "message_group_id", "in": "query", "required": True, "schema": {"type": "integer"},
             "description": "Owning message group from the trace-list row."},
        ],
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
