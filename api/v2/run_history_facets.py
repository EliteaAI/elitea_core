"""Run History filter options: the values that have runs for one entity (EliteaAI/elitea_issues#7682, AC-9).

GET /elitea_core/run_history_facets/prompt_lib/{project_id}?participant_id=<entity id>
Read once when a filter dropdown first opens. Answers the versions, schedules and tool names with runs for the
entity, so a filter lists every value that exists, not only the values on loaded rows.
"""
from flask import request
from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Run History Filter Options",
        description="The versions, schedules and tool names that have runs for one agent or pipeline",
        mcp_description="""
        USE to list the values a Run History filter can offer for an entity: its versions, its schedules and webhooks,
        and the tools its runs called.
        """,
        tags=["elitea_core/chat"],
        parameters=[
            {"name": "participant_id", "in": "query", "required": True, "schema": {"type": "integer"},
             "description": "The agent or pipeline id whose runs are listed."},
        ],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        participant_id = request.args.get('participant_id', type=int)
        if not participant_id or participant_id < 1:
            return {'code': 'invalid_input', 'error': 'participant_id is required', 'field': 'participant_id'}, 400
        user_id = auth.current_user().get("id")
        rpc = rpc_tools.RpcMixin().rpc
        user_is_admin: bool = rpc.timeout(3).admin_check_user_is_admin(project_id, user_id)
        result = rpc.timeout(10).chat_run_history_facets_rpc(
            project_id=project_id,
            user_id=user_id,
            entity_id=participant_id,
            is_admin=user_is_admin,
        )
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
