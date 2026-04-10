import json

from flask import request
from pylon.core.tools import log
from tools import api_tools, config as c, auth, context

from ...models.mcp import McpToolCallPostBody
from ...utils.sio_utils import SioEvents


class PromptLibAPI(api_tools.APIModeHandler):
    # @auth.decorators.check_api(
    #     {
    #         "permissions": ["models.applications.mcp.run"],
    #         "recommended_roles": {
    #             c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    #             c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
    #         },
    #     }
    # )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        current_user = auth.current_user()
        if not current_user:
            return {"error": "Unauthorized"}, 401
        real_user_id = current_user["id"]
        if not auth.is_user_in_project(project_id) or \
                not auth.check_user_in_project(project_id, real_user_id):
            return {"error": "Access denied"}, 403
        #
        body = McpToolCallPostBody.model_validate(request.json)
        log.debug(f"[MCP_CLIENT] Call Tool : {json.dumps(body.model_dump(), indent=2)}")
        #
        mcp_servers = self.module.get_registered_servers_private_and_current(project_id, real_user_id)
        server = next((serv for serv in mcp_servers if serv.name == body.server), None)
        #
        if server:
            log.debug(f"[MCP_CLIENT] Calling MCP tools on server {body.server} for project {project_id}.")
            tools_call_result = context.sio.call(SioEvents.mcp_tools_call, body.model_dump(), to=server.sio_sid, timeout=server.timeout_tools_call)
            return tools_call_result, 200
        #
        return {"error": f"Mcp Server '{body.server}' is currently not connected to either private or current project."}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])

    mode_handlers = {
        c.DEFAULT_MODE: PromptLibAPI,
    }
