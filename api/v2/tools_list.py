from tools import api_tools, config as c, serialize, auth


class PromptLibAPI(api_tools.APIModeHandler):
    # @auth.decorators.check_api(
    #     {
    #         "permissions": ["models.applications.mcp.list"],
    #         "recommended_roles": {
    #             c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    #             c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
    #         },
    #     }
    # )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        current_user = auth.current_user()
        if not current_user:
            return {"error": "Unauthorized"}, 401
        user_id = current_user["id"]
        if not auth.is_user_in_project(project_id) or \
                not auth.check_user_in_project(project_id, user_id):
            return {"error": "Access denied"}, 403
        #
        mcp_servers = self.module.get_registered_servers_private_and_current(project_id, user_id)
        return serialize(mcp_servers), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])

    mode_handlers = {
        c.DEFAULT_MODE: PromptLibAPI,
    }
