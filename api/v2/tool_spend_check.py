"""Tool Spend check (#6980): called by the SDK's metered_call around every paid provider call."""

from flask import request
from tools import api_tools, auth

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.tool_spend import PylonDeps, ToolSpendError, ToolSpendService


class PromptLibAPI(api_tools.APIModeHandler):
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        current_user = auth.current_user()
        if not current_user:
            return {"error": "Unauthorized"}, 401
        user_id = current_user["id"]
        if not auth.is_user_in_project(project_id) or not auth.check_user_in_project(project_id, user_id):
            return {"error": "Access denied"}, 403
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return {"error": "JSON object body required"}, 400
        service = ToolSpendService(PylonDeps(self.module))
        try:
            return service.check(project_id, user_id, body, run_id=request.headers.get("X-Elitea-Run-Id")), 200
        except ToolSpendError as exc:
            return {"error": exc.message}, exc.status


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
