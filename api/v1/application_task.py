from flask import request

from ...utils.constants import PROMPT_LIB_MODE
from tools import api_tools, auth, config as c, db


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.task.get"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    def get(self, project_id: int, task_id: str, **kwargs):
        try:
            current_status = self.module.task_node.get_task_status(task_id)
            #
            with_meta = request.args.get("meta", "no").lower().strip() in ["yes", "true"]
            with_result = request.args.get("result", "no").lower().strip() in ["yes", "true"]
            #
            result = {
                "status": current_status,
            }
            #
            if with_meta:
                result["meta"] = self.module.task_node.get_task_meta(task_id)
            #
            if with_result:
                result["result"] = self.module.task_node.get_task_result(task_id)
            #
            return result
        except Exception as e:
            return {
                "ok": False,
                "error": str(e)
            }, 400

    @auth.decorators.check_api({
        "permissions": ["models.applications.task.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, task_id: str, **kwargs):
        try:
            self.module.task_node.stop_task(task_id)
        except Exception as e:
            return {
                "ok": False,
                "error": str(e)
            }, 400
        return None, 204


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<string:task_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
