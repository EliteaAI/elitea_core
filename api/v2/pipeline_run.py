from flask import request

from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import PredictPayloadError


class PromptLibAPI(api_tools.APIModeHandler):

    @register_openapi(
        name="Run Pipeline",
        description="Execute pipeline with optional async mode and callback URL.",
        tags=["elitea_core/applications"],
        parameters=[
            {"name": "async", "in": "query", "required": False, "schema": {"type": "boolean"}, "description": "Run asynchronously."},
        ],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.predict.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        request_json = dict(request.json)

        callback_url = request_json.pop("callback_url", None)
        callback_headers = request_json.pop("callback_headers", None)
        async_mode = request_json.pop("async_mode", False)

        is_async = callback_url is not None or async_mode or \
            request.args.get("async", "no").lower().strip() in ["yes", "true"]

        try:
            result: dict = self.module.do_pipeline_run(
                project_id=project_id,
                user_id=auth.current_user()["id"],
                payload_in=request_json,
                predict_wait=not is_async,
            )

            if callback_url is not None and "task_id" in result and result["task_id"]:
                if hasattr(self.module, "callback_tasks"):
                    self.module.callback_tasks[result["task_id"]] = {
                        "callback_url": callback_url,
                        "callback_headers": callback_headers,
                    }
        except ValidationError as e:
            return e.errors(), 400
        except PredictPayloadError as e:
            return {"error": str(e)}, 400
        except BaseException as exc:
            log.exception("Pipeline run error: %s", exc)
            return {"error": "Cannot execute pipeline"}, 500

        response_code = 200
        if "error" in result and result["error"] is not None:
            response_code = 400

        return result, response_code


class API(api_tools.APIBase):

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
