"""Shared handler for the five Elitea Builder MCP tools (#7383); one API module per tool (api/v2/builder_*.py)."""
from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ..constants import PROMPT_LIB_MODE

BUILDER_TAG = "elitea_core/builder"
_ROLES = {
    c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
    c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
}


def _run(project_id, action, model):
    from .runtime import handle  # pylint: disable=C0415
    try:
        body = model.model_validate(request.json or {}).model_dump()
    except ValidationError as e:
        return {"status": "invalid", "message": "Invalid arguments.",
                "errors": [{"field": ".".join(str(p) for p in err["loc"]), "message": err["msg"]}
                           for err in e.errors()]}, 400
    try:
        return handle(project_id, auth.current_user()["id"], action, body), 200
    except Exception as exc:  # pylint: disable=W0703
        log.exception("[builder] %s failed in project %s", action, project_id)
        return {"status": "error", "message": f"Builder {action} failed: {type(exc).__name__}"}, 500


def make_api(action: str, model, description: str):
    class Handler(api_tools.APIModeHandler):
        @register_openapi(
            name=f"builder_{action}", description=description, mcp_description=description,
            tags=[BUILDER_TAG], request_body=model, mcp_tool=True, available_to_users=False,
        )
        @auth.decorators.check_api({
            "permissions": ["models.applications.applications.details"], "recommended_roles": _ROLES,
        })
        @api_tools.endpoint_metrics
        def post(self, project_id: int, **kwargs):
            return _run(project_id, action, model)

    class API(api_tools.APIBase):
        url_params = api_tools.with_modes(["<int:project_id>"])
        mode_handlers = {PROMPT_LIB_MODE: Handler}

    return API
