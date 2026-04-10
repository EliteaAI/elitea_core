from flask import request
from pydantic import ValidationError

from ...models.pd.publish import UnpublishRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import get_public_project_id
from ...utils.publish_utils import admin_unpublish, user_unpublish

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    """Unpublish an agent version from Agent Studio.

    Admin unpublish: from within the public project.
    User unpublish: from the author's private project.
    """

    @register_openapi(
        name="Unpublish Agent Version",
        description="Unpublish an agent version from Agent Studio. Admin unpublish from public project; user unpublish from private project.",
        request_body=UnpublishRequest,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.unpublish.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, **kwargs):
        body = request.get_json(silent=True) or {}
        try:
            parsed = UnpublishRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400
        reason = parsed.reason
        user_id = auth.current_user().get("id")
        public_project_id = get_public_project_id()

        try:
            if project_id == public_project_id:
                return admin_unpublish(project_id, version_id, reason, user_id)
            else:
                return user_unpublish(
                    project_id, version_id, user_id, public_project_id, reason,
                )
        except Exception as e:
            log.exception("[UNPUBLISH] Unexpected error")
            return {"error": "internal_error", "msg": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
