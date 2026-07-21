from flask import request
from pydantic import ValidationError

from ...models.pd.skill_publish import SkillUnpublishRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.skill_publish_utils import admin_unpublish_skill, user_unpublish_skill
from ...utils.utils import get_public_project_id

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Unpublish a skill version from the public skill catalog",
        description="Removes a published skill version from the public project and "
                    "reverts the source version to draft. Must be called from the "
                    "project the skill was originally published from.",
        request_body=SkillUnpublishRequest,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.publish"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, skill_id: int, version_id: int, **kwargs):
        body = request.get_json(silent=True) or {}
        try:
            parsed = SkillUnpublishRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        user_id = auth.current_user().get("id")
        public_project_id = get_public_project_id()

        try:
            if project_id == public_project_id:
                return admin_unpublish_skill(
                    project_id, skill_id, version_id, user_id, parsed.reason,
                )
            return user_unpublish_skill(
                project_id, skill_id, version_id, user_id, public_project_id,
            )
        except Exception as e:
            log.exception("[SKILL_UNPUBLISH] Unexpected error during unpublish")
            return {"error": "internal_error", "msg": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
