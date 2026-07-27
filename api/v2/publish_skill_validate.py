from flask import request
from pydantic import ValidationError

from ...models.pd.skill_publish import (
    PublishSkillValidateRequest,
    SkillValidationResult,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.publish_utils import AIValidationError
from ...utils.skill_publish_utils import (
    is_skill_publish_blocked_for_project,
    validate_skill_for_publish,
)
from ...utils.skill_utils import get_skill_version_by_id
from ...utils.utils import get_public_project_id

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Validate a skill version before publishing",
        description="Runs deterministic + AI pre-publish checks on a skill version. "
                    "Returns PASS/WARN + validation_token (200), FAIL (422), or "
                    "ai_validation_failed (400). Skills only.",
        request_body=PublishSkillValidateRequest,
        response_model=SkillValidationResult,
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
            parsed = PublishSkillValidateRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        public_project_id = get_public_project_id()
        if project_id != public_project_id and is_skill_publish_blocked_for_project(project_id):
            return {"error": "publishing_blocked",
                    "msg": "Skill publishing is blocked for this project by platform policy."}, 403

        if not get_skill_version_by_id(project_id, skill_id, version_id):
            return {"error": f"Skill version {version_id} not found"}, 404

        try:
            result = validate_skill_for_publish(
                project_id, skill_id, version_id, parsed.version_name,
                category=parsed.category,
            )
        except AIValidationError as e:
            log.error("[SKILL_PUBLISH_VALIDATE] AI validation failed: %s", e)
            return {"error": "ai_validation_failed", "msg": str(e)}, 400
        except Exception as e:
            log.exception("[SKILL_PUBLISH_VALIDATE] Validation failed")
            return {"error": "validation_error", "msg": str(e)}, 500

        status_code = 200 if result.get('status') != 'FAIL' else 422
        return result, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
