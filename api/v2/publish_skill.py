from flask import request
from pydantic import ValidationError

from ...models.enums.all import PublishStatus
from ...models.pd.skill_publish import SkillPublishRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.publish_utils import AIValidationError
from ...utils.skill_category_utils import validate_skill_category
from ...utils.skill_publish_utils import (
    admin_publish_skill,
    is_skill_publish_blocked_for_project,
    user_publish_skill,
    validate_skill_for_publish,
    verify_skill_token_for_publish,
)
from ...utils.skill_utils import get_skill_version_by_id
from ...utils.utils import get_public_project_id

from pylon.core.tools import log
from tools import api_tools, auth, config as c, this, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Publish a skill version to the public skill catalog",
        description="Publishes a skill version to the public project, making it "
                    "available to the broader community. Requires the version to "
                    "pass pre-publish validation; run /publish_skill_validate first "
                    "to receive a validation_token that skips re-validation.",
        request_body=SkillPublishRequest,
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
            parsed = SkillPublishRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        user_id = auth.current_user().get("id")
        public_project_id = get_public_project_id()
        max_versions = int(this.descriptor.config.get("max_published_versions_per_skill", 3))

        if project_id != public_project_id and is_skill_publish_blocked_for_project(project_id):
            return {"error": "publishing_blocked",
                    "msg": "Skill publishing is blocked for this project by platform policy."}, 403

        source_version = get_skill_version_by_id(project_id, skill_id, version_id)
        if source_version is None:
            return {"error": f"Skill version {version_id} not found"}, 404

        if project_id != public_project_id and source_version.status == PublishStatus.published:
            return {"error": "already_published",
                    "msg": "This skill version is already published."}, 409

        if parsed.category and not validate_skill_category(parsed.category):
            return {"error": "invalid_category",
                    "msg": f"Category '{parsed.category}' is not a valid skill category."}, 400

        try:
            if project_id != public_project_id:
                if parsed.validation_token:
                    is_valid, err_msg = verify_skill_token_for_publish(
                        project_id, version_id, user_id, parsed.validation_token,
                    )
                    if not is_valid:
                        return {"error": "validation_token_invalid", "msg": err_msg}, 400
                else:
                    result = validate_skill_for_publish(
                        project_id, skill_id, version_id, parsed.version_name,
                        category=parsed.category,
                    )
                    if result.get("status") == "FAIL":
                        return {
                            "error": "validation_token_invalid",
                            "msg": "Skill failed pre-publish validation. Use /publish_skill_validate first.",
                            "validation_result": result,
                        }, 400

            if project_id == public_project_id:
                return admin_publish_skill(
                    project_id, skill_id, version_id,
                    parsed.version_name, user_id, max_versions,
                    category=parsed.category,
                )
            return user_publish_skill(
                project_id, skill_id, version_id,
                parsed.version_name, user_id, public_project_id, max_versions,
                category=parsed.category,
            )

        except AIValidationError as e:
            return {"error": "ai_validation_failed", "msg": str(e)}, 400
        except Exception as e:
            log.exception("[SKILL_PUBLISH] Unexpected error during publish")
            return {"error": "internal_error", "msg": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
