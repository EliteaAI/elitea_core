from flask import request
from pydantic import ValidationError

from ...models.all import ApplicationVersion
from ...models.pd.publish import PublishValidateRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.publish_utils import AIValidationError, validate_for_publish

from pylon.core.tools import log
from tools import api_tools, auth, config as c, db, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    """Pre-publish validation: deterministic + AI checks."""

    @register_openapi(
        name="Validate Agent for Publishing",
        description="Run deterministic + AI pre-publish validation checks on an agent version.",
        request_body=PublishValidateRequest,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.publish.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, **kwargs):
        body = request.get_json(silent=True) or {}
        try:
            parsed = PublishValidateRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        user_id = auth.current_user().get("id")

        # Verify version exists
        with db.get_session(project_id) as session:
            version = session.query(ApplicationVersion).get(version_id)
            if not version:
                return {"error": f"Version {version_id} not found"}, 404
            application_id = version.application_id

        try:
            result = validate_for_publish(
                project_id, version_id, application_id, parsed.version_name, user_id,
            )
        except AIValidationError as e:
            log.error(
                "[PUBLISH_VALIDATE] AI validation failed: %s", e,
            )
            return {
                "error": "ai_validation_failed",
                "msg": str(e),
            }, 400
        except Exception as e:
            log.exception("[PUBLISH_VALIDATE] Validation failed")
            return {"error": "validation_error", "msg": str(e)}, 500

        status_code = 200 if result.get('status') != 'FAIL' else 422
        return result, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
