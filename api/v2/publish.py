from flask import request
from pydantic import ValidationError

from ...models.all import ApplicationVersion
from ...models.enums.all import PublishStatus
from ...models.pd.publish import PublishRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import get_public_project_id
from ...utils.publish_utils import (
    AIValidationError,
    admin_publish,
    user_publish,
    validate_for_publish,
    verify_token_for_publish,
)

from pylon.core.tools import log
from tools import api_tools, auth, config as c, db, this, register_openapi


class PromptLibAPI(api_tools.APIModeHandler):
    """Publish an agent version to Agent Studio (public project).

    User cross-project publish: snapshot -> copy to public project.
    Admin in-place publish: toggle status inside public project.
    """

    @register_openapi(
        name="Publish Agent Version",
        description="Publish an agent version to Agent Studio. User publish copies to public project; admin publish toggles status in-place.",
        request_body=PublishRequest,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.publish.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, **kwargs):
        # --- Parse & validate request body ---
        body = request.get_json(silent=True) or {}
        try:
            parsed = PublishRequest.model_validate(body)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        version_name = parsed.version_name
        validation_token = parsed.validation_token

        user_id = auth.current_user().get("id")
        public_project_id = get_public_project_id()
        max_versions = int(this.descriptor.config.get("max_published_versions_per_agent", 3))

        try:
            with db.get_session(project_id) as session:
                version = session.query(ApplicationVersion).get(version_id)
                if not version:
                    return {"error": f"Version {version_id} not found"}, 404
                if version.status == PublishStatus.published:
                    return {"error": "already_published", "msg": "This version is already published"}, 409
                source_app_id = version.application_id

            # --- Validation gate (skip for admin in-place publish) ---
            if project_id != public_project_id:
                if validation_token:
                    is_valid, err_msg = verify_token_for_publish(
                        project_id, version_id, user_id, validation_token,
                    )
                    if not is_valid:
                        return {"error": "validation_failed", "msg": err_msg}, 400
                else:
                    # Inline validation fallback
                    result = validate_for_publish(
                        project_id, version_id, source_app_id, version_name, user_id,
                    )
                    if result.get('status') == 'FAIL':
                        return {
                            "error": "validation_failed",
                            "msg": "Agent failed pre-publish validation. Use /publish_validate first.",
                            "validation_result": result,
                        }, 422

            # --- Branch: admin in-place vs user cross-project ---
            if project_id == public_project_id:
                return admin_publish(
                    project_id, version_id, source_app_id,
                    version_name, user_id, max_versions,
                )
            else:
                return user_publish(
                    project_id, version_id, source_app_id,
                    version_name, user_id, public_project_id, max_versions,
                )

        except AIValidationError as e:
            return {
                "error": "ai_validation_failed",
                "msg": str(e),
            }, 400
        except Exception as e:
            log.exception("[PUBLISH] Unexpected error during publish")
            return {"error": "internal_error", "msg": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
