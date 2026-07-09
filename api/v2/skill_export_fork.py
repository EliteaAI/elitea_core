from traceback import format_exc

from tools import api_tools, config as c, auth, register_openapi
from pylon.core.tools import log

from ...utils.skill_export_import import build_skill_fork_payload
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Build a skill fork payload",
        description=(
            "Returns the JSON fork payload for a skill (a copy of a single "
            "version, with icon and tags), suitable for the fork endpoint. When "
            "a version_id path segment is supplied that version is copied (404 if "
            "it does not exist); otherwise the skill's default/base version is "
            "used. The copied version is always emitted as the target's single "
            "'base' version."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "required": False, "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.fork.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        try:
            payload = build_skill_fork_payload(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
        except Exception as e:
            log.error(f"Skill fork payload build failed: {e}\n{format_exc()}")
            return {"error": "Internal server error"}, 500

        if payload is None:
            return {"error": f"Skill {skill_id} not found"}, 404

        return {"skills": [payload]}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
