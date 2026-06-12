from tools import api_tools, config as c, auth, register_openapi

from ...utils.skill_utils import (
    get_available_skills_for_agent,
    MAX_SKILLS_PER_AGENT,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List skills attached to an application version",
        description="Returns the skills attached to the given application version. Used by the chat UI to populate the `~` skill autocomplete and by the agent configuration page. Attaching/detaching skills is done via PATCH on the skill endpoint (Link or unlink a skill to an agent version).",
        tags=["elitea_core/skills"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, app_version_id: int, **kwargs):
        skills = get_available_skills_for_agent(
            project_id=project_id,
            entity_version_id=app_version_id,
        )
        return {
            'skills': skills,
            'max_skills': MAX_SKILLS_PER_AGENT,
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:app_version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
