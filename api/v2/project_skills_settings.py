from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.project_skills import read_platform_project_skills_settings


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        return read_platform_project_skills_settings(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
