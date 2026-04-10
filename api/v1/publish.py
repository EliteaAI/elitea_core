from ...utils.constants import PROMPT_LIB_MODE

from tools import api_tools, auth, config as c


class PromptLibAPI(api_tools.APIModeHandler):
    """Deprecated v1 publish endpoint. Use v2."""

    @auth.decorators.check_api({
        "permissions": ["models.applications.publish.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, **kwargs):
        return {"error": "This endpoint is deprecated. Use v2."}, 410


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
