from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, version_id: int, **kwargs):
        """
        Check if a version is in use by any parent agents/pipelines.
        Returns info about referencing parents and available replacement versions.
        """
        result = self.module.check_version_in_use(project_id, version_id)
        if 'error' in result:
            return {"ok": False, "error": result['error']}, 400
        return result, 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
