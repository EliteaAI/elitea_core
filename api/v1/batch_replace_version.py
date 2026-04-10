from flask import request

from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, old_version_id: int, new_version_id: int, **kwargs):
        """
        Batch replace all references to old_version_id with new_version_id,
        then optionally delete the old version.
        """
        delete_old_version = request.args.get('delete_old', 'true').lower() == 'true'

        result = self.module.batch_replace_version_references(
            project_id=project_id,
            old_version_id=old_version_id,
            new_version_id=new_version_id,
            delete_old_version=delete_old_version
        )

        if 'error' in result:
            return {"ok": False, "error": result['error']}, 400

        return result, 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:old_version_id>/<int:new_version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
