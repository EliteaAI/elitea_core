from ...utils.constants import PROMPT_LIB_MODE
from ...utils.collections import CollectionPublishing
from ...utils.utils import add_public_project_id

from pylon.core.tools import log
from tools import api_tools, auth, config as c


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.reject_collection.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, collection_id: int, **kwargs):
        project_id = kwargs.get('project_id')
        try:
            result = CollectionPublishing.reject(project_id, collection_id)
        except Exception as e:
            log.error(e)
            return {"ok": False, "error": str(e)}, 400

        if not result['ok']:
            code = result.pop('error_code', 400)
            return result, code
        #
        return result['result'], 200


class API(api_tools.APIBase):
    module_name_override = "promptlib_shared"

    url_params = api_tools.with_modes([
        '',
        '<int:collection_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
