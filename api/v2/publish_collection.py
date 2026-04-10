from ...utils.constants import PROMPT_LIB_MODE
from ...utils.collections import CollectionPublishing

from tools import api_tools, auth
from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @api_tools.endpoint_metrics
    def post(self, project_id: int, collection_id: int, **kwargs):
        if not auth.is_user_in_project(project_id):
            return {"ok": False, "error": "Access denied"}, 403
        #
        try:
            result = CollectionPublishing(project_id, collection_id).publish()
        except Exception as e:
            log.error(e)
            return {"ok": False, "error": str(e)}, 400

        if not result['ok']:
            code = result.pop('error_code', 400)
            return result, code
        #
        prompt_version = result['new_collection']
        return prompt_version, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:collection_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
