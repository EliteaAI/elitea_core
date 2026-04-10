from flask import request
from tools import api_tools, auth, config as c
from pylon.core.tools import log

from ...utils.toolkits_utils import get_toolkit_schemas, get_mcp_schemas
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.toolkits.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):

        user_id = auth.current_user()["id"]
        filter_mcp = request.args.get('mcp', 'false').lower() == 'true'

        try:
            if filter_mcp:
                result = get_mcp_schemas(project_id, user_id)
            else:
                result = get_toolkit_schemas(project_id, user_id)
            #
        except Exception as e:
            log.error(f"Error occurred while fetching toolkits: {e}")
            return {"ok": False, "error": "Error getting toolkits schemas"}, 400
        #
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
