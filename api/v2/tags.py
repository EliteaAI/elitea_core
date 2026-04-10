from flask import request

from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.tags import list_tags

class PromptLibAPI(api_tools.APIModeHandler):

    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.tags.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id):
        return list_tags(project_id, request.args), 200


class API(api_tools.APIBase):
    url_params = [
        '<string:mode>/<int:project_id>',
        '<int:project_id>',
    ]

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
