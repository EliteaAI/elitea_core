from flask import request, url_for
from tools import config as c, api_tools, auth

from ...utils.constants import PROMPT_LIB_MODE


# routes/default_entity_icons
FLASK_ROUTE_URL: str = 'elitea_core.default_entity_icons'


class PromptLibAPI(api_tools.APIModeHandler):
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        return [
            {
                'name': i.name,
                'url': url_for(
                    f'{FLASK_ROUTE_URL}',
                    sub_path=f'{i.name}',
                    _external=True
                )
            } for i in self.module.default_entity_icons_path.iterdir()
        ], 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '',
        '<string:mode>/<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
