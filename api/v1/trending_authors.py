import json
from typing import List

from tools import api_tools, auth, config as c

from ...utils.application_utils_general import get_trending_authors
from ...models.pd.authors import TrendingAuthorModel
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.trending_authors.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int):
        authors: List[TrendingAuthorModel] = get_trending_authors(project_id)
        return [author.model_dump(mode='json') for author in authors], 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
