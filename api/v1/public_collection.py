from tools import api_tools, auth, config as c

from ...utils.collections import get_collection
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import add_public_project_id


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.public_collection.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def get(self, collection_id: int, *, project_id, **kwargs):
        result = get_collection(project_id, collection_id, only_public=True)
        if not result:
            return {"error": f"No collection found with id '{collection_id}'"}, 400
        return result, 200


class API(api_tools.APIBase):
    module_name_override = "promptlib_shared"

    url_params = api_tools.with_modes(
        [
            "<int:collection_id>",
        ]
    )

    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}