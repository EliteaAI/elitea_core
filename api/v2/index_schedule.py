from tools import api_tools, auth, config as c

from ...utils.application_tools import INDEX_SCHEDULE_MANAGED_MESSAGE
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.folder_access import require_folder_access, TOOLKIT_ENTITY_TYPES

_ADMIN_ROLES = {
    c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
}


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.edit"],
        "recommended_roles": _ADMIN_ROLES,
    })
    @api_tools.endpoint_metrics
    @require_folder_access(TOOLKIT_ENTITY_TYPES, 'toolkit_id', write=True)
    def delete(self, project_id: int, toolkit_id: int, index_name: str):
        # Index schedules are rows of the Schedules module; writing the old settings key would start a second,
        # unseen schedule for the same index
        return {"ok": False, "error": INDEX_SCHEDULE_MANAGED_MESSAGE}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
