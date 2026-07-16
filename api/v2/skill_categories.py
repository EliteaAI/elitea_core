"""Skill categories endpoint exposing the active predefined category list."""

from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.skill_category_utils import get_skill_categories_detailed


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List skill categories",
        description=(
            "Returns the predefined skill categories available for publishing. "
            "Each entry includes an `is_default` flag. Use the `category` field "
            "when publishing a skill version to assign it to a discoverable "
            "category in the Skills Studio."
        ),
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.tags.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        return {"categories": get_skill_categories_detailed()}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
