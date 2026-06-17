"""Agent categories endpoint exposing the active predefined category list."""

from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.category_utils import get_active_categories_detailed


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List agent categories",
        description=(
            "Returns the active predefined agent categories available for publishing. "
            "The list combines the hardcoded defaults (e.g. Development, DevOps, Other) "
            "with any extra categories an admin has added via the guardrails configuration. "
            "Each entry includes an `is_default` flag. Use the `category` field when "
            "publishing an agent version to assign it to a discoverable category in "
            "Agent Studio."
        ),
        tags=["elitea_core/applications"],
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
        """Return the active agent categories (defaults + admin-added)."""
        return {"categories": get_active_categories_detailed()}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
