"""Report the current user's PAT state for an internal Elitea MCP toolkit (VALID/EXPIRED/MISSING),
so the toolkit UI can gate actions before the toolkit silently fails to connect. Scoped to the
caller's own tokens; the token value is never returned."""

from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.internal_tools import is_internal_mcp_toolkit, resolve_user_token_state


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, toolkit_type: str, **kwargs):
        _ = kwargs, project_id

        # Empty settings: a prebuilt internal toolkit resolves its URL from the type.
        if not is_internal_mcp_toolkit({'type': toolkit_type, 'settings': {}}):
            return {"internal": False, "state": "VALID"}, 200

        current_user = auth.current_user()
        user_id = current_user.get('id') if current_user else None
        state, _token = resolve_user_token_state(user_id)
        return {"internal": True, "state": state}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:toolkit_type>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
