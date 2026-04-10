"""Context Analytics API - Get context status from conversation meta."""

from tools import api_tools, auth, config as c
from pylon.core.tools import log

from ...utils.context_analytics import get_context_data, build_context_response
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int, **kwargs):
        try:
            analytics, max_tokens, strategy_name = get_context_data(project_id, conversation_id)
            return build_context_response(analytics, max_tokens, strategy_name), 200
        except Exception as e:
            log.exception(f"Error getting context for conversation {conversation_id}")
            return {"error": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
