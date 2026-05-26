"""Context Analytics API - Get context status from conversation meta."""

from tools import api_tools, auth, config as c, register_openapi
from pylon.core.tools import log

from ...utils.context_analytics import get_context_data, build_context_response
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Conversation Context Analytics",
        description=(
            "Returns context utilisation analytics for a conversation: token counts, "
            "context window usage percentage, active context strategy, and message breakdown."
        ),
        tags=["Analytics"],
        responses={
            "200": {
                "description": "Conversation context analytics",
                "content": {
                    "application/json": {
                        "example": {
                            "used_tokens": 3200,
                            "max_tokens": 8192,
                            "usage_pct": 39.1,
                            "strategy": "summarize",
                            "messages": {
                                "total": 24,
                                "human": 12,
                                "ai": 12,
                            },
                        }
                    }
                },
            },
            "401": {"description": "Unauthorized"},
            "500": {"description": "Internal server error"},
        },
    )
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
