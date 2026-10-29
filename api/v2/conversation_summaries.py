"""Conversation Summaries API - Generate and retrieve conversation summaries."""

from tools import api_tools, auth, config as c, register_openapi
from pylon.core.tools import log

from ...utils.summarization import (
    ConversationNotFoundError, SummaryGenerationError, SummaryRequestError,
    generate_manual_summary, get_conversation_summary_history,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Conversation Summary",
        description=(
            "Triggers a manual summarization of the conversation's message history. "
            "Summarizes the messages since the last summary, keeping the most recent ones, "
            "with the configured summary model or the project low-tier model."
        ),
        tags=["elitea_core/chat"],
        responses={
            "200": {
                "description": "Summary generated",
                "content": {
                    "application/json": {
                        "example": {
                            "success": True,
                            "summary_uuid": "550e8400-e29b-41d4-a716-446655440000",
                            "summary_content": "The conversation covered...",
                            "original_token_count": 3000,
                            "summary_token_count": 200,
                            "compression_ratio": 15.0,
                            "generation_time_ms": 2500,
                            "message": "Generated summary for 12 message groups",
                        }
                    }
                },
            },
            "400": {"description": "No summary model available or nothing to summarize"},
            "401": {"description": "Unauthorized"},
            "403": {"description": "Forbidden"},
            "404": {"description": "Conversation not found"},
            "500": {"description": "Internal server error"},
            "502": {"description": "Summary generation failed"},
        },
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int, **kwargs):
        try:
            user_id = auth.current_user().get('id')
            result = generate_manual_summary(
                rpc_manager=self.module.context.rpc_manager,
                project_id=project_id,
                conversation_id=conversation_id,
                user_id=user_id,
            )
            return result, 200
        except ConversationNotFoundError as e:
            return {"error": str(e)}, 404
        except SummaryRequestError as e:
            return {"error": str(e)}, 400
        except SummaryGenerationError as e:
            log.warning("Summary generation failed for conversation %d: %s", conversation_id, e)
            return {"error": str(e)}, 502
        except Exception:
            log.exception("Error generating summary for conversation %d", conversation_id)
            return {"error": "Failed to generate summary"}, 500

    @register_openapi(
        name="Get Conversation Summaries",
        description="Returns the summary history stored in the conversation's context analytics.",
        tags=["elitea_core/chat"],
        responses={
            "200": {
                "description": "Summary history",
                "content": {
                    "application/json": {
                        "example": {
                            "summaries": [],
                            "total_count": 0,
                            "summaries_generated": 0,
                        }
                    }
                },
            },
            "404": {"description": "Conversation not found"},
            "500": {"description": "Internal server error"},
        },
        available_to_users=True,
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
            result = get_conversation_summary_history(project_id, conversation_id)
            return result, 200
        except ConversationNotFoundError as e:
            return {"error": str(e)}, 404
        except Exception:
            log.exception("Error getting summaries for conversation %d", conversation_id)
            return {"error": "Failed to get summaries"}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
