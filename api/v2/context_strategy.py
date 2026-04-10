"""Context Strategy API - Update conversation context strategy."""

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, config as c
from pylon.core.tools import log

from ...models.pd.context import ContextStrategyUpdate
from ...utils.context_analytics import get_context_data, build_context_response, update_conversation_meta
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, conversation_id: int, **kwargs):
        """Update context strategy for a conversation."""
        try:
            data = request.json or {}

            try:
                validated = ContextStrategyUpdate.model_validate(data)
            except ValidationError as e:
                return {"error": e.errors()}, 400

            analytics, current_max_tokens, current_strategy_name = get_context_data(project_id, conversation_id)

            updated_strategy = {
                'enabled': validated.enabled if validated.enabled is not None else True,
                'max_context_tokens': validated.max_context_tokens or current_max_tokens,
                'preserve_recent_messages': validated.preserve_recent_messages or 5,
                'preserve_system_messages': validated.preserve_system_messages if validated.preserve_system_messages is not None else True,
                'enable_summarization': validated.enable_summarization if validated.enable_summarization is not None else True,
                'summary_llm_settings': validated.summary_llm_settings.model_dump() if validated.summary_llm_settings else {},
                'summary_instructions': validated.summary_instructions or '',
                'name': data.get('name', current_strategy_name),
            }

            update_conversation_meta(
                project_id=project_id,
                conversation_id=conversation_id,
                meta_updates={'context_strategy': updated_strategy}
            )

            response = build_context_response(analytics, updated_strategy['max_context_tokens'])
            response['message'] = 'Strategy updated successfully'
            response['updated_strategy'] = updated_strategy
            return response, 200

        except Exception as e:
            log.exception(f"Error updating strategy for conversation {conversation_id}")
            return {"error": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
