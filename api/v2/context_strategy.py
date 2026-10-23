"""Context Strategy API - Update conversation context strategy."""

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, config as c, register_openapi
from pylon.core.tools import log

from ...models.pd.context import ContextStrategyUpdate
from ...utils.context_analytics import (
    build_context_response, get_context_data, get_conversation_meta, update_conversation_meta,
)
from ...utils.context_window import LEGACY_TRIGGER_KEY, MANUAL_TOKENS_KEYS, manual_change_denied, normalize_mode
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Update Context Strategy",
        description="Update context strategy settings for a conversation.",
        tags=["elitea_core/chat"],
        request_body=ContextStrategyUpdate,
        available_to_users=True,
    )
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
                return {"error": e.errors(include_context=False, include_url=False)}, 400

            meta = get_conversation_meta(project_id, conversation_id, missing=False)
            if meta is False:
                return {"error": "Conversation not found"}, 404
            current_strategy = meta.get('context_strategy') or {}

            updated_strategy = {
                **current_strategy,
                'enabled': True,
                'preserve_recent_messages': validated.preserve_recent_messages or current_strategy.get('preserve_recent_messages') or 5,
                'preserve_system_messages': validated.preserve_system_messages if validated.preserve_system_messages is not None else current_strategy.get('preserve_system_messages', True),
                'enable_summarization': validated.enable_summarization if validated.enable_summarization is not None else current_strategy.get('enable_summarization', True),
                'summary_llm_settings': validated.summary_llm_settings.model_dump() if validated.summary_llm_settings else current_strategy.get('summary_llm_settings') or {},
                'summary_instructions': validated.summary_instructions if validated.summary_instructions is not None else current_strategy.get('summary_instructions', ''),
                'name': data.get('name', current_strategy.get('name', 'default')),
            }
            if validated.enable_context_editing is not None:
                updated_strategy['enable_context_editing'] = validated.enable_context_editing
            if validated.context_window_mode is not None:
                updated_strategy['context_window_mode'] = validated.context_window_mode
                updated_strategy.pop('max_context_tokens', None)
                updated_strategy.pop(LEGACY_TRIGGER_KEY, None)
            elif validated.max_context_tokens is not None:
                log.warning('PUT context_strategy with max_context_tokens is deprecated, use context_window_mode')
                updated_strategy['max_context_tokens'] = validated.max_context_tokens
                updated_strategy[LEGACY_TRIGGER_KEY] = True
            updated_strategy['context_window_mode'] = normalize_mode(updated_strategy.get('context_window_mode'))
            user_id = auth.current_user().get('id')
            if manual_change_denied(
                    validated.model_fields_set, validated.manual_max_context_tokens, current_strategy, user_id):
                return {"error": "Manual Max Context Tokens is not available"}, 403
            if 'manual_max_context_tokens' in validated.model_fields_set \
                    and validated.manual_max_context_tokens != current_strategy.get('manual_max_context_tokens'):
                for key in MANUAL_TOKENS_KEYS:
                    updated_strategy.pop(key, None)
                if validated.manual_max_context_tokens is not None:
                    updated_strategy.update({
                        'manual_max_context_tokens': validated.manual_max_context_tokens,
                        'manual_set_by_admin': True,
                        'manual_set_by': user_id,
                    })

            update_conversation_meta(
                project_id=project_id,
                conversation_id=conversation_id,
                meta_updates={'context_strategy': updated_strategy}
            )

            response = build_context_response(*get_context_data(project_id, conversation_id, user_id))
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
