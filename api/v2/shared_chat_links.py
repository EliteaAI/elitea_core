"""Authenticated API for managing shareable conversation links.

Endpoints (all require ELITEA login):
  POST   /elitea_core/shared_chat_links/prompt_lib/<project_id>/<conversation_id>  — create token
  GET    /elitea_core/shared_chat_links/prompt_lib/<project_id>/<conversation_id>  — list tokens
  DELETE /elitea_core/shared_link/prompt_lib/<project_id>/<token>             — revoke token
"""
from pydantic import ValidationError
from flask import request

from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create Conversation Share Link",
        description="Generate a shareable external link for a conversation that can be accessed without login",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.share"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int, **kwargs):
        user_id = auth.current_user().get("id")
        try:
            from ...models.pd.shared_chat_link import SharedLinkCreate
            payload = SharedLinkCreate.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(include_url=False), 400

        rpc = rpc_tools.RpcMixin().rpc
        result = rpc.timeout(10).chat_create_share_token(
            project_id=project_id,
            conversation_id=conversation_id,
            expiry=payload.expiry,
            password=payload.password,
            scope=payload.scope,
            message_group_ids=payload.message_group_ids,
            created_by=user_id,
        )
        if result is None:
            return {"error": "Conversation not found or access denied"}, 404
        return result, 201

    @register_openapi(
        name="List Conversation Share Links",
        description="List all active share tokens for a conversation",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.share"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc
        result = rpc.timeout(5).chat_list_share_tokens(
            project_id=project_id,
            conversation_id=conversation_id,
        )
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
