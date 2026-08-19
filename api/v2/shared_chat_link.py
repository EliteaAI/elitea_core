"""Authenticated API for revoking a single share token.

  DELETE /elitea_core/shared_chat_link/prompt_lib/<project_id>/<token>  — revoke
"""
from tools import api_tools, auth, config as c, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Revoke Conversation Share Link",
        description="Revoke (invalidate) an existing share token so the link stops working",
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
    def delete(self, project_id: int, token: str, **kwargs):
        user_id = auth.current_user().get("id")
        rpc = rpc_tools.RpcMixin().rpc
        ok = rpc.timeout(5).chat_revoke_share_token(
            project_id=project_id,
            token=token,
            user_id=user_id,
        )
        if not ok:
            return {"error": "Token not found or access denied"}, 404
        return {"message": "Link revoked"}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:token>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
