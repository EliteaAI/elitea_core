"""Public (unauthenticated) API for viewing shared conversations.

  GET  /elitea_core/shared_chat_view/prompt_lib/<token>  — fetch conversation (password check via cookie)
"""
from datetime import datetime

from flask import current_app, request
from itsdangerous import BadSignature, TimestampSigner
from pylon.core.tools import log

from tools import api_tools, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE

_UNLOCK_COOKIE_PREFIX = 'share_unlocked_'
# Cookies are valid for 1 hour after being set by the unlock endpoint.
_COOKIE_MAX_AGE = 3600


def _get_signer() -> TimestampSigner:
    return TimestampSigner(current_app.config['SECRET_KEY'], salt='share_unlock')


def _is_session_unlocked(token: str) -> bool:
    cookie_name = _UNLOCK_COOKIE_PREFIX + token
    value = request.cookies.get(cookie_name)
    if not value:
        return False
    try:
        payload = _get_signer().unsign(value, max_age=_COOKIE_MAX_AGE).decode()
        return payload == token
    except BadSignature:
        return False


class SharedConversationViewAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="View Shared Conversation",
        description="Retrieve a shared conversation by its public token. No authentication required. Returns 401 if password-protected and not yet unlocked.",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @api_tools.endpoint_metrics
    def get(self, token: str, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc
        result = rpc.timeout(10).chat_get_shared_conversation(
            token=token,
            unlocked=_is_session_unlocked(token),
        )
        if result is None:
            return {"error": "This link is no longer available."}, 404
        status = result.get("status")
        if status == "expired":
            return {"error": "This link has expired."}, 410
        if status == "revoked":
            return {"error": "This link is no longer available."}, 404
        if status == "password_required":
            return {"password_required": True}, 401
        return result.get("data"), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<string:token>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: SharedConversationViewAPI,
    }
