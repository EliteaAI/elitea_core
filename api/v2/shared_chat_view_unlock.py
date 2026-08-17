"""Public (unauthenticated) API for unlocking password-protected shared conversations.

  POST /elitea_core/shared_chat_view_unlock/prompt_lib/<token>/unlock  — verify password, set session cookie
"""
from flask import current_app, make_response, request
from itsdangerous import TimestampSigner

from tools import api_tools, rpc_tools, register_openapi

from ...utils.constants import PROMPT_LIB_MODE

_UNLOCK_COOKIE_PREFIX = 'share_unlocked_'
_COOKIE_MAX_AGE = 3600


def _get_signer() -> TimestampSigner:
    return TimestampSigner(current_app.config['SECRET_KEY'], salt='share_unlock')


class SharedConversationUnlockAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Unlock Password-Protected Shared Conversation",
        description="Verify the password for a password-protected shared conversation link. Sets a session cookie on success.",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @api_tools.endpoint_metrics
    def post(self, token: str, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc
        password = (request.json or {}).get("password", "")
        ok = rpc.timeout(5).chat_verify_share_token_password(
            token=token,
            password=password,
        )
        if ok is None:
            return {"error": "This link is no longer available."}, 404
        if ok == 'locked':
            return {"error": "Too many failed attempts. Try again in 15 minutes."}, 429
        if not ok:
            return {"error": "Incorrect password."}, 401

        signed_value = _get_signer().sign(token.encode()).decode()
        response = make_response({"ok": True})
        response.set_cookie(
            _UNLOCK_COOKIE_PREFIX + token,
            signed_value,
            max_age=_COOKIE_MAX_AGE,
            httponly=True,
            samesite="Lax",
        )
        return response


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<string:token>/unlock',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: SharedConversationUnlockAPI,
    }
