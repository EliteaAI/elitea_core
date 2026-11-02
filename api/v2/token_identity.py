"""A scoped API token's owner consents to its runs acting with their Microsoft sign-in, or withdraws it (#7337, C-1009).

The owner's own act, in their own signed-in session (owner self-service: no platform or project role). ``GET``: what
the token list shows (whether the platform allows it, whether the account is linked, the consents of the caller's
tokens). ``POST {action: consent | withdraw, token_id}``. An API token never reaches this route under the governed
profile (``token_scope.TOKEN_ROUTES``), and the handler itself answers a signed-in session only.
"""
from typing import Literal

from flask import request
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from pylon.core.tools import log
from tools import api_tools, config as c, register_openapi

from ...utils import binding_api as api
from ...utils import token_identity

OPENAPI_TAG = "elitea_core/governance"
STATUS = {"IDENTITY_SOURCE_NOT_ALLOWED": 403, "TOKEN_NOT_FOUND": 404, "TOKEN_NOT_SCOPED": 409, "INVALID_STATE": 409,
          "CONSENT_NOT_GIVEN": 409, "OWNER_NOT_LINKED": 409, "DENIED_SUBJECT_MISMATCH": 403,
          "DENIED_ASSERTION_UNAVAILABLE": 409, "DENIED_DELEGATION_UNSUPPORTED_CALLER": 403,
          "DEPENDENCY_UNAVAILABLE": 503, "AUDIT_UNAVAILABLE": 503, "CONCURRENT_CHANGE": 409,
          "GOVERNANCE_NOT_ENFORCED": 409}


class TokenIdentityAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["consent", "withdraw"]
    token_id: StrictInt = Field(gt=0)


class DefaultAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="API Token Identity Consents",
        description=("Whether scoped API tokens may act with their owner's Microsoft sign-in on this platform, whether "
                     "the caller's account is linked to one, and the consents of the caller's tokens "
                     "{allowed, linked, mandates, consents: {token_id: consented_at}}. Signed-in sessions only."),
        tags=[OPENAPI_TAG], responses={"200": {"description": "{allowed, linked, mandates, consents}"},
                                       "403": {"description": "Not a signed-in session."}},
        mcp_tool=False, available_to_users=False,
    )
    def get(self, **kwargs):
        actor = api.session_actor()
        if actor is None:
            return api.refusal("FORBIDDEN")
        try:
            return token_identity.status(self.module, actor), 200
        except Exception as exc:  # pylint: disable=W0703
            log.error("token identity: the status could not be read (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")

    @register_openapi(
        name="API Token Identity Consent Action",
        description=("consent: the token's owner, signed in, lets the scoped token's runs act with their Microsoft "
                     "sign-in (signing out does not end it); withdraw: the owner withdraws it. Returns {ok, token_id}."),
        tags=[OPENAPI_TAG], request_body=TokenIdentityAction,
        responses={"200": {"description": "{ok, token_id}"}, "403": {"description": "Not allowed."},
                   "404": {"description": "No such token of the caller."}, "409": {"description": "State conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    def post(self, **kwargs):
        actor = api.session_actor()
        if actor is None:
            return {"error": token_identity.MESSAGES["DENIED_DELEGATION_UNSUPPORTED_CALLER"],
                    "code": "DENIED_DELEGATION_UNSUPPORTED_CALLER"}, 403
        body, error = api.parse_body(request, TokenIdentityAction)
        if error:
            return error
        try:
            if body.action == "consent":
                result = token_identity.consent(self.module, body.token_id, actor)
            else:
                result = token_identity.withdraw(self.module, body.token_id, actor)
        except token_identity.IdentityRefused as refused:
            message = token_identity.MESSAGES.get(refused.code, token_identity.MESSAGES["DEPENDENCY_UNAVAILABLE"])
            return {"error": message, "code": refused.code}, \
                STATUS.get(refused.code, 400)
        return {"ok": True, **result}, 200


class API(api_tools.APIBase):
    url_params = ["<string:mode>"]
    mode_handlers = {c.DEFAULT_MODE: DefaultAPI}
