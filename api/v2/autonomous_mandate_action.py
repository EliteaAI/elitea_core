"""A lifecycle action on one autonomous mandate, and its owner's consent (#7131, C-1009).

``submit``, ``reject``, ``suspend``, ``revoke`` need governance.bindings.manage; ``approve`` and ``resume`` need
governance.bindings.approve and someone who is neither the creator nor the owner (the admin task's rules, the same
code). ``consent``: the mandate's owner, signed in with their own session, lets its runs act with their Microsoft
sign-in for the credentials the version uses now (``consent_preview`` names them first); ``withdraw_consent``: the
owner, or an operator with manage, withdraws it.
"""
import uuid

from flask import request
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.mandate_api import MandateActionRequest
from ...utils import binding_api as api
from ...utils import mandate_identity
from ...utils.mandate_admin import manage_mandate
from .autonomous_mandates import task_answer

APPROVE_ACTIONS = ("approve", "resume")
IDENTITY_STATUS = {"IDENTITY_SOURCE_NOT_ALLOWED": 403, "IDENTITY_NOT_DELEGATED": 409, "CONSENT_OWNER_ONLY": 403,
                   "WITHDRAW_NOT_ALLOWED": 403, "NOTHING_TO_CONSENT": 409,
                   "MANDATE_NOT_FOUND": 404, "INVALID_STATE": 409, "CONSENT_NOT_GIVEN": 409,
                   "DENIED_SUBJECT_MISMATCH": 403, "DENIED_ASSERTION_UNAVAILABLE": 409,
                   "DENIED_DELEGATION_UNSUPPORTED_CALLER": 403, "DEPENDENCY_UNAVAILABLE": 503,
                   "AUDIT_UNAVAILABLE": 503, "CONCURRENT_CHANGE": 409}


def _identity(action, module, project_id, mandate_id, actor):
    try:
        if action == "consent":
            result = mandate_identity.consent(module, project_id, mandate_id, actor)
        elif action == "consent_preview":
            result = mandate_identity.consent_preview(module, project_id, mandate_id, actor)
        else:
            result = mandate_identity.withdraw(module, project_id, mandate_id, actor,
                                               operator=api.has_permission(api.MANAGE, project_id))
    except mandate_identity.IdentityRefused as refused:
        return {"error": mandate_identity.MESSAGES[refused.code], "code": refused.code}, \
            IDENTITY_STATUS.get(refused.code, 400)
    extra = {key: result[key] for key in ("consented_at", "covered") if key in result}
    return {"ok": True, "id": result["mandate_ref"], **extra}, 200


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Autonomous Mandate Action (Admin)",
        description=("submit, reject, approve, suspend, resume or revoke a mandate (approve needs approval_ref and "
                     "policy_revision), or consent / withdraw_consent to its runs acting with the owner's Microsoft "
                     "sign-in (consent: the owner only, in their own signed-in session). Returns {ok, id, state}."),
        tags=[api.OPENAPI_TAG],
        parameters=[{"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    {"name": "mandate_id", "in": "path", "required": True, "schema": {"type": "string", "format": "uuid"}}],
        request_body=MandateActionRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed."},
                   "404": {"description": "No such mandate in this project."}, "409": {"description": "State conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    # any of the three gets in; the one the action needs is checked below (consent and the owner's withdrawal: by the
    # action, against the mandate's owner)
    @auth.decorators.check_api(api.check_spec(api.VIEW, api.MANAGE, api.APPROVE))
    def post(self, project_id: int, mandate_id, **kwargs):
        actor = api.current_actor()
        if actor is None:
            return api.refusal("FORBIDDEN")
        body, error = api.parse_body(request, MandateActionRequest)
        if error:
            return error
        if body.action != "approve" and (body.approval_ref is not None or body.policy_revision is not None):
            return api.refusal("INVALID_REQUEST", "approval_ref and policy_revision belong to action approve only")
        mandate_id = uuid.UUID(str(mandate_id))
        if body.action in ("consent", "consent_preview", "withdraw_consent"):   # the owner's own (withdraw: or manage)
            return _identity(body.action, self.module, project_id, mandate_id, actor)
        needed = api.APPROVE if body.action in APPROVE_ACTIONS else api.MANAGE
        if not api.has_permission(needed, project_id):
            return api.refusal("FORBIDDEN")
        param = f"project_id={project_id};action={body.action};mandate={mandate_id}"
        if body.action == "approve":
            if not body.approval_ref or not body.policy_revision:
                return api.refusal("APPROVAL_REQUIRED", "approve needs approval_ref and policy_revision")
            param += f";approval_ref={body.approval_ref};policy_revision={body.policy_revision}"
        return task_answer(manage_mandate(self.module, param, actor_user_id=actor),
                           lambda result: {"ok": True, "id": result["mandate_ref"], "state": result["state"]})


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<uuid:mandate_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
