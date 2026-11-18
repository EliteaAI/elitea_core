"""A lifecycle action on one agent identity binding (#7105, C-1009); (#7269) also for the project's admins in project
mode while governance is enforced: the same lifecycle and four-eyes rule, the permission checked in the project."""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.binding_api import BindingActionRequest
from ...utils import binding_api as api
from ...utils.binding_admin import BindingActionError, parse_platform_operations, run_binding_action
from ...utils.binding_queries import find_binding


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Agent Identity Action (Admin)",
        description=(
            "submit, activate, suspend, resume, retire or rebind a binding. `expected_state` is the state the "
            "operator saw; a different current state is 409 CONCURRENT_CHANGE. activate needs `approval_ref` and "
            "`policy_revision`, the permission governance.bindings.approve, and an operator who neither created the "
            "binding nor last changed its state (403 SELF_APPROVAL). rebind moves an active or suspended binding "
            "back to pending_approval for the definition as it is now. Returns {ok, id, state}."
        ),
        tags=[api.OPENAPI_TAG],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "required": True, "schema": {"type": "string", "format": "uuid"}},
        ],
        request_body=BindingActionRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed, or self-approval."},
                   "404": {"description": "No such binding in this project."}, "409": {"description": "State conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    # any of the two gets in; the one the action needs is checked below (approve for activate, manage for the rest)
    @auth.decorators.check_api(api.check_spec(api.MANAGE, api.APPROVE))
    def post(self, project_id: int, binding_id, **kwargs):
        return binding_action(self.module, project_id, binding_id, c.ADMINISTRATION_MODE)


def binding_action(module, project_id, binding_id, mode):
    """One action, with the permission it needs asked in ``mode``."""
    actor = api.current_actor()
    if actor is None:
        return api.refusal("FORBIDDEN")
    body, error = api.parse_body(request, BindingActionRequest)
    if error:
        return error
    if body.action != "activate" and (body.approval_ref is not None or body.policy_revision is not None):
        return api.refusal("INVALID_REQUEST", "approval_ref and policy_revision belong to action activate only")
    if body.action != "activate" and body.platform_operations is not None:   # (#7228) a rebind proposes in the app
        return api.refusal("INVALID_REQUEST", "platform_operations belongs to action activate only")
    if (body.action == "activate") != (body.definition_digest is not None):   # (#7228, C9-SEC-98)
        return api.refusal("INVALID_REQUEST", "definition_digest (the definition the approver reviewed) is required "
                                              "for action activate and belongs to it only")
    if not api.has_permission(api.APPROVE if body.action == "activate" else api.MANAGE, project_id, mode):
        return api.refusal("FORBIDDEN")
    try:
        found = find_binding(project_id, binding_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("agent identity action: read failed (%s)", type(exc).__name__)
        return api.refusal("DEPENDENCY_UNAVAILABLE")
    if found is None:
        return api.refusal("NOT_FOUND")
    refused = _foreign_suspension(project_id, found, body.action, mode)
    if refused:
        return refused
    # (#7228, C9-SEC-100) a move into approval makes its mover the request's actor only when they may approve
    takes = body.action not in ("submit", "rebind") or api.has_permission(api.APPROVE, project_id, mode)
    values = {
        "takes_request": takes, "mover_kind": "deliberate" if takes else "not_approver",
        "project_id": project_id, "application_id": found["application_id"], "version_id": found["version_id"],
        "mode": found["identity_mode"], "binding_id": found["id"], "action": body.action,
        "expected_state": body.expected_state, "approval_ref": body.approval_ref,
        "policy_revision": body.policy_revision, "dry_run": False, "reviewed_digest": body.definition_digest,
    }
    if body.platform_operations is not None:   # (#7228) the same parser as create and the admin task
        operations, error = parse_platform_operations(body.platform_operations, body.action)
        if error:
            return api.refusal("INVALID_REQUEST", error)
        values["platform_operations"] = operations
    try:
        result = run_binding_action(module, values, actor)
    except BindingActionError as exc:
        return api.action_refusal(exc)
    answer = {"ok": True, "id": result["binding_ref"], "state": result["state"]}
    if result.get("consents_to_withdraw"):   # (#7228, D3-S4) retired; an owner's consent is still to be withdrawn
        answer["consents_to_withdraw"] = result["consents_to_withdraw"]
    return answer, 200


def _foreign_suspension(project_id, found, action, mode):
    """(#7269) In project mode a suspended binding leaves its suspension (resume, or a rebind back to pending approval)
    only when an admin of the project suspended it; what anyone else (a platform operator) suspended is lifted in
    administration mode only (``RESUME_NEEDS_OPERATOR``). None: go on."""
    if mode != c.DEFAULT_MODE or found["state"] != "suspended" or action not in ("resume", "rebind"):
        return None
    from ...utils.governance_inbox import AdminsUnavailable, project_admins  # pylint: disable=C0415
    try:
        admins = project_admins(project_id)
    except AdminsUnavailable:
        return api.refusal("DEPENDENCY_UNAVAILABLE")
    if found.get("state_changed_by") not in admins:
        return api.refusal("RESUME_NEEDS_OPERATOR")
    return None


class ProjectAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Agent Identity Action (Project)",
        description=(
            "The administration route's actions for the project's own admins: activate needs "
            "governance.bindings.approve in the project and an admin who neither created the binding nor last changed "
            "its state (403 SELF_APPROVAL), the rest governance.bindings.manage. Only while governance is enforced."
        ),
        tags=[api.OPENAPI_TAG],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "required": True, "schema": {"type": "string", "format": "uuid"}},
        ],
        request_body=BindingActionRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed, or self-approval."},
                   "404": {"description": "No such binding in this project."}, "409": {"description": "State conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.project_check_spec(api.MANAGE, api.APPROVE))
    def post(self, project_id: int, binding_id, **kwargs):
        return api.project_gate(self.module) or binding_action(self.module, project_id, binding_id, c.DEFAULT_MODE)


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<uuid:binding_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI, c.DEFAULT_MODE: ProjectAPI}
