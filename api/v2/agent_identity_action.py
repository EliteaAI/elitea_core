"""A lifecycle action on one agent identity binding (#7105, C-1009)."""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.binding_api import BindingActionRequest
from ...utils import binding_api as api
from ...utils.binding_admin import BindingActionError, run_binding_action
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
        actor = api.current_actor()
        if actor is None:
            return api.refusal("FORBIDDEN")
        body, error = api.parse_body(request, BindingActionRequest)
        if error:
            return error
        if not api.has_permission(api.APPROVE if body.action == "activate" else api.MANAGE, project_id):
            return api.refusal("FORBIDDEN")
        try:
            found = find_binding(project_id, binding_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("agent identity action: read failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")
        if found is None:
            return api.refusal("NOT_FOUND")
        values = {
            "project_id": project_id, "application_id": found["application_id"], "version_id": found["version_id"],
            "mode": found["identity_mode"], "binding_id": found["id"], "action": body.action,
            "expected_state": body.expected_state, "approval_ref": body.approval_ref,
            "policy_revision": body.policy_revision, "dry_run": False,
        }
        try:
            result = run_binding_action(self.module, values, actor)
        except BindingActionError as exc:
            return api.action_refusal(exc)
        return {"ok": True, "id": result["binding_ref"], "state": result["state"]}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<uuid:binding_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
