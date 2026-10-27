"""Agent identity bindings of a project: the operator list and create (#7105, C-1009).

Platform administration only. The lifecycle is ``utils/binding_admin.run_binding_action``, the same code the
``manage_agent_identity_binding`` admin task runs.
"""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.binding_api import CreateBindingRequest
from ...utils import binding_api as api
from ...utils.binding_admin import PLATFORM_OPERATIONS, BindingActionError, run_binding_action
from ...utils.binding_queries import SORTS, list_page

PROJECT_ID_PARAM = {
    "name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"},
    "description": "Project whose agent identities to read or change.",
}
LIST_PARAMETERS = [
    PROJECT_ID_PARAM,
    {"name": "state", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Binding state."},
    {"name": "identity_mode", "in": "query", "required": False, "schema": {"type": "string"}},
    {"name": "application_id", "in": "query", "required": False, "schema": {"type": "integer"}},
    {"name": "search", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Agent name contains."},
    {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Default 20, at most 50."},
    {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}},
    {"name": "sort_by", "in": "query", "required": False, "schema": {"type": "string", "enum": list(SORTS)}},
    {"name": "sort_order", "in": "query", "required": False, "schema": {"type": "string", "enum": ["asc", "desc"]}},
]


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Agent Identities (Admin)",
        description=(
            "One page of the project's agent identity bindings: agent and version names, state, the stored digest "
            "(first 12 hash characters) and whether the agent's definition still matches it, policy revision, "
            "revocation epoch, who created the binding and who last changed its state. Returns {total, rows}."
        ),
        tags=[api.OPENAPI_TAG], parameters=LIST_PARAMETERS,
        responses={"200": {"description": "{total, rows}"}, "403": {"description": "Missing permission."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.VIEW))
    def get(self, project_id: int, **kwargs):
        params, error = api.page_args(request.args, SORTS, "updated_at")
        if error:
            return error
        filters, error = api.list_filters(request.args)
        if error:
            return error
        try:
            return list_page(project_id, **params, **filters), 200
        except Exception as exc:  # pylint: disable=W0703
            log.error("agent identities: list failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")

    @register_openapi(
        name="Create Agent Identity (Admin)",
        description=(
            "Create a draft binding for one agent version, with the digest of its definition as it is now. "
            "Returns {ok, id, state, digest}."
        ),
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM], request_body=CreateBindingRequest,
        responses={"200": {"description": "{ok, id, state, digest}"}, "404": {"description": "No such version."},
                   "409": {"description": "The binding exists."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.MANAGE))
    def post(self, project_id: int, **kwargs):
        actor = api.current_actor()
        if actor is None:
            return api.refusal("FORBIDDEN")
        body, error = api.parse_body(request, CreateBindingRequest)
        if error:
            return error
        values = {
            "project_id": project_id, "application_id": body.application_id, "version_id": body.version_id,
            "mode": body.identity_mode, "action": "create", "dry_run": False,
        }
        if body.platform_operations is not None:   # the same parser as the admin task (#7214)
            values["platform_operations"] = sorted(set(body.platform_operations), key=PLATFORM_OPERATIONS.index)
        try:
            result = run_binding_action(self.module, values, actor)
        except BindingActionError as exc:
            return api.action_refusal(exc)
        return {"ok": True, "id": result["binding_ref"], "state": result["state"],
                "digest": result["definition_digest"]}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
