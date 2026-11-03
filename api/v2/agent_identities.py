"""Agent identity bindings of a project: the operator list and create (#7105, C-1009).

Platform administration, and (#7269) the project's own admins in project mode while governance is enforced: the same
list and lifecycle (``utils/binding_admin.run_binding_action``, the code the ``manage_agent_identity_binding`` admin task
runs), with the project-mode permissions checked in the project. ``escalated=true`` lists the bindings waiting for an
approval nobody in the project can give (``utils/governance_inbox``).
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
    {"name": "escalated", "in": "query", "required": False, "schema": {"type": "boolean"},
     "description": "true: only the bindings waiting for an approval nobody in the project can give."},
]


def list_bindings(project_id, project_mode=False):
    """The list both modes answer; a project-mode page says of each row whether it is escalated."""
    params, error = api.page_args(request.args, SORTS, "updated_at")
    if error:
        return error
    filters, error = api.list_filters(request.args)
    if error:
        return error
    admins, only, error = api.inbox_args(project_id, request.args, always=project_mode)
    if error:
        return error
    inbox = {} if admins is None else {"admins": admins, "escalated_only": only}
    try:
        return list_page(project_id, **params, **filters, **inbox), 200
    except Exception as exc:  # pylint: disable=W0703
        log.error("agent identities: list failed (%s)", type(exc).__name__)
        return api.refusal("DEPENDENCY_UNAVAILABLE")


def create_binding(module, project_id):
    """A draft binding for one version, as the admin task makes it."""
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
        result = run_binding_action(module, values, actor)
    except BindingActionError as exc:
        return api.action_refusal(exc)
    return {"ok": True, "id": result["binding_ref"], "state": result["state"],
            "digest": result["definition_digest"]}, 200


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Agent Identities (Admin)",
        description=(
            "One page of the project's agent identity bindings: agent and version names, state, the stored digest "
            "(first 12 hash characters) and whether the agent's definition still matches it, policy revision, "
            "revocation epoch, who created the binding and who last changed its state. Returns {total, rows}. "
            "escalated=true: only the bindings waiting for an approval nobody in the project can give, each with "
            "escalated."
        ),
        tags=[api.OPENAPI_TAG], parameters=LIST_PARAMETERS,
        responses={"200": {"description": "{total, rows}"}, "403": {"description": "Missing permission."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.VIEW))
    def get(self, project_id: int, **kwargs):
        return list_bindings(project_id)

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
        return create_binding(self.module, project_id)


class ProjectAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Agent Identities (Project)",
        description=(
            "The project's agent identity bindings for its own admins, as the administration list, each row with "
            "escalated (waiting for an approval nobody in the project can give). Only while governance is enforced. "
            "Returns {total, rows}."
        ),
        tags=[api.OPENAPI_TAG], parameters=LIST_PARAMETERS,
        responses={"200": {"description": "{total, rows}"}, "403": {"description": "Missing permission."},
                   "409": {"description": "Governance is not enforced."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.project_check_spec(api.VIEW))
    def get(self, project_id: int, **kwargs):
        return api.project_gate(self.module) or list_bindings(project_id, project_mode=True)

    @register_openapi(
        name="Create Agent Identity (Project)",
        description="Create a draft binding for one agent version of the project, as the administration route does.",
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM], request_body=CreateBindingRequest,
        responses={"200": {"description": "{ok, id, state, digest}"}, "404": {"description": "No such version."},
                   "409": {"description": "The binding exists, or governance is not enforced."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.project_check_spec(api.MANAGE))
    def post(self, project_id: int, **kwargs):
        return api.project_gate(self.module) or create_binding(self.module, project_id)


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI, c.DEFAULT_MODE: ProjectAPI}
