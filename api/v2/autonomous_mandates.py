"""Autonomous mandates of a project: the operator list and create (#7131, C-1009).

Platform administration. Create runs the same code as the ``manage_autonomous_mandate`` admin task
(``utils/mandate_admin.manage_mandate``), with the body turned into the task's parameter after strict validation.

Owner mode (``prompt_lib``, #7337 item 8): the caller's own mandates in the project (read only), so the schedule card
shows its owner the consent state and the consent actions (``autonomous_mandate_action`` owner mode).

Project mode (``default``, #7269): the administration list and create for the project's own admins, with the mandate
permissions (``governance.mandates.*``) checked in the project, while governance is enforced; each listed row says
whether it is escalated. ``escalated=true`` (both modes) lists the mandates waiting for an approval nobody in the project
can give.
"""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.mandate_api import CreateMandateRequest
from ...utils import binding_api as api
from ...utils.mandate_admin import manage_mandate
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.governance import is_governed
from ...utils.mandate_queries import DEFAULT_LIMIT, MAX_LIMIT, list_page
from ...utils.schedule_access import schedule_check_spec

PROJECT_ID_PARAM = {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}
STATUS = {"VERSION_NOT_FOUND": 404, "TOOLKIT_NOT_FOUND": 404, "TOOLKIT_CHANGED": 403, "MANDATE_NOT_FOUND": 404, "OWNER_NOT_FOUND": 404, "MANDATE_EXISTS": 409,
          "INVALID_STATE": 409, "MANDATE_EXPIRED": 409, "CONCURRENT_CHANGE": 409, "BINDING_DIGEST_MISMATCH": 409,
          "BINDING_NOT_ACTIVE": 409, "DENIED_BINDING": 403, "SELF_APPROVAL": 403, "OWNER_NOT_LINKED": 409,
          "OWNER_INACTIVE": 409, "IDENTITY_SOURCE_NOT_ALLOWED": 403, "DEPENDENCY_UNAVAILABLE": 503,
          "FAILED": 503}


def task_answer(result, ok):
    """The response for a ``manage_mandate`` result: ``ok(result)`` or ``{error, code}`` with the code's status."""
    if "error" not in result:
        return ok(result), 200
    code = result.get("code") or "INVALID_REQUEST"
    return {"error": result["error"], "code": code}, STATUS.get(code, 400)


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Autonomous Mandates (Admin)",
        description=("One page of the project's autonomous mandates: agent and version (or toolkit), owner (email), state, "
                     "operations, limits, approval, and the identity the runs act with with whether its owner consented "
                     "(never the consent itself). Returns {total, rows, identity_sources}."),
        tags=[api.OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM,
                    {"name": "state", "in": "query", "required": False, "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}}],
        responses={"200": {"description": "{total, rows, identity_sources}"}, "403": {"description": "Not allowed."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.VIEW))
    def get(self, project_id: int, **kwargs):
        return list_mandates(self.module, project_id)

    @register_openapi(
        name="Create Autonomous Mandate (Admin)",
        description=("Create a draft mandate for one agent version with an active identity binding (version_id; "
                     "operations run:schedule, run:webhook), or for the owner's index schedules of one toolkit "
                     "(toolkit_id; operation run:index_schedule): owner, max runtime, validity, and optionally the "
                     "identity its runs act with (owner_delegated, application) when the platform allows it. Exactly "
                     "one of version_id and toolkit_id. Returns {ok, id, state}."),
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM], request_body=CreateMandateRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed."},
                   "409": {"description": "A live mandate exists, or a state conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.MANAGE))
    def post(self, project_id: int, **kwargs):
        return create_mandate(self.module, project_id)


def list_mandates(module, project_id, project_mode=False):
    """The list both operator modes answer; a project-mode page says of each row whether it is escalated."""
    try:
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return api.refusal("INVALID_REQUEST")
    if not 1 <= limit <= MAX_LIMIT or offset < 0:
        return api.refusal("INVALID_REQUEST")
    admins, only, error = api.inbox_args(project_id, request.args, always=project_mode)
    if error:
        return error
    inbox = {} if admins is None else {"admins": admins, "escalated_only": only}
    try:
        return list_page(module, project_id, state=request.args.get("state"), limit=limit, offset=offset, **inbox), 200
    except Exception as exc:  # pylint: disable=W0703
        log.error("autonomous mandates: list failed (%s)", type(exc).__name__)
        return api.refusal("DEPENDENCY_UNAVAILABLE")


def create_mandate(module, project_id):
    """A draft mandate, as the admin task makes it."""
    actor = api.current_actor()
    if actor is None:
        return api.refusal("FORBIDDEN")
    body, error = api.parse_body(request, CreateMandateRequest)
    if error:
        return error
    if body.valid_until.tzinfo is None:
        return api.refusal("INVALID_REQUEST", "valid_until needs a time zone offset")
    covers = f"toolkit_id={body.toolkit_id}" if body.toolkit_id is not None else f"version_id={body.version_id}"
    param = (f"project_id={project_id};action=create;{covers};"
             f"owner_user_id={body.owner_user_id};operations={','.join(body.operations)};"
             f"max_runtime_s={body.max_runtime_s};valid_until={body.valid_until.isoformat()}")
    if body.identity is not None:
        param += f";identity={body.identity}"
    return task_answer(manage_mandate(module, param, actor_user_id=actor),
                       lambda result: {"ok": True, "id": result["mandate_ref"], "state": result["state"]})


class ProjectAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Autonomous Mandates (Project)",
        description=("The project's mandates for its own admins, as the administration list, each row with escalated "
                     "(waiting for an approval nobody in the project can give). Only while governance is enforced."),
        tags=[api.OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM,
                    {"name": "state", "in": "query", "required": False, "schema": {"type": "string"}},
                    {"name": "escalated", "in": "query", "required": False, "schema": {"type": "boolean"}},
                    {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}}],
        responses={"200": {"description": "{total, rows, identity_sources}"}, "403": {"description": "Not allowed."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.project_check_spec(api.MANDATE_VIEW))
    def get(self, project_id: int, **kwargs):
        return api.project_gate(self.module) or list_mandates(self.module, project_id, project_mode=True)

    @register_openapi(
        name="Create Autonomous Mandate (Project)",
        description="Create a draft mandate in the project, as the administration route does (governance.mandates.manage "
                    "in the project). Only while governance is enforced. Returns {ok, id, state}.",
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM], request_body=CreateMandateRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed."},
                   "409": {"description": "A live mandate exists, a state conflict, or governance is not enforced."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.project_check_spec(api.MANAGE_MANDATES))
    def post(self, project_id: int, **kwargs):
        return api.project_gate(self.module) or create_mandate(self.module, project_id)


class OwnerAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List My Autonomous Mandates",
        description=("The caller's own mandates in the project (as the owner), with the identity their runs act with and "
                     "whether the caller consented (never the consent itself). Returns {total, rows, identity_sources}."),
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM],
        responses={"200": {"description": "{total, rows, identity_sources}"}, "403": {"description": "Not allowed."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(schedule_check_spec("details"))
    def get(self, project_id: int, **kwargs):
        if api.current_actor() is None:
            return api.refusal("FORBIDDEN")
        actor = api.session_actor()   # the owner's own session: never an API token (C9-SEC-82)
        if actor is None:
            return api.refusal("DENIED_DELEGATION_UNSUPPORTED_CALLER")
        if not is_governed(self.module):
            return api.refusal("GOVERNANCE_NOT_ENFORCED")
        try:
            return list_page(self.module, project_id, limit=MAX_LIMIT, owner_user_id=actor), 200
        except Exception as exc:  # pylint: disable=W0703
            log.error("autonomous mandates: the owner's list failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI, PROMPT_LIB_MODE: OwnerAPI, c.DEFAULT_MODE: ProjectAPI}
