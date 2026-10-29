"""Autonomous mandates of a project: the operator list and create (#7131, C-1009).

Platform administration only. Create runs the same code as the ``manage_autonomous_mandate`` admin task
(``utils/mandate_admin.manage_mandate``), with the body turned into the task's parameter after strict validation.
"""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.mandate_api import CreateMandateRequest
from ...utils import binding_api as api
from ...utils.mandate_admin import manage_mandate
from ...utils.mandate_queries import DEFAULT_LIMIT, MAX_LIMIT, list_page

PROJECT_ID_PARAM = {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}
STATUS = {"VERSION_NOT_FOUND": 404, "MANDATE_NOT_FOUND": 404, "OWNER_NOT_FOUND": 404, "MANDATE_EXISTS": 409,
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
        description=("One page of the project's autonomous mandates: agent and version, owner (email), state, "
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
        try:
            limit = int(request.args.get("limit", DEFAULT_LIMIT))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return api.refusal("INVALID_REQUEST")
        if not 1 <= limit <= MAX_LIMIT or offset < 0:
            return api.refusal("INVALID_REQUEST")
        try:
            return list_page(self.module, project_id, state=request.args.get("state"), limit=limit, offset=offset), 200
        except Exception as exc:  # pylint: disable=W0703
            log.error("autonomous mandates: list failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")

    @register_openapi(
        name="Create Autonomous Mandate (Admin)",
        description=("Create a draft mandate for one agent version with an active identity binding: owner, operations "
                     "(run:schedule, run:webhook), max runtime, validity, and optionally the identity its runs act "
                     "with (owner_delegated, application) when the platform allows it. Returns {ok, id, state}."),
        tags=[api.OPENAPI_TAG], parameters=[PROJECT_ID_PARAM], request_body=CreateMandateRequest,
        responses={"200": {"description": "{ok, id, state}"}, "403": {"description": "Not allowed."},
                   "409": {"description": "A live mandate exists, or a state conflict."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.MANAGE))
    def post(self, project_id: int, **kwargs):
        actor = api.current_actor()
        if actor is None:
            return api.refusal("FORBIDDEN")
        body, error = api.parse_body(request, CreateMandateRequest)
        if error:
            return error
        if body.valid_until.tzinfo is None:
            return api.refusal("INVALID_REQUEST", "valid_until needs a time zone offset")
        param = (f"project_id={project_id};action=create;version_id={body.version_id};"
                 f"owner_user_id={body.owner_user_id};operations={','.join(body.operations)};"
                 f"max_runtime_s={body.max_runtime_s};valid_until={body.valid_until.isoformat()}")
        if body.identity is not None:
            param += f";identity={body.identity}"
        return task_answer(manage_mandate(self.module, param, actor_user_id=actor),
                           lambda result: {"ok": True, "id": result["mandate_ref"], "state": result["state"]})


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
