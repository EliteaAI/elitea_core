"""A member's request (#7269, C-1009): file what readiness says an entity needs, as one batch.

``POST .../governance_requests/prompt_lib/<project_id>`` ``{entity_kind, entity_id, keys?}``: every requestable item
(``keys``: those only) is created and submitted by the caller as the existing rows in their waiting states, in one
transaction, all or nothing (``utils/governance_requests``). Needs ``governance.request`` in the project; never an
approval. Answers ``{filed: [{key, kind, id}], items}`` (the readiness after the batch).
"""
from typing import Annotated, List, Optional

from flask import request
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from pylon.core.tools import log
from tools import api_tools, auth, register_openapi

from ...utils import binding_api as api
from ...utils import governance_readiness as readiness
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.governance_requests import RequestRefused, file_requests

STATUS = {"STALE_READINESS": 409, "PROJECT_GOVERNANCE_OFF": 409, "VERSION_NOT_FOUND": 404, "NOT_FOUND": 404,
          "REFERENCE_EXISTS": 409, "GRANT_EXISTS": 409, "BINDING_EXISTS": 409, "MANDATE_EXISTS": 409,
          "DENIED_BINDING": 409, "OWNER_NOT_LINKED": 409, "OWNER_UNLINKED": 409, "INVALID_STATE": 409,
          "CONCURRENT_CHANGE": 409, "FORBIDDEN": 403, "INVALID_REQUEST": 400, "AUDIT_UNAVAILABLE": 503,
          "DEPENDENCY_UNAVAILABLE": 503}
MESSAGES = {
    "STALE_READINESS": "What this needs has changed; read it again. Nothing was requested.",
    "PROJECT_GOVERNANCE_OFF": "Requests apply only while credential governance is enforced; nothing was requested.",
    "MANDATE_EXISTS": "Another member's schedule already holds the mandate of this agent version; nothing was "
                      "requested.",
}


class RequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    entity_kind: StrictStr
    entity_id: Annotated[StrictInt, Field(ge=1, le=readiness.MAX_INT32)]
    keys: Optional[List[StrictStr]] = None


def _refused(exc):
    body, _ = api.refusal(exc.code, MESSAGES.get(exc.code) or api.MESSAGES.get(exc.code) or
                          "the request was refused; nothing was requested")
    if exc.key:
        body["key"] = exc.key
    return body, STATUS.get(exc.code, 409)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Request Governed Setup",
        description=("File what an entity still needs to run governed (its readiness), as your request: one "
                     "transaction, all or nothing; a project admin or a platform operator approves. Body {entity_kind, "
                     "entity_id, keys?}. Returns {filed, items}."),
        tags=[api.OPENAPI_TAG],
        parameters=[{"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}],
        request_body=RequestBody,
        responses={"200": {"description": "{filed, items}"}, "403": {"description": "Not allowed."},
                   "409": {"description": "Stale, or not enforced."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api({"permissions": [api.REQUEST], "recommended_roles": api.REQUEST_ROLES})
    def post(self, project_id: int, **kwargs):
        refused = api.project_gate(self.module)
        if refused:
            return refused
        body, error = api.parse_body(request, RequestBody)
        if error:
            return error
        if body.entity_kind not in readiness.KINDS:
            return api.refusal("INVALID_REQUEST", "entity_kind is one of: version, toolkit, schedule")
        actor = api.current_actor()
        try:
            filed = file_requests(self.module, project_id, body.entity_kind, body.entity_id, actor, keys=body.keys)
            items = readiness.public(readiness.readiness(self.module, project_id, body.entity_kind, body.entity_id,
                                                         actor))
        except RequestRefused as exc:
            return _refused(exc)
        except readiness.EntityNotFound:
            return api.refusal("NOT_FOUND")
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance requests: failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")
        return {"filed": filed, "items": items}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
