"""What an entity still needs to run governed for the caller (#7269, C-1009): the Save card's list.

``GET .../governance_readiness/prompt_lib/<project_id>/<entity_kind>/<entity_id>`` with ``entity_kind`` ``version``
(an agent or pipeline version), ``toolkit`` or ``schedule``. Read only (``utils/governance_readiness``): the same checks
as admission, no audit row, no decision. Under off and audit only: ``{items: []}`` with nothing read. The caller needs
the entity's own view right in the project; a schedule is its owner's (or a schedule admin's).
"""
from pylon.core.tools import log
from tools import api_tools, auth, register_openapi

from ...utils import binding_api as api
from ...utils import governance_readiness as readiness
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.governance import is_governed

VIEW_RIGHT = {
    "version": "models.applications.version.details",
    "toolkit": "models.applications.tool.details",
    "schedule": "models.applications.schedules.details",
}
# These view rights are registered by their own routes: recommending nobody here adds no role to any of them
NO_ROLES = {mode: {"system": False, "super_admin": False, "admin": False, "editor": False, "viewer": False}
            for mode in ("administration", "default")}


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Governance Readiness",
        description=("What an agent or pipeline version, a toolkit or a schedule still needs to run governed for you: "
                     "{items: [{key, kind, ref, title, state, requestable, for}]}. Empty while governance is not "
                     "enforced."),
        tags=[api.OPENAPI_TAG],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "entity_kind", "in": "path", "required": True,
             "schema": {"type": "string", "enum": list(readiness.KINDS)}},
            {"name": "entity_id", "in": "path", "required": True, "schema": {"type": "integer"}},
        ],
        responses={"200": {"description": "{items}"}, "404": {"description": "No such entity for you here."}},
        mcp_tool=False, available_to_users=False,
    )
    # any of the view rights opens it; the one of the entity's kind is asked below
    @auth.decorators.check_api({"permissions": list(VIEW_RIGHT.values()), "recommended_roles": NO_ROLES})
    def get(self, project_id: int, entity_kind: str, entity_id: int, **kwargs):
        if not is_governed(self.module):
            return {"items": []}, 200   # off and audit only: nothing is read
        refused = api.project_gate(self.module)
        if refused:
            return refused
        if entity_kind not in readiness.KINDS:
            return api.refusal("INVALID_REQUEST", "entity_kind is one of: version, toolkit, schedule")
        if not api.has_permission(VIEW_RIGHT[entity_kind], project_id, PROMPT_LIB_MODE):
            return api.refusal("NOT_FOUND")
        try:
            found = readiness.readiness(self.module, project_id, entity_kind, entity_id, api.current_actor())
        except readiness.EntityNotFound:
            return api.refusal("NOT_FOUND")
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance readiness: failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")
        return {"items": readiness.public(found)}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<string:entity_kind>/<int:entity_id>"])
    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
