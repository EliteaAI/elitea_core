"""One agent identity binding: the operator detail (#7105, C-1009)."""
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils import binding_api as api
from ...utils.binding_queries import get_detail


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Agent Identity (Admin)",
        description=(
            "One binding with the digest as stored and as it is now, and the inputs the digest is made of "
            "(agent type, model, the length and hash of the instructions, toolkits with the stored configuration "
            "each points to). Never the instruction text. A binding of another project is 404."
        ),
        tags=[api.OPENAPI_TAG],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "required": True, "schema": {"type": "string", "format": "uuid"}},
        ],
        responses={"200": {"description": "The binding."}, "404": {"description": "No such binding in this project."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.VIEW))
    def get(self, project_id: int, binding_id, **kwargs):
        try:
            detail = get_detail(project_id, binding_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("agent identity: read failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")
        return (detail, 200) if detail is not None else api.refusal("NOT_FOUND")


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<uuid:binding_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
