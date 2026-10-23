"""The project's agents with their binding status, for the create-binding dialog (#7105, C-1009)."""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils import binding_api as api
from ...utils.binding_queries import list_agents


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Agents For Governance (Admin)",
        description=(
            "Agents of the project in name order, each with its latest 50 versions and the bindings every version "
            "already has (id, identity mode, state). Returns {total, rows}."
        ),
        tags=[api.OPENAPI_TAG],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
            {"name": "search", "in": "query", "required": False, "schema": {"type": "string"}},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Default 20, at most 50."},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}},
        ],
        responses={"200": {"description": "{total, rows}"}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api(api.check_spec(api.VIEW))
    def get(self, project_id: int, **kwargs):
        params, error = api.page_args(request.args, (), "name")
        if error:
            return error
        try:
            return list_agents(project_id, search=params["search"], limit=params["limit"], offset=params["offset"]), 200
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance agents: list failed (%s)", type(exc).__name__)
            return api.refusal("DEPENDENCY_UNAVAILABLE")


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {c.ADMINISTRATION_MODE: AdminAPI}
