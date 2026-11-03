"""Which version of each agent external MCP clients reach in a project (#7341, C-1009), for the MCP page."""
from pylon.core.tools import log
from sqlalchemy import select
from tools import api_tools, auth, db, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.governance import is_governed
from ...utils.mcp_exposed_version import bound_version_ids, exposed_version

MAX_AGENTS = 200
NO_ROLES = {mode: {"system": False, "super_admin": False, "admin": False, "editor": False, "viewer": False}
            for mode in ("administration", "default")}


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="MCP Exposed Agent Versions",
        description=("For each agent of the project (at most 200), the version the Elitea MCP server gives external "
                     "clients and why: bound (the version with an active identity binding, under enforce), published "
                     "(the latest published one) or stored (the first stored one). {rows: [{application_id, "
                     "application_name, version_id, version_name, rule}]}"),
        parameters=[{"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}],
        mcp_tool=False, available_to_users=False,
    )
    # the agent list's own view right; recommended to nobody here (the agents' routes register it)
    @auth.decorators.check_api({"permissions": ["models.applications.applications.list"], "recommended_roles": NO_ROLES})
    def get(self, project_id: int, **kwargs):
        from ...models.all import Application, ApplicationVersion  # pylint: disable=C0415
        governed = is_governed(self.module)
        try:
            with db.get_session(project_id) as session:
                versions = session.execute(
                    select(ApplicationVersion.id, ApplicationVersion.application_id, ApplicationVersion.name,
                           ApplicationVersion.status, ApplicationVersion.created_at, Application.name.label("app"))
                    .join(Application, Application.id == ApplicationVersion.application_id)
                    .order_by(ApplicationVersion.application_id, ApplicationVersion.id)).all()
                by_agent = {}
                for version in versions:
                    by_agent.setdefault(version.application_id, []).append(version)
                agents = sorted(by_agent)[:MAX_AGENTS]
                bound = bound_version_ids(session, project_id, agents) if governed else {}
        except Exception as exc:  # pylint: disable=W0703
            log.error("mcp exposed versions: failed (%s)", type(exc).__name__)
            return {"error": "the versions could not be read", "code": "DEPENDENCY_UNAVAILABLE"}, 503
        rows = []
        for application_id in agents:
            version, rule = exposed_version(by_agent[application_id], bound.get(application_id, set()), governed)
            rows.append({"application_id": application_id, "application_name": version.app, "version_id": version.id,
                         "version_name": version.name, "rule": rule})
        return {"rows": rows}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
