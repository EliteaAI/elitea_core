"""Which agent versions external MCP clients reach in a project and under which tool name (#1597, #7341, C-1009):
read by the MCP page and by the local MCP client, so it exposes what the Elitea MCP server does."""
from dataclasses import asdict

from pylon.core.tools import log
from tools import api_tools, auth, db, register_openapi

from ...utils.application_utils import list_applications_api
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.governance import is_governed
from ...utils.mcp_exposed_version import MCP_TAG, read_exposed_versions
from ...utils.tags import list_tags

NO_ROLES = {mode: {"system": False, "super_admin": False, "admin": False, "editor": False, "viewer": False}
            for mode in ("administration", "default")}


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="MCP Exposed Agent Versions",
        description=("Every agent version of the project that external MCP clients reach: the versions tagged 'mcp', "
                     "and under enforce only those with an active identity binding. The default version's tool is "
                     "named after the agent, any other one <agent>_<version>. {rows: [{application_id, "
                     "application_name, version_id, version_name, tool_name, description, rule}]}; rule is tagged, or "
                     "bound under enforce."),
        parameters=[{"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}],
        mcp_tool=False, available_to_users=False,
    )
    # the agent list's own view right; recommended to nobody here (the agents' routes register it)
    @auth.decorators.check_api({"permissions": ["models.applications.applications.list"], "recommended_roles": NO_ROLES})
    def get(self, project_id: int, **kwargs):
        try:
            tag_ids = [tag["id"] for tag in list_tags(project_id, {})["rows"] if tag["name"] == MCP_TAG][:1]
            if not tag_ids:
                return {"rows": []}, 200
            with db.get_session(project_id) as session:
                applications = list_applications_api(
                    project_id=project_id, limit=None, session=session, tags=tag_ids)["applications"]
                exposed = read_exposed_versions(session, project_id, applications, is_governed(self.module))
        except Exception as exc:  # pylint: disable=W0703
            log.error("mcp exposed versions: failed (%s)", type(exc).__name__)
            return {"error": "the versions could not be read", "code": "DEPENDENCY_UNAVAILABLE"}, 503
        return {"rows": [asdict(version) for version in exposed]}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>"])
    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
