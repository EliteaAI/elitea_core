from tools import api_tools, config as c, auth, register_openapi

from ...utils.skill_files import list_version_files
from ...utils.skill_utils import SkillError
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.folder_access import require_folder_access


VERSION_PATH = '<string:mode>/<int:project_id>/<int:skill_id>/<int:version_id>'


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List the files bundled with a skill version",
        description=(
            "Returns the manifest of the files stored with the skill version: path under the skill root, "
            "size in bytes, sha256, content type and whether the file is text. File contents are not "
            "included; read one file with the skill_file endpoint."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "schema": {"type": "integer"}},
        ],
        path_suffix_override=VERSION_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id')
    def get(self, project_id: int, skill_id: int, version_id: int, **kwargs):
        try:
            files = list_version_files(project_id=project_id, skill_id=skill_id, version_id=version_id)
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return {"files": files}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
