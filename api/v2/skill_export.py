from traceback import format_exc

from flask import Response

from tools import api_tools, config as c, auth, register_openapi
from pylon.core.tools import log

from ...utils.skill_export_import import export_skill_md
from ...utils.skill_utils import SkillError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Export a skill as a standalone Markdown file",
        description=(
            "Exports the specified skill as a downloadable Markdown (.md) file "
            "with YAML frontmatter (type, name, description, optional version and "
            "tags) and the skill instructions as the body. When a version_name "
            "path segment is supplied that version is exported (404 if it does not "
            "exist); otherwise the default ('base') version is used."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_name", "in": "path", "required": False, "schema": {"type": "string"}, "description": "Optional version name to export (defaults to 'base')."},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.export"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, skill_id: int, version_name: str | None = None, **kwargs):
        try:
            result = export_skill_md(
                project_id=project_id,
                skill_id=skill_id,
                version_name=version_name,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        except Exception as e:
            log.error(f"Skill MD export failed: {e}\n{format_exc()}")
            return {"error": "Internal server error"}, 500

        if not result.get('ok'):
            return {"error": result.get('msg', 'Export failed')}, 404

        return Response(
            result['content'],
            mimetype='text/markdown; charset=utf-8',
            headers={
                'Content-Disposition': f'attachment; filename="{result["filename"]}"',
                'Access-Control-Expose-Headers': 'Content-Disposition',
            },
        )


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
