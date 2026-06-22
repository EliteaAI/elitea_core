from flask import request

from tools import api_tools, config as c, auth, register_openapi
from pylon.core.tools import log

from ...utils.skill_export_import import (
    import_skill_md,
    validate_skill_import_filename,
)
from ...utils.skill_utils import SkillError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Import a skill from a standalone Markdown file",
        description=(
            "Creates a new skill from an uploaded Markdown (.md) file. The file is "
            "sent as multipart/form-data under the 'file' field (a raw Markdown "
            "string in the JSON body's 'content' field is also accepted). Only .md "
            "files are accepted (400 otherwise). The frontmatter must contain at "
            "least name and description; the body becomes the skill instructions. "
            "Re-importing a name that already exists creates a new, independent "
            "skill (skill names are not unique)."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        author_id = auth.current_user().get("id")

        content = None
        filename = None

        files = getattr(request, 'files', None)
        if files and 'file' in files:
            uploaded = files['file']
            filename = uploaded.filename

            try:
                validate_skill_import_filename(filename)
            except ValueError as e:
                return {"error": str(e)}, 400

            try:
                raw = uploaded.read()
                content = raw.decode('utf-8') if isinstance(raw, bytes) else raw
            except (UnicodeDecodeError, ValueError) as e:
                return {"error": f"Could not read file as UTF-8 text: {e}"}, 400
        else:
            payload = request.json if request.is_json else None
            if isinstance(payload, dict):
                content = payload.get('content')
                filename = payload.get('filename')
                if filename:
                    try:
                        validate_skill_import_filename(filename)
                    except ValueError as e:
                        return {"error": str(e)}, 400

        if not content or not str(content).strip():
            return {"error": "No Markdown content provided. Upload a .md file or send a 'content' field."}, 400

        try:
            detail = import_skill_md(
                project_id=project_id,
                content=content,
                author_id=author_id,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        except ValueError as e:
            return {"error": str(e)}, 400

        return detail, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
