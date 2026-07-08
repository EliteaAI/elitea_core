from typing import Optional
from uuid import uuid4
from pathlib import Path

from flask import request
from pydantic import ValidationError

from tools import config as c, api_tools, auth, db, register_openapi
from ...models.skill import SkillVersion
from ...models.pd.icon_meta import UpdateIcon

from ...utils.constants import PROMPT_LIB_MODE


# routes/skill_icon
FLASK_ROUTE_URL: str = 'elitea_core.skill_icon'
MAX_FILE_SIZE_KB: int = 512
MAX_ICON_DIMENSION: int = 64


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List uploaded skill icons",
        description=(
            "Lists icons uploaded for skills in the project, paginated via "
            "'skip'/'limit' query params. Each row carries the icon file name "
            "and its public URL."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skip", "in": "query", "required": False, "schema": {"type": "integer", "default": 0}},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 200}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.upload_icon.get"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        skip = int(request.args.get('skip', 0))
        limit = int(request.args.get('limit', 200))
        folder_path: Path = self.module.skill_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        results = self.module.context.rpc_manager.call.social_get_icons_list(
            project_id, FLASK_ROUTE_URL, folder_path, skip, limit
        )
        return results, 200

    @register_openapi(
        name="Upload a skill icon",
        description=(
            "Uploads an image (multipart 'file', max 512 KB) as a skill icon; the "
            "image is resized to at most 64x64 and stored as PNG. When a "
            "skill_version_id path segment is supplied, the icon is immediately "
            "bound to that skill version (stored in its meta.icon_meta)."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_version_id", "in": "path", "required": False, "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.upload_icon.post"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, skill_version_id: Optional[int] = None, **kwargs):
        if 'file' not in request.files:
            return {'error': 'No file in request.files'}, 400

        file = request.files['file']

        # kB
        max_file_size = MAX_FILE_SIZE_KB * 1024

        # move the cursor to the end of the file
        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)

        if file_size > max_file_size:
            return {'error': f'File size exceeds {MAX_FILE_SIZE_KB} KB'}, 400

        # Clamp client-supplied dimensions: 0/negative values would bypass the
        # min-dimension guard in social_save_image, oversized values inflate the
        # thumbnail box.
        final_width = min(max(int(request.form.get('width', MAX_ICON_DIMENSION)), 1), MAX_ICON_DIMENSION)
        final_height = min(max(int(request.form.get('height', MAX_ICON_DIMENSION)), 1), MAX_ICON_DIMENSION)
        folder_path: Path = self.module.skill_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path: Path = folder_path.joinpath(f'{uuid4()}.png')

        result = self.module.context.rpc_manager.call.social_save_image(
            file, file_path, FLASK_ROUTE_URL, final_width, final_height, project_id
        )
        if result['ok']:
            if skill_version_id:
                self.module.context.rpc_manager.call.social_update_icon_with_entity(
                    project_id, skill_version_id,
                    self.module.skill_icon_path, result['data'], SkillVersion
                )
            return result['data'], 200
        else:
            return result['error'], 400

    @register_openapi(
        name="Set a skill version's icon",
        description=(
            "Binds an already-uploaded icon (by name/url meta payload) to the "
            "given skill version, storing it in the version's meta.icon_meta. "
            "Sending empty name/url resets the version to the default icon."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_version_id", "in": "path", "schema": {"type": "integer"}},
        ],
        response_model=UpdateIcon,
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.upload_icon.update"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id, skill_version_id, **kwargs):
        raw = dict(request.json)
        try:
            update_input = UpdateIcon.parse_obj(raw)
        except ValidationError as e:
            return {'error': f'Validation error on item: {e}'}, 400

        with db.get_session(project_id) as session:
            version = session.query(SkillVersion).filter(
                SkillVersion.id == skill_version_id
            ).first()
            if not version:
                return {'ok': False, 'msg': f'There is no such version id {skill_version_id}'}

            if version.meta:
                version.meta['icon_meta'] = update_input.dict()
            else:
                version.meta = {'icon_meta': update_input.dict()}
            session.commit()

        return {'updated': True}, 200

    @register_openapi(
        name="Delete an uploaded skill icon",
        description=(
            "Deletes an uploaded skill icon file and unlinks it from every skill "
            "version that uses it (those versions revert to the default icon)."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "icon_name", "in": "path", "schema": {"type": "string"}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.upload_icon.delete"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id, icon_name, **kwargs):
        folder_path: Path = self.module.skill_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)

        return self.module.context.rpc_manager.call.social_delete_icon_from_entity(
            project_id, icon_name, folder_path, SkillVersion
        ), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '',
        '<string:mode>/<int:project_id>',
        '<string:mode>/<int:project_id>/<int:skill_version_id>',
        '<string:mode>/<int:project_id>/<string:icon_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
