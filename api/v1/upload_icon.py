from typing import Optional
from uuid import uuid4
from pathlib import Path

from flask import request, url_for
from pydantic import ValidationError

from tools import config as c, api_tools, auth, db
from ...models.all import ApplicationVersion
from ...models.pd.icon_meta import UpdateIcon

from ...utils.constants import PROMPT_LIB_MODE, ICON_PATH_DELIMITER


# routes/application_icon
FLASK_ROUTE_URL: str = 'elitea_core.application_icon'
MAX_FILE_SIZE_KB: int = 512


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.upload_icon.get"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    def get(self, project_id: int, **kwargs):
        skip = int(request.args.get('skip', 0))
        limit = int(request.args.get('limit', 200))
        folder_path: Path = self.module.application_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        results = self.module.context.rpc_manager.call.social_get_icons_list(
            project_id, FLASK_ROUTE_URL, folder_path, skip, limit
        )
        return results, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.upload_icon.post"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    def post(self, project_id: int, application_version_id: Optional[int] = None, **kwargs):
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

        final_width = int(request.form.get('width', 64))
        final_height = int(request.form.get('height', 64))
        folder_path: Path = self.module.application_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path: Path = folder_path.joinpath(f'{uuid4()}.png')

        result = self.module.context.rpc_manager.call.social_save_image(
            file, file_path, FLASK_ROUTE_URL, final_width, final_height, project_id
        )
        if result['ok']:
            if application_version_id:
                self.module.context.rpc_manager.call.social_update_icon_with_entity(
                    project_id, application_version_id,
                    self.module.application_icon_path, result['data'], ApplicationVersion
                )
            return result['data'], 200
        else:
            return result['error'], 400

    @auth.decorators.check_api({
        "permissions": ["models.applications.upload_icon.update"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    def put(self, project_id, application_version_id, **kwargs):
        raw = dict(request.json)
        try:
            update_input = UpdateIcon.parse_obj(raw)
        except ValidationError as e:
            return {'error': f'Validation error on item: {e}'}, 400

        with db.get_session(project_id) as session:
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == application_version_id
            ).first()
            if not version:
                return {'ok': False, 'msg': f'There is no such version id {application_version_id}'}

            if version.meta:
                version.meta['icon_meta'] = update_input.dict()
            else:
                version.meta = {'icon_meta': update_input.dict()}
            session.commit()

        return {'updated': True}, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.upload_icon.delete"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    def delete(self, project_id, icon_name, **kwargs):
        folder_path: Path = self.module.application_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)

        return self.module.context.rpc_manager.call.social_delete_icon_from_entity(
            project_id, icon_name, folder_path, ApplicationVersion
        ), 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '',
        '<string:mode>/<string:file_name>',
        '<string:mode>/<int:project_id>',
        '<string:mode>/<int:project_id>/<int:application_version_id>',
        '<string:mode>/<int:project_id>/<string:icon_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
