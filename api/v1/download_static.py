import os
from typing import Optional

from flask import request
from pydantic import BaseModel, ValidationError

from ...scripts.tool_icons import download_github_repo_zip, unzip_file
from tools import api_tools, auth, db, serialize, db_tools, config as c, this

# from pylon.core.tools import log


class DownloadStatic(BaseModel):
    local_dir: Optional[str] = this.descriptor.config.get("icons_base_path", "/data/static")
    repo_owner: Optional[str] = this.descriptor.config.get("icons_repo_owner", "EliteaAI")
    repo_name: Optional[str] = this.descriptor.config.get("icons_repo_name", "elitea_static")
    subfolder: Optional[str] = this.descriptor.config.get("icons_zip_subfolder", None)


class AdminAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.download_static.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self):
        payload = dict(request.json)

        try:
            download_static = DownloadStatic.parse_obj(payload)
        except ValidationError as e:
            return e.errors(), 400

        if not os.path.exists(download_static.local_dir):
            os.makedirs(download_static.local_dir)

        zip_path = download_github_repo_zip(
            repo_owner=download_static.repo_owner,
            repo_name=download_static.repo_name,
            local_dir=download_static.local_dir,
        )

        if not zip_path.get('ok'):
            return serialize(zip_path), 400

        result = unzip_file(
            zip_path.get('path'), download_static.local_dir, download_static.subfolder
        )

        return serialize(result), 200 if result.get('ok') else 400


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
