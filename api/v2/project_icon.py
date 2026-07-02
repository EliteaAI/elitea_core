from uuid import uuid4
from pathlib import Path

from flask import request
from tools import config as c, api_tools, auth
from ...utils.constants import PROMPT_LIB_MODE

FLASK_ROUTE_URL: str = "elitea_core.project_icon"
MAX_FILE_SIZE_KB: int = 512


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        skip = int(request.args.get("skip", 0))
        limit = int(request.args.get("limit", 200))
        folder_path: Path = self.module.project_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        results = self.module.context.rpc_manager.call.social_get_icons_list(
            project_id, FLASK_ROUTE_URL, folder_path, skip, limit
        )
        return results, 200

    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.edit"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        if "file" not in request.files:
            return {"error": "No file in request.files"}, 400

        file = request.files["file"]

        max_file_size = MAX_FILE_SIZE_KB * 1024

        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)

        if file_size > max_file_size:
            return {"error": f"File size exceeds {MAX_FILE_SIZE_KB} KB"}, 400

        final_width = int(request.form.get("width", 64))
        final_height = int(request.form.get("height", 64))
        folder_path: Path = self.module.project_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path: Path = folder_path.joinpath(f"{uuid4()}.png")

        result = self.module.context.rpc_manager.call.social_save_image(
            file, file_path, FLASK_ROUTE_URL, final_width, final_height, project_id
        )
        if result["ok"]:
            return result["data"], 200
        else:
            return result["error"], 400

    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.edit"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, icon_name: str = None, **kwargs):
        if not icon_name or "/" in icon_name or ".." in icon_name:
            return {"error": "Invalid icon name"}, 400

        folder_path: Path = self.module.project_icon_path.joinpath(str(project_id))
        folder_path.mkdir(parents=True, exist_ok=True)

        file_path = folder_path.joinpath(icon_name)
        if file_path.exists():
            file_path.unlink()
            return {"ok": True, "msg": "Icon deleted"}, 200
        return {"ok": False, "msg": "Icon not found"}, 404


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<string:mode>/<int:project_id>",
            "<string:mode>/<int:project_id>/<string:icon_name>",
        ]
    )

    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
