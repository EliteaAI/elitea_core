from io import BytesIO
from pathlib import PurePosixPath

from flask import request, send_file
from pydantic import ValidationError
from werkzeug.exceptions import RequestEntityTooLarge

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.skill_file import SkillFileRenameModel, SkillFileTextWriteModel
from ...utils.skill_files import (
    delete_version_file,
    describe_size,
    read_version_file,
    rename_version_file,
    write_version_file,
)
from ...utils.skill_utils import SkillError, get_max_file_bytes
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.folder_access import require_folder_access


FILE_PATH = '<string:mode>/<int:project_id>/<int:skill_id>/<int:version_id>/<path:path>'

PATH_PARAMETERS = [
    {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
    {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
    {"name": "version_id", "in": "path", "schema": {"type": "integer"}},
    {"name": "path", "in": "path", "schema": {"type": "string"},
     "description": "File path under the skill root, such as references/guide.md"},
]

UPLOAD_ENVELOPE_BYTES = 64 * 1024
JSON_ESCAPED_BYTES_PER_BYTE = 6
JSON_MIMETYPE = 'application/json'

WRITE_ROLES = {
    c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
}


def read_uploaded_bytes():
    """The bytes a write carries: a multipart ``file`` part, or UTF-8 text in a JSON ``content`` field. The body
    is read only up to the per-file cap plus room for its envelope, a JSON one with room for every byte escaped as
    ``\\uXXXX``; a larger one is a 413 before any of it is kept. The decoded content is held to the cap after parsing."""
    max_file_bytes = get_max_file_bytes()
    encoded_bytes_per_byte = JSON_ESCAPED_BYTES_PER_BYTE if request.mimetype == JSON_MIMETYPE else 1
    max_body_bytes = encoded_bytes_per_byte * max_file_bytes + UPLOAD_ENVELOPE_BYTES
    too_large = {"error": f"the upload is too large: a skill file can be at most {describe_size(max_file_bytes)}"}, 413
    if request.content_length is not None and request.content_length > max_body_bytes:
        return None, too_large
    request.max_content_length = max_body_bytes
    try:
        uploaded = request.files.get('file')
        if uploaded is not None:
            return uploaded.read(), None
        body = request.get_json(silent=True)
    except RequestEntityTooLarge:
        return None, too_large
    if not isinstance(body, dict):
        return None, ({"error": "send the file as a multipart 'file' part or as JSON {\"content\": \"...\"}"}, 400)
    try:
        return SkillFileTextWriteModel.model_validate(body).content.encode('utf-8'), None
    except ValidationError as exc:
        return None, ({"error": exc.errors()}, 400)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Download one file bundled with a skill version",
        description=(
            "Returns the raw bytes of the file at the given path under the skill root. The path is matched "
            "exactly first, then case-insensitively. The ETag header carries the file's sha256. The response is "
            "sandboxed, so an uploaded page or image never runs as the platform."
        ),
        parameters=PATH_PARAMETERS,
        path_suffix_override=FILE_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=False,
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
    def get(self, project_id: int, skill_id: int, version_id: int, path: str, **kwargs):
        try:
            entry, content = read_version_file(
                project_id=project_id, skill_id=skill_id, version_id=version_id, path=path,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        response = send_file(
            BytesIO(content),
            mimetype=entry['content_type'],
            download_name=PurePosixPath(entry['path']).name,
            as_attachment=False,
        )
        response.headers['ETag'] = entry['sha256']
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Content-Security-Policy'] = 'sandbox'
        return response

    @register_openapi(
        name="Store one file in a skill version",
        description=(
            "Creates or replaces the file at the given path under the skill root. Send either a multipart "
            "'file' part (any content) or a JSON body {\"content\": \"...\"} with UTF-8 text. Rejected with "
            "the reason when the path leaves the skill root, the version is published, or a size cap is "
            "exceeded. SKILL.md is the version's instructions and cannot be stored as a file."
        ),
        parameters=PATH_PARAMETERS,
        request_body=SkillFileTextWriteModel,
        path_suffix_override=FILE_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": WRITE_ROLES,
    })
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def put(self, project_id: int, skill_id: int, version_id: int, path: str, **kwargs):
        data, error = read_uploaded_bytes()
        if error:
            return error
        try:
            entry = write_version_file(
                project_id=project_id, skill_id=skill_id, version_id=version_id, path=path, data=data,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return entry, 200

    @register_openapi(
        name="Rename one file of a skill version",
        description="Moves the file at the given path to new_path under the skill root of a draft version.",
        parameters=PATH_PARAMETERS,
        request_body=SkillFileRenameModel,
        path_suffix_override=FILE_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": WRITE_ROLES,
    })
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def patch(self, project_id: int, skill_id: int, version_id: int, path: str, **kwargs):
        try:
            rename = SkillFileRenameModel.model_validate(request.get_json(silent=True) or {})
        except ValidationError as exc:
            return {"error": exc.errors()}, 400
        try:
            entry = rename_version_file(
                project_id=project_id, skill_id=skill_id, version_id=version_id,
                path=path, new_path=rename.new_path,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return entry, 200

    @register_openapi(
        name="Delete one file of a skill version",
        description="Removes the file at the given path from a draft version.",
        parameters=PATH_PARAMETERS,
        path_suffix_override=FILE_PATH,
        tags=["elitea_core/skills"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": WRITE_ROLES,
    })
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id', write=True)
    def delete(self, project_id: int, skill_id: int, version_id: int, path: str, **kwargs):
        try:
            delete_version_file(project_id=project_id, skill_id=skill_id, version_id=version_id, path=path)
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return {"ok": True}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>/<int:version_id>/<path:path>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
