import uuid
from pathlib import Path

from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, db, config as c, register_openapi, serialize

from ...models.pd.attachment import ChunkUploadPayload
from ...models.pd.conversation_import import ImportCommitPayload
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_import import (
    MSG_TOO_LARGE,
    ChatImportError,
    commit_import,
    discard_user_staging,
    get_import_limits,
    prepare_import,
)
from ...utils.conversation_utils import get_conversation_details
from ...utils.file_utils import (
    CHUNKS_TEMP_DIR,
    are_all_chunks_received,
    cleanup_chunks,
    merge_chunks,
    save_chunk,
)

_RECOMMENDED_ROLES = {
    c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
}


def _public_project_error(project_id: int):
    from ...utils.utils import get_public_project_id  # pylint: disable=C0415
    if get_public_project_id() == project_id:
        return {"error": "Chats can not be imported into the public project"}, 400
    return None


def _receive_upload(project_id: int):
    """Store the uploaded (optionally chunked) file. Returns (path, file_name, None) or (None, None, response)."""
    max_bytes = get_import_limits(project_id)['total_mb'] * 1024 * 1024
    too_large = {"error": MSG_TOO_LARGE.format(max_bytes // (1024 * 1024))}, 400
    upload = request.files.get('file')
    if upload is None:
        return None, None, ({"error": "No file provided"}, 400)

    merged_dir = Path(CHUNKS_TEMP_DIR) / 'merged'
    merged_dir.mkdir(parents=True, exist_ok=True)
    chunk_args = [request.form.get(k) for k in ('file_id', 'chunk_index', 'total_chunks')]

    if not all(chunk_args):
        size = upload.seek(0, 2)
        upload.seek(0)
        if size > max_bytes:
            return None, None, too_large
        file_name = upload.filename or 'chat'
        path = merged_dir / f'chat_import_{uuid.uuid4().hex}.enc'
        upload.save(path)
        return path, file_name, None

    try:
        chunk = ChunkUploadPayload(
            file_id=chunk_args[0],
            chunk_index=int(chunk_args[1]),
            total_chunks=int(chunk_args[2]),
            file_name=request.form.get('file_name') or upload.filename or 'chat',
        )
    except (ValidationError, ValueError) as e:
        return None, None, ({"error": f"Invalid chunk parameters: {e}"}, 400)

    save_chunk(file_id=chunk.file_id, chunk_index=chunk.chunk_index, chunk_data=upload.stream)
    if not are_all_chunks_received(chunk.file_id, chunk.total_chunks):
        return None, None, ({
            "status": "chunk_received",
            "file_id": chunk.file_id,
            "chunk_index": chunk.chunk_index,
            "total_chunks": chunk.total_chunks,
        }, 202)

    path = merged_dir / f'chat_import_{chunk.file_id}.enc'
    try:
        size = merge_chunks(file_id=chunk.file_id, total_chunks=chunk.total_chunks, output_path=path)
    finally:
        cleanup_chunks(chunk.file_id)
    if size > max_bytes:
        path.unlink(missing_ok=True)
        return None, None, too_large
    return path, chunk.file_name, None


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Prepare Conversation Import",
        description=(
            "Upload an encrypted chat export (.json.enc or .zip.enc), optionally in chunks. "
            "Validates it and returns an import preview with an import_id."
        ),
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.create"],
        "recommended_roles": _RECOMMENDED_ROLES,
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        denied = _public_project_error(project_id)
        if denied:
            return denied
        user_id = auth.current_user()['id']

        path, file_name, response = _receive_upload(project_id)
        if response is not None:
            return response
        try:
            return prepare_import(project_id, user_id, path, file_name), 200
        except ChatImportError as e:
            return {"error": str(e)}, 400
        except Exception:  # pylint: disable=W0703
            log.exception("Chat import: failed to prepare file for project %s", project_id)
            return {"error": "Failed to read the chat file"}, 500
        finally:
            path.unlink(missing_ok=True)

    @register_openapi(
        name="Commit Conversation Import",
        description="Create a new conversation from a prepared import, with the selected attachments.",
        request_body=ImportCommitPayload,
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.create"],
        "recommended_roles": _RECOMMENDED_ROLES,
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, import_id: str, **kwargs):
        denied = _public_project_error(project_id)
        if denied:
            return denied
        user_id = auth.current_user()['id']
        try:
            payload = ImportCommitPayload.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:
            try:
                conversation_id, context_strategy, failed = commit_import(
                    session, project_id, user_id, import_id, payload.selected_attachments
                )
            except ChatImportError as e:
                return {"error": str(e)}, 400

            session.expire_all()
            serialized = serialize(get_conversation_details(session, conversation_id, project_id, user_id))
            if context_strategy and 'meta' in serialized:
                serialized['meta']['context_strategy'] = context_strategy
        return {"conversation": serialized, "failed_attachments": failed}, 201

    @register_openapi(
        name="Cancel Conversation Import",
        description="Discard a prepared import.",
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.create"],
        "recommended_roles": _RECOMMENDED_ROLES,
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, import_id: str, **kwargs):
        try:
            discard_user_staging(import_id, project_id, auth.current_user()['id'])
        except ChatImportError:
            pass
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<string:import_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
