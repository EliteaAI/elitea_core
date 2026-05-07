from io import BytesIO
from typing import List, Dict, Any, Tuple
from pylon.core.tools import web
from werkzeug.datastructures import FileStorage

from ..utils.attachments import process_uploaded_files, handle_chunked_upload


class RPC:
    @web.rpc("chat_upload_chunk_rpc", "upload_chunk_rpc")
    def upload_chunk_rpc(
        self,
        project_id: int,
        conversation_id: int,
        file_id: str,
        chunk_index: int,
        total_chunks: int,
        file_name: str,
        chunk_data: bytes,
        overwrite: bool = False,
        user_id: int = None,
    ) -> Tuple[Dict[str, Any], int]:
        """
        Handle chunked file upload.

        Args:
            project_id: Project ID
            conversation_id: Conversation ID (integer)
            file_id: Unique identifier for the upload session
            chunk_index: Index of current chunk (0-based)
            total_chunks: Total number of chunks
            file_name: Original filename
            chunk_data: Chunk bytes
            overwrite: Whether to overwrite existing files
            user_id: User ID

        Returns:
            Tuple of (response_dict, status_code)
        """
        chunk_file = FileStorage(
            stream=BytesIO(chunk_data),
            filename=file_name,
            content_type='application/octet-stream',
        )

        return handle_chunked_upload(
            project_id=project_id,
            conversation_id=conversation_id,
            file_id=file_id,
            chunk_index=str(chunk_index),
            total_chunks=str(total_chunks),
            file_name=file_name,
            chunk_file=chunk_file,
            overwrite_attachments=overwrite,
            user_id=user_id,
        )

    @web.rpc("chat_upload_attachments_rpc", "upload_attachments_rpc")
    def upload_attachments_rpc(
        self,
        project_id: int,
        conversation_id: int,
        files: List[Dict[str, Any]],
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Upload file attachments to a conversation.

        Args:
            project_id: Project ID
            conversation_id: Conversation ID (integer)
            files: List of dicts with 'filename' and 'data' (bytes)
            overwrite: Whether to overwrite existing files

        Returns:
            Dict with 'attachments' list or 'error' string
        """
        form_files = []
        for file_info in files:
            filename = file_info.get('filename', '')
            data = file_info.get('data', b'')
            file_storage = FileStorage(
                stream=BytesIO(data),
                filename=filename,
                content_type='application/octet-stream',
            )
            form_files.append(file_storage)

        result, status_code = process_uploaded_files(
            project_id=project_id,
            conversation_id=conversation_id,
            form_files=form_files,
            overwrite_attachments=overwrite,
        )

        if status_code >= 400:
            return result

        return {"attachments": result}
