"""
S3-backed icon storage for horizontal scaling.

Replaces local filesystem icon storage with S3-compatible object storage,
enabling stateless pod operation. Uses the existing artifacts RPC for upload/download
and MinioClient for delete/list operations.
"""

import io
import logging
from typing import Optional
from uuid import uuid4

from PIL import Image


log = logging.getLogger(__name__)

ICONS_BUCKET = "icons"
MAX_ICON_SIZE_KB = 512
DEFAULT_ICON_WIDTH = 64
DEFAULT_ICON_HEIGHT = 64
SUPPORTED_FORMATS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.ico'}


class IconStorageError(Exception):
    """Base exception for icon storage operations."""
    pass


class IconValidationError(IconStorageError):
    """Raised when icon file validation fails."""
    pass


class IconNotFoundError(IconStorageError):
    """Raised when an icon cannot be found in storage."""
    pass


class IconStorage:
    """S3-backed icon storage using existing artifacts RPC infrastructure.

    Stores icons in an S3-compatible bucket (MinIO/RustFS) with the structure:
        {bucket_prefix}icons/{project_id}/{uuid}.png

    This replaces local filesystem storage at /data/static/application_icon/
    to enable horizontal scaling (pods don't share a filesystem).
    """

    def __init__(self, rpc_caller, minio_client=None):
        """
        Args:
            rpc_caller: RPC interface with .timeout(N).method() pattern.
                        Typically: rpc_tools.RpcMixin().rpc
            minio_client: MinioClient instance for list/delete operations.
                          If None, list_icons and delete_icon will use RPC fallbacks.
        """
        self._rpc = rpc_caller
        self._minio = minio_client

    def upload_icon(
        self,
        project_id: int,
        icon_data: bytes,
        filename: str,
        width: int = DEFAULT_ICON_WIDTH,
        height: int = DEFAULT_ICON_HEIGHT,
    ) -> dict:
        """Upload and resize an icon to S3 storage.

        Args:
            project_id: Project ID for bucket namespace.
            icon_data: Raw image bytes.
            filename: Original filename (used for extension validation).
            width: Target icon width in pixels.
            height: Target icon height in pixels.

        Returns:
            dict with 'name' (stored filename), 'url' (relative path for retrieval),
            'size' (WxH string), 'initial_file_size' (bytes), 'resulting_file_size' (bytes).

        Raises:
            IconValidationError: If file is too large, empty, wrong format, or too small.
            IconStorageError: If S3 upload fails.
        """
        if not icon_data:
            raise IconValidationError("The file is empty")

        file_size = len(icon_data)
        if file_size > MAX_ICON_SIZE_KB * 1024:
            raise IconValidationError(f"File size exceeds {MAX_ICON_SIZE_KB} KB")

        ext = _get_extension(filename)
        if ext.lower() not in SUPPORTED_FORMATS:
            raise IconValidationError(f"Unsupported image format: {ext}")

        try:
            img = Image.open(io.BytesIO(icon_data))
        except Exception as e:
            raise IconValidationError(f"Cannot open image: {e}") from e

        if img.width < width or img.height < height:
            raise IconValidationError(
                f"Image dimensions ({img.width}x{img.height}) are too small, "
                f"minimum is {width}x{height}"
            )

        img.thumbnail(size=(width, height))

        output = io.BytesIO()
        img.save(output, format="PNG")
        png_data = output.getvalue()

        icon_name = f"{uuid4()}.png"
        s3_filename = f"{project_id}/{icon_name}"

        try:
            self._rpc.timeout(10).artifacts_upload(
                project_id=project_id,
                bucket=ICONS_BUCKET,
                filename=s3_filename,
                file_data=png_data,
                create_if_not_exists=True,
                bucket_retention_days=None,
                check_duplicates=False,
                overwrite=False,
            )
        except Exception as e:
            raise IconStorageError(f"Failed to upload icon to S3: {e}") from e

        return {
            "name": icon_name,
            "url": f"/icons/{project_id}/{icon_name}",
            "size": f"{width}x{height}",
            "initial_file_size": file_size,
            "resulting_file_size": len(png_data),
        }

    def get_icon_url(self, project_id: int, icon_name: str) -> str:
        """Get the URL path for serving an icon.

        Args:
            project_id: Project ID.
            icon_name: Icon filename (e.g. "uuid.png").

        Returns:
            Relative URL path for icon retrieval via the /icons/ route.
        """
        return f"/icons/{project_id}/{icon_name}"

    def get_icon_data(self, project_id: int, icon_name: str) -> bytes:
        """Download icon bytes from S3.

        Args:
            project_id: Project ID.
            icon_name: Icon filename.

        Returns:
            Image bytes.

        Raises:
            IconNotFoundError: If the icon doesn't exist in storage.
            IconStorageError: If S3 retrieval fails.
        """
        s3_filename = f"{project_id}/{icon_name}"
        try:
            result = self._rpc.timeout(10).artifacts_get_file_data(
                project_id=project_id,
                bucket=ICONS_BUCKET,
                filename=s3_filename,
            )
        except Exception as e:
            raise IconStorageError(f"Failed to retrieve icon from S3: {e}") from e

        if result is None:
            raise IconNotFoundError(
                f"Icon not found: {ICONS_BUCKET}/{s3_filename}"
            )

        file_data = result.get("file_data")
        if file_data is None:
            raise IconNotFoundError(
                f"Icon not found: {ICONS_BUCKET}/{s3_filename}"
            )

        return file_data

    def delete_icon(self, project_id: int, icon_name: str) -> bool:
        """Delete an icon from S3 storage.

        Args:
            project_id: Project ID.
            icon_name: Icon filename.

        Returns:
            True if deletion succeeded.

        Raises:
            IconStorageError: If deletion fails or MinioClient not available.
        """
        if self._minio is None:
            raise IconStorageError(
                "MinioClient required for delete operations"
            )

        s3_filename = f"{project_id}/{icon_name}"
        try:
            self._minio.remove_file(ICONS_BUCKET, s3_filename)
        except Exception as e:
            raise IconStorageError(f"Failed to delete icon from S3: {e}") from e

        return True

    def list_icons(
        self, project_id: int, skip: int = 0, limit: int = 200
    ) -> dict:
        """List all icons for a project.

        Args:
            project_id: Project ID.
            skip: Number of results to skip.
            limit: Maximum number of results.

        Returns:
            dict with 'total' (int) and 'rows' (list of {name, url}).
        """
        if self._minio is None:
            log.warning("MinioClient required for list operations")
            return {"total": 0, "rows": []}

        try:
            all_files = self._minio.list_files(ICONS_BUCKET)
        except Exception as e:
            log.warning(f"Failed to list icons for project {project_id}: {e}")
            return {"total": 0, "rows": []}

        prefix = f"{project_id}/"
        rows = []
        for file_info in all_files:
            name = file_info.get("name", "")
            if not name.startswith(prefix):
                continue
            icon_name = name[len(prefix):]
            if not icon_name:
                continue
            rows.append({
                "name": icon_name,
                "url": f"/icons/{project_id}/{icon_name}",
            })

        rows.sort(key=lambda x: x["name"])
        total = len(rows)
        paginated = rows[skip:skip + limit]

        return {"total": total, "rows": paginated}


def _get_extension(filename: str) -> str:
    """Extract file extension from filename."""
    if not filename or "." not in filename:
        return ""
    return "." + filename.rsplit(".", 1)[1]
