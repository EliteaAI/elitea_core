"""Content of skill version files in object storage (#7185, option A): one ``skill-files`` bucket per project.

The only module that touches storage for skill files. Objects are keyed by the sha256 of their bytes, so a project
holds each distinct content once however many versions list it; the manifest rows carry path and content type.
Server-side clients only: callers have already checked the user's access to the version.
"""
from pylon.core.tools import log
from tools import MinioClient

SKILL_FILES_BUCKET = 'skill-files'


def _ensure_bucket(client) -> None:
    if SKILL_FILES_BUCKET not in client.list_bucket():
        client.create_bucket(SKILL_FILES_BUCKET, bucket_type='system')


def _has_object(client, sha256: str) -> bool:
    return client.is_file_exist(client.format_bucket_name(SKILL_FILES_BUCKET), sha256)


def ensure_bucket(project_id: int) -> None:
    """Create the project's skill-files bucket when it does not exist yet, without lifecycle expiry."""
    _ensure_bucket(MinioClient.from_project_id(project_id))


def put_contents(project_id: int, contents: dict) -> None:
    """Store each ``{sha256: bytes}`` the project's bucket does not hold yet."""
    if not contents:
        return
    client = MinioClient.from_project_id(project_id)
    _ensure_bucket(client)
    for sha256, data in contents.items():
        if not _has_object(client, sha256):
            client.upload_file(SKILL_FILES_BUCKET, data, sha256)


def put_content(project_id: int, sha256: str, data: bytes) -> None:
    """Store one content in the project's bucket unless it is already there."""
    put_contents(project_id, {sha256: data})


def get_content(project_id: int, sha256: str) -> bytes:
    """The bytes stored under ``sha256`` in the project's bucket."""
    return MinioClient.from_project_id(project_id).download_file(SKILL_FILES_BUCKET, sha256)


def content_exists(project_id: int, sha256: str) -> bool:
    """Whether the project's bucket holds ``sha256``."""
    client = MinioClient.from_project_id(project_id)
    return SKILL_FILES_BUCKET in client.list_bucket() and _has_object(client, sha256)


def delete_contents(project_id: int, hashes) -> None:
    """Remove each of ``hashes`` from the project's bucket."""
    client = MinioClient.from_project_id(project_id)
    for sha256 in hashes:
        client.remove_file(SKILL_FILES_BUCKET, sha256)


def delete_content(project_id: int, sha256: str) -> None:
    """Remove ``sha256`` from the project's bucket."""
    delete_contents(project_id, [sha256])


def copy_contents(source_project_id: int, target_project_id: int, hashes) -> list:
    """Copy into the target project's bucket each hash it lacks, reading from the source project's bucket."""
    wanted = list(dict.fromkeys(hashes))
    if not wanted or source_project_id == target_project_id:
        return []
    source = MinioClient.from_project_id(source_project_id)
    target = MinioClient.from_project_id(target_project_id)
    _ensure_bucket(target)
    copied = []
    for sha256 in wanted:
        if _has_object(target, sha256):
            continue
        target.upload_file(SKILL_FILES_BUCKET, source.download_file(SKILL_FILES_BUCKET, sha256), sha256)
        log.info('skill files: copied %s from project %s to project %s', sha256, source_project_id, target_project_id)
        copied.append(sha256)
    return copied


def list_contents(project_id: int) -> list:
    """Every object of the project's bucket as ``{name, size, modified}``; empty when the bucket does not exist."""
    client = MinioClient.from_project_id(project_id)
    if SKILL_FILES_BUCKET not in client.list_bucket():
        return []
    return client.list_files(SKILL_FILES_BUCKET)
