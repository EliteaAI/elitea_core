"""Bundled files of a skill version (#7185): path rules, text detection, caps, the manifest rows and their copies.

The rows live in the project schema; the bytes live in the project's ``skill-files`` bucket under their sha256
(``skill_file_store``). Bytes are written before the row that names them, so a failed row insert leaves at most an
orphan object.
"""
import hashlib
import mimetypes

from pylon.core.tools import log
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from tools import serialize

from .skill_file_store import get_content, project_client, put_content
from .skill_utils import (
    SkillFileError,
    SkillFileNotFoundError,
    SkillVersionNotFoundError,
    _ensure_version_updatable,
    _skill_session,
    get_max_file_bytes,
    get_max_package_bytes,
    get_max_package_files,
)
from ..models.skill import SkillVersion, SkillVersionFile
from ..models.pd.skill_file import SkillFileModel, describe_manifest


MAX_PATH_LENGTH = 1024
MAX_PATH_DEPTH = 16
INSTRUCTIONS_FILE_NAME = 'skill.md'


class SkillFileContentUnavailableError(SkillFileError):
    http_status = 503

    def __init__(self, path: str):
        super().__init__(f"The stored content of '{path}' could not be read; try again later")


class SkillFilePathTakenError(SkillFileError):
    http_status = 409

    def __init__(self, path: str, version_id: int):
        super().__init__(f"'{path}' already exists in skill version {version_id}")


def normalize_skill_file_path(path: str) -> str:
    """Return the stored form of a path under the skill root, or raise when it could leave or alias the root."""
    candidate = (path or '').strip()
    if candidate.startswith('./'):
        candidate = candidate[2:]
    parts = candidate.split('/')
    if (
        not candidate
        or '\\' in candidate
        or '\x00' in candidate
        or ':' in parts[0]
        or any(part in ('', '.', '..') for part in parts)
    ):
        raise SkillFileError(
            f"'{path}' is not a valid skill file path: use a relative path under the skill root, "
            "such as references/guide.md"
        )
    if len(candidate) > MAX_PATH_LENGTH:
        raise SkillFileError(f"'{path[:40]}…' is longer than {MAX_PATH_LENGTH} characters")
    if len(parts) > MAX_PATH_DEPTH:
        raise SkillFileError(f"'{path}' is nested deeper than {MAX_PATH_DEPTH} folders")
    if candidate.lower() == INSTRUCTIONS_FILE_NAME:
        raise SkillFileError("SKILL.md is the version's instructions; edit them instead of uploading a file")
    return candidate


def is_text_content(data: bytes) -> bool:
    """Text is what the read tool can hand to a model: UTF-8 without NUL bytes."""
    if b'\x00' in data:
        return False
    try:
        data.decode('utf-8')
    except UnicodeDecodeError:
        return False
    return True


def detect_content_type(path: str, is_text: bool) -> str:
    """Content type from the extension, falling back on the text/binary split."""
    return mimetypes.guess_type(path)[0] or ('text/plain' if is_text else 'application/octet-stream')


def describe_size(size: int) -> str:
    """Human size for cap messages, in MB from 1 MB up and KB below."""
    if size >= 1024 * 1024:
        return f'{size / 1024 / 1024:.1f} MB'
    return f'{size / 1024:.0f} KB'


def validate_package_limits(
    current_sizes: dict,
    path: str,
    size: int,
    *,
    max_file_bytes: int,
    max_package_bytes: int,
    max_package_files: int,
) -> None:
    """Raise when storing ``size`` bytes at ``path`` would push the version over an admin cap."""
    if size > max_file_bytes:
        raise SkillFileError(
            f"'{path}' is {describe_size(size)}; a skill file can be at most {describe_size(max_file_bytes)}"
        )
    other_sizes = [current for current_path, current in current_sizes.items() if current_path != path]
    if len(other_sizes) + 1 > max_package_files:
        raise SkillFileError(f'a skill version can hold at most {max_package_files} files')
    total = sum(other_sizes) + size
    if total > max_package_bytes:
        raise SkillFileError(
            f"the version's files would be {describe_size(total)}; "
            f'a skill version can hold at most {describe_size(max_package_bytes)}'
        )


def validate_package(entries: list) -> None:
    """Raise when a whole file set breaks a cap or names one path twice (paths compare case-insensitively)."""
    caps = {
        'max_file_bytes': get_max_file_bytes(),
        'max_package_bytes': get_max_package_bytes(),
        'max_package_files': get_max_package_files(),
    }
    sizes = {}
    for entry in entries:
        key = entry['path'].lower()
        if key in sizes:
            raise SkillFileError(f"'{entry['path']}' is listed more than once")
        validate_package_limits(sizes, entry['path'], entry['size'], **caps)
        sizes[key] = entry['size']


def build_file_entry(path: str, data: bytes) -> dict:
    """A manifest entry for ``data`` stored at ``path``, with the bytes alongside."""
    stored_path = normalize_skill_file_path(path)
    is_text = is_text_content(data)
    return {
        'path': stored_path,
        'size': len(data),
        'sha256': hashlib.sha256(data).hexdigest(),
        'content_type': detect_content_type(stored_path, is_text),
        'is_text': is_text,
        'data': data,
    }


def _load_version(session, skill_id: int, version_id: int, for_update: bool = False) -> SkillVersion:
    query = session.query(SkillVersion).filter(
        SkillVersion.skill_id == skill_id,
        SkillVersion.id == version_id,
    )
    version = (query.with_for_update() if for_update else query).first()
    if not version:
        raise SkillVersionNotFoundError(skill_id, version_id=version_id)
    return version


def find_version_file(session, version_id: int, path: str):
    """Exact path first, then case-insensitive: skill bodies name their files loosely (FORMS.md vs forms.md)."""
    exact = session.query(SkillVersionFile).filter(
        SkillVersionFile.skill_version_id == version_id,
        SkillVersionFile.path == path,
    ).first()
    if exact:
        return exact
    return session.query(SkillVersionFile).filter(
        SkillVersionFile.skill_version_id == version_id,
        func.lower(SkillVersionFile.path) == path.lower(),
    ).first()


def _flush_file_row(session, version_file: SkillVersionFile) -> None:
    """Write the row now, turning a path another request took first (in any case) into a 409."""
    try:
        session.flush()
    except IntegrityError as exc:
        raise SkillFilePathTakenError(version_file.path, version_file.skill_version_id) from exc


def _manifest_entry(version_file: SkillVersionFile) -> dict:
    return serialize(SkillFileModel.model_validate(version_file))


def list_version_files(project_id: int, skill_id: int, version_id: int, session=None) -> list:
    """The version's manifest: every file with its size, type and hash, never the bytes."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id)
        return [_manifest_entry(version_file) for version_file in version.files]


def read_content(project_id: int, version_file, client=None) -> bytes:
    """The bytes of one manifest row of the project, or a 503 naming the path when storage cannot serve them
    or serves bytes that are not the ones the row names."""
    try:
        data = get_content(project_id, version_file.sha256, client)
    except Exception as exc:
        log.exception('skill files: content %s of project %s unreadable', version_file.sha256, project_id)
        raise SkillFileContentUnavailableError(version_file.path) from exc
    if hashlib.sha256(data).hexdigest() != version_file.sha256:
        log.error('skill files: content %s of project %s does not match its hash', version_file.sha256, project_id)
        raise SkillFileContentUnavailableError(version_file.path)
    return data


def read_version_file(project_id: int, skill_id: int, version_id: int, path: str, session=None) -> tuple:
    """The manifest entry and the bytes of one file."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id)
        version_file = find_version_file(s, version.id, normalize_skill_file_path(path))
        if not version_file:
            raise SkillFileNotFoundError(version_id, path)
        return _manifest_entry(version_file), read_content(project_id, version_file)


def write_version_file(project_id: int, skill_id: int, version_id: int, path: str, data: bytes, session=None) -> dict:
    """Store ``data`` at ``path`` in a draft version, replacing a file already there (case-insensitively).
    Writers of one version queue on its row, so each one checks the caps against the files the previous one left."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id, for_update=True)
        _ensure_version_updatable(version)
        entry = build_file_entry(path, data)
        replaced = find_version_file(s, version.id, entry['path'])
        current_sizes = {
            version_file.path: version_file.size
            for version_file in version.files
            if version_file is not replaced
        }
        validate_package_limits(
            current_sizes, entry['path'], entry['size'],
            max_file_bytes=get_max_file_bytes(),
            max_package_bytes=get_max_package_bytes(),
            max_package_files=get_max_package_files(),
        )
        put_content(project_id, entry['sha256'], data, project_client(project_id))
        version_file = replaced or SkillVersionFile(skill_version_id=version.id)
        for field in ('path', 'size', 'sha256', 'content_type', 'is_text'):
            setattr(version_file, field, entry[field])
        s.add(version_file)
        _flush_file_row(s, version_file)
        return _manifest_entry(version_file)


def rename_version_file(project_id: int, skill_id: int, version_id: int, path: str, new_path: str, session=None) -> dict:
    """Move a file of a draft version to another path under the skill root."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id)
        _ensure_version_updatable(version)
        version_file = find_version_file(s, version.id, normalize_skill_file_path(path))
        if not version_file:
            raise SkillFileNotFoundError(version_id, path)
        stored_path = normalize_skill_file_path(new_path)
        occupant = find_version_file(s, version.id, stored_path)
        if occupant and occupant is not version_file:
            raise SkillFilePathTakenError(occupant.path, version_id)
        version_file.path = stored_path
        _flush_file_row(s, version_file)
        return _manifest_entry(version_file)


def delete_version_file(project_id: int, skill_id: int, version_id: int, path: str, session=None) -> None:
    """Remove a file from a draft version."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id)
        _ensure_version_updatable(version)
        version_file = find_version_file(s, version.id, normalize_skill_file_path(path))
        if not version_file:
            raise SkillFileNotFoundError(version_id, path)
        s.delete(version_file)


def list_version_manifest(session, version_id: int) -> list:
    """The manifest of one version as copies carry it: path, size, hash, type and text flag."""
    return describe_manifest(
        session.query(SkillVersionFile).filter(SkillVersionFile.skill_version_id == version_id)
        .order_by(SkillVersionFile.path).all()
    )
