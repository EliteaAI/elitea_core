"""Bundled files of a skill version (#7185): path rules, text detection, caps, the manifest rows and their copies.

The rows live in the project schema; the bytes live in the project's ``skill-files`` bucket under their sha256
(``skill_file_store``). Bytes are written before the row that names them, so a failed row insert leaves at most an
orphan object for ``sweep_skill_files`` to reclaim.
"""
import base64
import binascii
import hashlib
import mimetypes
from datetime import datetime, timedelta, timezone

from pylon.core.tools import log
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from tools import auth, config as c, db, serialize

from .folder_access import resolve_entity_access
from .skill_file_store import (
    content_exists, delete_contents, get_content, list_contents, project_client, put_content, put_contents,
)
from .skill_utils import (
    SkillFileError,
    SkillFileNotFoundError,
    SkillVersionNotFoundError,
    _ensure_version_updatable,
    _skill_session,
    claim_skill_file_deletes,
    get_max_file_bytes,
    get_max_package_bytes,
    get_max_package_files,
    hold_skill_file_deletes,
    release_unreferenced_contents,
)
from .utils import get_public_project_id
from ..models.skill import SkillVersion, SkillVersionFile
from ..models.pd.skill_file import SkillFileModel, describe_manifest


MAX_PATH_LENGTH = 1024
MAX_PATH_DEPTH = 16
INSTRUCTIONS_FILE_NAME = 'skill.md'
SKILL_READ_PERMISSION = 'models.applications.skills.details'
SWEEP_MIN_AGE = timedelta(hours=1)


class SkillFileContentUnavailableError(SkillFileError):
    http_status = 503

    def __init__(self, path: str):
        super().__init__(f"The stored content of '{path}' could not be read; try again later")


class SkillFileSourceUnavailableError(SkillFileError):
    def __init__(self, path: str):
        super().__init__(
            f"'{path}' could not be copied: its content is no longer stored where it is copied from "
            "(deleted, unpublished or replaced meanwhile); copy the skill again"
        )


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
        hold_skill_file_deletes(s, project_id)
        put_content(project_id, entry['sha256'], data, project_client(project_id))
        replaced_hash = replaced.sha256 if replaced else None
        version_file = replaced or SkillVersionFile(skill_version_id=version.id)
        for field in ('path', 'size', 'sha256', 'content_type', 'is_text'):
            setattr(version_file, field, entry[field])
        s.add(version_file)
        _flush_file_row(s, version_file)
        result = _manifest_entry(version_file)
    release_unreferenced_contents(project_id, {replaced_hash} - {entry['sha256'], None})
    return result


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
    """Remove a file from a draft version and release its content when nothing else in the project uses it."""
    with _skill_session(session, project_id) as s:
        version = _load_version(s, skill_id, version_id)
        _ensure_version_updatable(version)
        version_file = find_version_file(s, version.id, normalize_skill_file_path(path))
        if not version_file:
            raise SkillFileNotFoundError(version_id, path)
        released_hash = version_file.sha256
        s.delete(version_file)
    release_unreferenced_contents(project_id, {released_hash})


def _unique_by_hash(entries: list) -> list:
    return list({entry['sha256']: entry for entry in entries}.values())


def _fetch_from_source(source_project_id, project_id: int, entries: list) -> dict:
    """The verified bytes of each entry, read from the source project's bucket."""
    if source_project_id is None or source_project_id == project_id:
        raise SkillFileSourceUnavailableError(entries[0]['path'])
    source_client = project_client(source_project_id)
    fetched = {}
    for entry in _unique_by_hash(entries):
        try:
            data = get_content(source_project_id, entry['sha256'], source_client)
        except Exception as exc:
            log.exception('skill files: content %s of project %s unreadable for a copy', entry['sha256'],
                          source_project_id)
            raise SkillFileSourceUnavailableError(entry['path']) from exc
        if hashlib.sha256(data).hexdigest() != entry['sha256']:
            log.error('skill files: content %s of project %s does not match its hash', entry['sha256'],
                      source_project_id)
            raise SkillFileSourceUnavailableError(entry['path'])
        fetched[entry['sha256']] = data
    return fetched


def stage_version_files(project_id: int, entries: list, source_project_id: int = None) -> dict:
    """Store in the project's bucket, before any transaction adds rows naming them, the bytes the entries carry and
    those the project lacks of the rest, read and verified from ``source_project_id``; returns the bytes stored, by
    hash, for ``add_version_files`` to store again should a delete remove one meanwhile."""
    if not entries:
        return {}
    contents = {entry['sha256']: entry['data'] for entry in entries if 'data' in entry}
    client = project_client(project_id)
    lacking = [
        entry for entry in _unique_by_hash(entries)
        if 'data' not in entry and not content_exists(project_id, entry['sha256'], client)
    ]
    if lacking:
        contents.update(_fetch_from_source(source_project_id, project_id, lacking))
    put_contents(project_id, contents, client)
    return contents


def _restore_missing_objects(project_id: int, entries: list, contents: dict, source_project_id) -> None:
    if not entries:
        return
    client = project_client(project_id)
    missing = [entry for entry in _unique_by_hash(entries) if not content_exists(project_id, entry['sha256'], client)]
    if not missing:
        return
    log.warning('skill files: %s object(s) of project %s went missing before their rows; storing them again',
                len(missing), project_id)
    restored = {entry['sha256']: contents[entry['sha256']] for entry in missing if entry['sha256'] in contents}
    unstaged = [entry for entry in missing if entry['sha256'] not in contents]
    if unstaged:
        restored.update(_fetch_from_source(source_project_id, project_id, unstaged))
    put_contents(project_id, restored, client)


def add_version_files(
    session, project_id: int, version_id: int, entries: list, contents: dict = None, source_project_id: int = None,
) -> None:
    """Give a version of ``project_id`` the entries as file rows. The objects are ``stage_version_files``'s work,
    done before this transaction; here, with the project's deletes held off, each one is checked to still be there
    (stored again from ``contents`` or the source if a delete removed it) and the rows are added."""
    hold_skill_file_deletes(session, project_id)
    carried = {entry['sha256']: entry['data'] for entry in entries if 'data' in entry}
    _restore_missing_objects(project_id, entries, {**carried, **(contents or {})}, source_project_id)
    for entry in entries:
        session.add(SkillVersionFile(
            skill_version_id=version_id,
            path=entry['path'],
            size=entry['size'],
            sha256=entry['sha256'],
            content_type=entry['content_type'],
            is_text=entry['is_text'],
        ))
    session.flush()


def list_version_manifest(session, version_id: int) -> list:
    """The manifest of one version as copies carry it: path, size, hash, type and text flag."""
    return describe_manifest(
        session.query(SkillVersionFile).filter(SkillVersionFile.skill_version_id == version_id)
        .order_by(SkillVersionFile.path).all()
    )


def read_version_contents(project_id: int, version_id: int, session=None) -> list:
    """Every file of a version as ``(manifest entry, bytes)``, for exports."""
    with _skill_session(session, project_id) as s:
        version_files = s.query(SkillVersionFile).filter(
            SkillVersionFile.skill_version_id == version_id,
        ).order_by(SkillVersionFile.path).all()
        if not version_files:
            return []
        client = project_client(project_id)
        return [
            (describe_manifest([version_file])[0], read_content(project_id, version_file, client))
            for version_file in version_files
        ]


def _decode_payload_file(payload_file: dict) -> dict:
    try:
        data = base64.b64decode(payload_file['content_base64'], validate=True)
    except (binascii.Error, TypeError, ValueError) as exc:
        raise SkillFileError(f"'{payload_file.get('path')}' does not carry valid base64 content") from exc
    return build_file_entry(payload_file.get('path'), data)


def _not_a_source_file(payload_file: dict, source_project_id, source_version_id) -> SkillFileError:
    return SkillFileError(
        f"'{payload_file.get('path')}' is not a file of skill version {source_version_id} in project {source_project_id}"
    )


def _actor_holds_skill_read(actor_id, source_project_id) -> bool:
    if actor_id is None:
        return False
    try:
        permissions = auth.get_user_permissions(actor_id, mode=c.DEFAULT_MODE, project_id=source_project_id)
    except Exception:  # pylint: disable=W0703
        log.exception('skill files: permissions of user %s in project %s unreadable', actor_id, source_project_id)
        return False
    return SKILL_READ_PERMISSION in (permissions or ())


def _actor_may_read_source_version(source_session, actor_id, source_project_id, source_version_id) -> bool:
    """Whether the acting user could read the source version's files through the file endpoint: the skill read
    permission in the source project and folder access to its skill. The public catalog is readable by everyone."""
    if source_project_id == get_public_project_id():
        return True
    if not _actor_holds_skill_read(actor_id, source_project_id):
        return False
    version = source_session.query(SkillVersion.skill_id).filter(SkillVersion.id == source_version_id).first()
    if version is None:
        return False
    return resolve_entity_access(source_project_id, 'skill', version.skill_id, actor_id) != 'no_access'


def _find_source_files(source_project_id, source_version_id, payload_files: list, actor_id=None) -> list:
    """The source version's rows matching each listed ``(path, sha256)``, for an acting user who may read them. A
    pair the source lacks and a source the user may not read are refused alike, so neither tells what exists."""
    if source_project_id is None or source_version_id is None:
        raise SkillFileError('the skill lists files without their content or the version they come from')
    with db.get_session(source_project_id) as source_session:
        if not _actor_may_read_source_version(source_session, actor_id, source_project_id, source_version_id):
            raise _not_a_source_file(payload_files[0], source_project_id, source_version_id)
        held = {
            (row['path'], row['sha256']): row
            for row in list_version_manifest(source_session, source_version_id)
        }
    entries = []
    for payload_file in payload_files:
        row = held.get((payload_file.get('path'), payload_file.get('sha256')))
        if row is None:
            raise _not_a_source_file(payload_file, source_project_id, source_version_id)
        entries.append(row)
    return entries


def resolve_payload_files(payload_files: list, source_project_id=None, source_version_id=None, actor_id=None) -> list:
    """Entries for the files a skill payload lists: decoded bytes when it carries them, checked against the caps,
    else the source version's own rows, which copy as they are so a lowered cap never breaks an existing skill.
    Rows of the source are given only to an acting user who may read them there."""
    carried = [payload_file for payload_file in payload_files if payload_file.get('content_base64') is not None]
    referenced = [payload_file for payload_file in payload_files if payload_file.get('content_base64') is None]
    entries = [_decode_payload_file(payload_file) for payload_file in carried]
    validate_package(entries)
    if referenced:
        entries.extend(_find_source_files(source_project_id, source_version_id, referenced, actor_id))
    return entries


def stage_import_files(project_id: int, versions: list, source_project_id=None, actor_id=None) -> dict:
    """Resolve the files each payload version lists, as ``actor_id`` may read them, and store their objects, before
    the import's transaction: ``{version name: (entries, bytes stored by hash)}``."""
    staged = {}
    for version in versions:
        payload_files = version.get('files') or []
        if not payload_files:
            continue
        entries = resolve_payload_files(payload_files, source_project_id, version.get('id'), actor_id)
        staged[version.get('name', 'base')] = (entries, stage_version_files(project_id, entries, source_project_id))
    return staged


def import_version_files(session, project_id: int, version_ids: dict, staged: dict, source_project_id=None) -> int:
    """Give the versions an import just created the files ``stage_import_files`` staged for them; returns how many
    rows were added."""
    added = 0
    for version_name, (entries, contents) in staged.items():
        version_id = version_ids.get(version_name)
        if version_id is None:
            continue
        add_version_files(session, project_id, version_id, entries, contents, source_project_id)
        added += len(entries)
    return added


def sweep_skill_files(project_id: int, dry_run: bool = False, now: datetime = None) -> dict:
    """Delete the objects of the project's bucket that no file row references and that are older than an hour,
    so an upload whose row is still being committed survives; refused while a transaction is adding file rows.
    An object storage reports no time for is kept and counted, since its age cannot be told."""
    cutoff = (now or datetime.now(timezone.utc)) - SWEEP_MIN_AGE
    with db.get_session(project_id) as s:
        if not claim_skill_file_deletes(s, project_id):
            raise SkillFileError(f'skill files of project {project_id} are being written; run the sweep again')
        referenced = {sha256 for (sha256,) in s.query(SkillVersionFile.sha256).distinct().all()}
        unreferenced = [obj for obj in list_contents(project_id) if obj['name'] not in referenced]
        undated = [obj['name'] for obj in unreferenced if not obj.get('modified')]
        stale = [
            obj['name'] for obj in unreferenced
            if obj.get('modified') and datetime.fromisoformat(obj['modified']).astimezone(timezone.utc) < cutoff
        ]
        if stale and not dry_run:
            delete_contents(project_id, stale)
    if undated:
        log.warning('skill files: %s unreferenced object(s) of project %s have no time; kept', len(undated), project_id)
    return {'project_id': project_id, 'deleted': stale, 'kept_recent': len(unreferenced) - len(stale) - len(undated),
            'kept_undated': len(undated), 'dry_run': dry_run}
