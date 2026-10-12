"""Chat import: decrypt, validate and stage an exported chat file, then recreate it as a new conversation.

Flow: prepare_import() turns an uploaded encrypted file into a staged, validated payload plus a preview;
commit_import() creates the conversation from the staging with the attachments the user selected.
Nothing from the file is trusted: ids, authors, projects, buckets and file paths are re-created.
"""
import json
import mimetypes
import os
import re
import shutil
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

from pydantic import ValidationError
from pylon.core.tools import log
from tools import MinioClient, VaultClient, rpc_tools

from ..models.all import Application
from ..models.conversation import Conversation
from ..models.enums.all import CanvasTypes, ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.attachment import AttachmentMessageItem
from ..models.message_items.canvas import CanvasMessageItem, CanvasVersionItem
from ..models.message_items.text import TextMessageItem
from ..models.pd.conversation import ConversationCreate
from ..models.pd.conversation_import import (
    ChatExportEnvelope,
    ImportAttachmentItem,
    ImportCanvasItem,
    ImportMessage,
    ImportTextItem,
)
from ..models.pd.participant import ParticipantCreate
from .attachments import parse_filepath
from .chat_crypto import (
    CHAT_ENVELOPE,
    ENVELOPE_SUFFIX,
    EnvelopeIntegrityError,
    EnvelopeKeyMismatch,
    chat_master_key,
    iter_file_chunks,
)
from .chat_constants import CONVERSATION_NAME_MAX_LENGTH
from .conversation_utils import create_conversation
from .file_utils import sanitize_filename
from .internal_tools import get_default_attachment_bucket
from .participant_utils import add_participant_to_conversation

IMPORT_STAGING_DIR = os.path.join(tempfile.gettempdir(), 'elitea_chat_imports')
STAGING_TTL_SECONDS = 3600
SUPPORTED_FORMAT_MAJOR = 1
MAX_MESSAGES = 10000
MAX_JSON_BYTES = 100 * 1024 * 1024
MAX_ZIP_ENTRIES = 1000
MAX_COMPRESSION_RATIO = 200
DEFAULT_IMPORTED_NAME = 'Imported chat'
CONVERSATION_NAME_MIN_LENGTH = 3

_IMPORT_ID_RE = re.compile(r'^[0-9a-f]{32}$')
_EXPORT_SUFFIX_RE = re.compile(r'_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}(-\d{2})?$')
_FILE_EXTENSIONS = ('.json', '.zip')
_ZIP_MAGIC = b'PK\x03\x04'
_CANVAS_TYPES = {t.value for t in CanvasTypes}

MSG_NOT_ENCRYPTED = 'Invalid file: only encrypted chat files exported from ELITEA can be imported.'
MSG_NO_KEY = 'Chat import is not available: encryption key is not configured.'
MSG_OTHER_SETUP = "This chat was exported from another ELITEA setup and can't be imported here."
MSG_TAMPERED = 'Invalid file: the file is corrupted or has been modified.'
MSG_MALFORMED_JSON = 'Invalid file: the JSON content is malformed.'
MSG_BAD_ZIP = 'Invalid file: the ZIP archive is corrupted or cannot be read.'
MSG_ZIP_JSON_COUNT = 'Invalid file: the ZIP must contain exactly one exported chat JSON file.'
MSG_UNSAFE_ZIP = 'Invalid file: the ZIP archive contains unsafe entries.'
MSG_NOT_EXPORT = 'Invalid file: this is not an ELITEA chat export.'
MSG_TOO_LARGE = 'File is too large. Maximum size is {} MB.'
MSG_EXPIRED = 'Import session expired or not found. Please select the file again.'


class ChatImportError(Exception):
    """Validation or import failure with a message that is safe to show to the user."""


# ---------- limits & names ----------

def get_import_limits(project_id: int) -> dict:
    secrets = VaultClient(project_id).get_all_secrets()
    return {
        'total_mb': int(secrets.get('chat_max_upload_size_mb', 150)),
        'file_mb': int(secrets.get('chat_max_file_upload_size_mb', 150)),
        'image_mb': int(secrets.get('chat_max_image_upload_size_mb', 3)),
        'retention_days': int(secrets.get('chat_bucket_retention_days', 365)),
    }


def is_image_name(name: str) -> bool:
    mime_type = mimetypes.guess_type(name or '')[0] or ''
    return mime_type.startswith('image') and not (name or '').lower().endswith('.svg')


def strip_export_suffix(name: str) -> str:
    return _EXPORT_SUFFIX_RE.sub('', (name or '').strip()).strip()


def file_name_to_chat_name(file_name: str) -> str:
    name = PurePosixPath((file_name or '').replace('\\', '/')).name
    if name.lower().endswith(ENVELOPE_SUFFIX):
        name = name[:-len(ENVELOPE_SUFFIX)]
    for extension in _FILE_EXTENSIONS:
        if name.lower().endswith(extension):
            name = name[:-len(extension)]
            break
    return strip_export_suffix(name)


def resolve_import_name(conversation_name: str | None, file_name: str | None) -> str:
    name = strip_export_suffix(conversation_name or '') or file_name_to_chat_name(file_name or '')
    name = name[:CONVERSATION_NAME_MAX_LENGTH].strip()
    if len(name) < CONVERSATION_NAME_MIN_LENGTH:
        return DEFAULT_IMPORTED_NAME
    return name


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------- payload validation ----------

def parse_chat_export(data: dict) -> tuple[ChatExportEnvelope, list[ImportMessage]]:
    if not isinstance(data, dict):
        raise ChatImportError(MSG_NOT_EXPORT)
    try:
        envelope = ChatExportEnvelope.model_validate(data)
    except ValidationError as exc:
        raise ChatImportError(MSG_NOT_EXPORT) from exc

    version = envelope.export_info.format_version
    try:
        major = int(str(version).split('.', 1)[0])
    except ValueError:
        major = None
    if major != SUPPORTED_FORMAT_MAJOR:
        raise ChatImportError(
            f'Unsupported export version {version}. Please export the chat again with the current ELITEA version.'
        )
    if len(envelope.messages) > MAX_MESSAGES:
        raise ChatImportError(f'Invalid file: the chat has more than {MAX_MESSAGES} messages.')

    messages = []
    for index, raw in enumerate(envelope.messages, start=1):
        try:
            messages.append(ImportMessage.model_validate(raw))
        except ValidationError as exc:
            raise ChatImportError(f'Invalid file: message #{index} is corrupted.') from exc
    return envelope, messages


# ---------- zip safety ----------

def _is_unsafe_member(info: zipfile.ZipInfo) -> bool:
    name = info.filename
    if not name or name.startswith('/') or '\\' in name or '\x00' in name:
        return True
    if '..' in PurePosixPath(name).parts or ':' in name.split('/', 1)[0]:
        return True
    is_symlink = (info.external_attr >> 16) & 0o170000 == 0o120000
    return is_symlink


def open_safe_zip(path: Path, max_total_bytes: int) -> tuple[zipfile.ZipFile, zipfile.ZipInfo]:
    """Open a decrypted archive after checking entry count, paths, sizes and compression ratio."""
    try:
        zf = zipfile.ZipFile(path)
    except (zipfile.BadZipFile, OSError) as exc:
        raise ChatImportError(MSG_BAD_ZIP) from exc
    try:
        infos = zf.infolist()
        if len(infos) > MAX_ZIP_ENTRIES:
            raise ChatImportError(f'Invalid file: the ZIP archive contains more than {MAX_ZIP_ENTRIES} files.')
        total = 0
        for info in infos:
            if _is_unsafe_member(info):
                raise ChatImportError(MSG_UNSAFE_ZIP)
            if info.flag_bits & 0x1:
                raise ChatImportError(MSG_BAD_ZIP)
            total += info.file_size
            if info.file_size > 1024 * 1024 and info.file_size > max(info.compress_size, 1) * MAX_COMPRESSION_RATIO:
                raise ChatImportError(MSG_UNSAFE_ZIP)
        if total > max_total_bytes:
            raise ChatImportError(MSG_TOO_LARGE.format(max_total_bytes // (1024 * 1024)))
        json_entries = [
            info for info in infos
            if not info.is_dir() and '/' not in info.filename and info.filename.lower().endswith('.json')
        ]
        if len(json_entries) != 1:
            raise ChatImportError(MSG_ZIP_JSON_COUNT)
        return zf, json_entries[0]
    except Exception:
        zf.close()
        raise


def read_zip_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, limit: int) -> bytes:
    try:
        with zf.open(info) as member:
            data = member.read(limit + 1)
    except (zipfile.BadZipFile, OSError, RuntimeError) as exc:
        raise ChatImportError(MSG_BAD_ZIP) from exc
    if len(data) > limit:
        raise ChatImportError(MSG_UNSAFE_ZIP)
    return data


def _load_json(data: bytes) -> dict:
    try:
        return json.loads(data.decode('utf-8-sig'))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ChatImportError(MSG_MALFORMED_JSON) from exc


# ---------- staging ----------

def _staging_dir(import_id: str) -> Path:
    if not _IMPORT_ID_RE.match(import_id or ''):
        raise ChatImportError(MSG_EXPIRED)
    return Path(IMPORT_STAGING_DIR) / import_id


def new_staging() -> tuple[str, Path]:
    prune_stale_stagings()
    import_id = uuid.uuid4().hex
    path = Path(IMPORT_STAGING_DIR) / import_id
    path.mkdir(parents=True, exist_ok=False)
    return import_id, path


def discard_staging(import_id: str) -> None:
    try:
        shutil.rmtree(_staging_dir(import_id), ignore_errors=True)
    except ChatImportError:
        pass


def prune_stale_stagings() -> None:
    base = Path(IMPORT_STAGING_DIR)
    if not base.exists():
        return
    now = time.time()
    for entry in base.iterdir():
        try:
            if entry.is_dir() and now - entry.stat().st_mtime > STAGING_TTL_SECONDS:
                shutil.rmtree(entry, ignore_errors=True)
        except OSError as exc:
            log.warning("Chat import: failed to prune staging %s: %s", entry.name, exc)


def _load_staging_meta(import_id: str, project_id: int, user_id: int) -> tuple[Path, dict]:
    path = _staging_dir(import_id)
    try:
        meta = json.loads((path / 'meta.json').read_text('utf-8'))
    except (OSError, ValueError) as exc:
        raise ChatImportError(MSG_EXPIRED) from exc
    if meta.get('project_id') != project_id or meta.get('user_id') != user_id:
        raise ChatImportError(MSG_EXPIRED)
    return path, meta


def discard_user_staging(import_id: str, project_id: int, user_id: int) -> None:
    _load_staging_meta(import_id, project_id, user_id)
    discard_staging(import_id)


# ---------- prepare ----------

def _decrypt_upload(encrypted_path: Path, target: Path) -> None:
    master_key = chat_master_key()
    with open(encrypted_path, 'rb') as source:
        head = source.read(len(CHAT_ENVELOPE.magic))
        if not CHAT_ENVELOPE.is_encrypted(head):
            raise ChatImportError(MSG_NOT_ENCRYPTED)
        if master_key is None:
            raise ChatImportError(MSG_NO_KEY)
        try:
            with open(target, 'wb') as output:
                for chunk in CHAT_ENVELOPE.iter_decrypt(iter_file_chunks(source), master_key):
                    output.write(chunk)
        except EnvelopeKeyMismatch as exc:
            raise ChatImportError(MSG_OTHER_SETUP) from exc
        except EnvelopeIntegrityError as exc:
            raise ChatImportError(MSG_TAMPERED) from exc


def _load_staged_payload(staging: Path, meta: dict, limits: dict):
    """Returns (envelope, messages, zip_file_or_None)."""
    decrypted = staging / 'decrypted.bin'
    if meta['kind'] == 'zip':
        zf, json_info = open_safe_zip(decrypted, limits['total_mb'] * 1024 * 1024)
        try:
            data = _load_json(read_zip_member(zf, json_info, MAX_JSON_BYTES))
            envelope, messages = parse_chat_export(data)
        except Exception:
            zf.close()
            raise
        return envelope, messages, zf
    if decrypted.stat().st_size > MAX_JSON_BYTES:
        raise ChatImportError(MSG_TOO_LARGE.format(MAX_JSON_BYTES // (1024 * 1024)))
    envelope, messages = parse_chat_export(_load_json(decrypted.read_bytes()))
    return envelope, messages, None


def _iter_attachment_items(messages: list[ImportMessage]):
    for message in messages:
        for order_index, item in enumerate(message.items):
            if isinstance(item, ImportAttachmentItem):
                yield message, order_index, item


def _zip_member_sizes(zf: zipfile.ZipFile | None) -> dict[str, int]:
    if zf is None:
        return {}
    return {info.filename: info.file_size for info in zf.infolist() if not info.is_dir()}


def _attachment_preview(messages, zf, limits) -> list[dict]:
    members = _zip_member_sizes(zf)
    seen = {}
    for message, _, item in _iter_attachment_items(messages):
        key = item.export_path or f'missing:{item.name}'
        if key in seen:
            continue
        size = members.get(item.export_path) if item.export_path else None
        available = size is not None and item.export_status != 'missing'
        max_mb = limits['image_mb'] if is_image_name(item.name) else limits['file_mb']
        too_large = bool(available and size > max_mb * 1024 * 1024)
        seen[key] = {
            'export_path': item.export_path if available else None,
            'name': item.name,
            'size': size if size is not None else item.file_size,
            'kind': 'generated' if item.type == 'generated_file' else 'uploaded',
            'available': available,
            'too_large': too_large,
            'max_size_mb': max_mb,
        }
    return list(seen.values())


def prepare_import(project_id: int, user_id: int, encrypted_path: Path, file_name: str) -> dict:
    limits = get_import_limits(project_id)
    import_id, staging = new_staging()
    try:
        decrypted = staging / 'decrypted.bin'
        _decrypt_upload(encrypted_path, decrypted)
        with open(decrypted, 'rb') as f:
            kind = 'zip' if f.read(len(_ZIP_MAGIC)) == _ZIP_MAGIC else 'json'
        meta = {
            'project_id': project_id,
            'user_id': user_id,
            'file_name': file_name,
            'kind': kind,
            'created_at': time.time(),
        }
        envelope, messages, zf = _load_staged_payload(staging, meta, limits)
        try:
            attachments = _attachment_preview(messages, zf, limits)
        finally:
            if zf is not None:
                zf.close()
        (staging / 'meta.json').write_text(json.dumps(meta), 'utf-8')
    except Exception:
        discard_staging(import_id)
        raise

    return {
        'import_id': import_id,
        'name': resolve_import_name(envelope.conversation.name, file_name),
        'messages_count': len(messages),
        'exported_at': envelope.export_info.exported_at,
        'include_attachments': kind == 'zip',
        'attachments_referenced': envelope.export_info.include_attachments,
        'attachments': attachments,
    }


# ---------- commit ----------

def _find_application_id_by_name(session, name: str) -> int | None:
    row = session.query(Application.id).filter(Application.name == name).order_by(Application.id.asc()).first()
    return row[0] if row else None


def _participant_key(participant) -> tuple:
    entity_meta = participant.entity_meta or {}
    return participant.entity_name, entity_meta.get('id')


def _link_ai_participants(session, project_id: int, user_id: int, conversation, envelope) -> dict[int, int]:
    """Re-add agents that exist in this project by exact name. Returns {original participant id: new participant id}."""
    wanted = {}
    for participant in envelope.conversation.participants:
        if participant.entity_name != ParticipantTypes.application.value or not participant.name or participant.id is None:
            continue
        app_id = _find_application_id_by_name(session, participant.name)
        if app_id is None:
            continue
        wanted.setdefault(app_id, []).append(participant.id)

    for app_id in wanted:
        try:
            add_participant_to_conversation(
                project_id=project_id,
                session=session,
                participant=ParticipantCreate(
                    entity_name=ParticipantTypes.application,
                    entity_meta={'id': app_id, 'project_id': project_id},
                ),
                conversation=conversation,
                initiator_id=user_id,
            )
            session.flush()
        except Exception as exc:  # pylint: disable=W0703
            log.warning("Chat import: could not add agent %s to conversation %s: %s", app_id, conversation.id, exc)

    session.refresh(conversation)
    by_key = {_participant_key(p): p.id for p in conversation.participants}
    mapping = {}
    for app_id, original_ids in wanted.items():
        new_id = by_key.get((ParticipantTypes.application.value, app_id))
        if new_id:
            for original_id in original_ids:
                mapping[original_id] = new_id
    return mapping


def _create_text(group, order_index: int, content: str, meta: dict | None = None):
    return TextMessageItem(
        message_group=group,
        item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
        content=content,
        order_index=order_index,
        meta=meta or {},
    )


def _create_canvas(group, order_index: int, item: ImportCanvasItem):
    canvas_type = item.canvas_type if item.canvas_type in _CANVAS_TYPES else CanvasTypes.CODE.value
    return CanvasMessageItem(
        name=(item.name or 'canvas').strip() or 'canvas',
        canvas_type=canvas_type,
        meta={},
        message_group=group,
        order_index=order_index,
        versions=[CanvasVersionItem(
            code_language=(item.code_language or None) and item.code_language[:32],
            canvas_content=item.content or '',
        )],
    )


class _AttachmentUploader:
    """Uploads selected archive members once per export_path into the new conversation folder."""

    def __init__(self, project_id: int, conversation_uuid, zf, selected: set[str], limits: dict):
        self.project_id = project_id
        self.conversation_uuid = conversation_uuid
        self.zf = zf
        self.selected = selected
        self.limits = limits
        self.bucket = get_default_attachment_bucket(project_id)
        self.members = {info.filename: info for info in zf.infolist()} if zf is not None else {}
        self.uploaded: dict[str, tuple[str, str]] = {}
        self.failed: dict[str, str] = {}
        self.names: list[str] = []

    def upload(self, item: ImportAttachmentItem) -> tuple[str, str] | None:
        path = item.export_path
        if not path or path not in self.selected or item.export_status == 'missing':
            return None
        if path in self.uploaded:
            return self.uploaded[path]
        if path in self.failed:
            return None
        info = self.members.get(path)
        if info is None:
            return None
        max_mb = self.limits['image_mb'] if is_image_name(item.name) else self.limits['file_mb']
        try:
            if info.file_size > max_mb * 1024 * 1024:
                raise ChatImportError(f'exceeds the {max_mb} MB limit')
            data = read_zip_member(self.zf, info, max_mb * 1024 * 1024)
            file_name, _ = sanitize_filename(item.name, self.names)
            result = rpc_tools.RpcMixin().rpc.timeout(60).artifacts_upload(
                project_id=self.project_id,
                bucket=self.bucket,
                filename=f"{self.conversation_uuid}/{file_name}",
                file_data=data,
                create_if_not_exists=True,
                bucket_retention_days=self.limits['retention_days'],
                check_duplicates=True,
                overwrite=False,
            )
            self.names.append(file_name)
            self.uploaded[path] = parse_filepath(result['filepath'])
            return self.uploaded[path]
        except Exception as exc:  # pylint: disable=W0703
            log.warning("Chat import: failed to upload %s: %s", path, exc)
            self.failed[path] = item.name
            return None

    def cleanup(self) -> None:
        if not self.uploaded:
            return
        try:
            mc = MinioClient.from_project_id(self.project_id)
            for bucket, name in self.uploaded.values():
                mc.remove_file(bucket, name)
        except Exception as exc:  # pylint: disable=W0703
            log.warning("Chat import: failed to remove uploaded files: %s", exc)


def _create_attachment(group, order_index: int, item: ImportAttachmentItem, uploader: _AttachmentUploader):
    stored = uploader.upload(item)
    if stored is None:
        return _create_text(
            group, order_index, f'📎 {item.name} (not imported)',
            meta={'import_placeholder': True, 'original_name': item.name},
        )
    bucket, name = stored
    return AttachmentMessageItem(
        message_group=group,
        item_type=AttachmentMessageItem.__mapper_args__['polymorphic_identity'],
        name=name,
        bucket=bucket,
        attachment_type=item.attachment_type or ('image' if is_image_name(item.name) else 'document'),
        content=[],
        order_index=order_index,
        meta={},
    )


def _import_messages(session, conversation, messages, participants: dict, uploader) -> int:
    user_id, dummy_id, ai_map = participants['user'], participants['dummy'], participants['ai']

    def resolve(ref):
        if ref is None:
            return None
        if ref.entity_name == ParticipantTypes.user.value:
            return user_id
        return ai_map.get(ref.participant_id, dummy_id)

    fallback_time = _utcnow() - timedelta(seconds=len(messages))
    groups_by_uuid = {}
    replies = []
    last_user_group = None
    previous_time = None

    for index, message in enumerate(messages):
        created_at = _parse_datetime(message.created_at) or (fallback_time + timedelta(seconds=index))
        if previous_time and created_at < previous_time:
            created_at = previous_time + timedelta(microseconds=1)
        previous_time = created_at

        author_id = resolve(message.author) or dummy_id
        group_meta = {}
        if message.author is not None and message.author.name:
            group_meta['imported_author'] = {'entity_name': message.author.entity_name, 'name': message.author.name}
        group = ConversationMessageGroup(
            conversation=conversation,
            author_participant_id=author_id,
            sent_to_id=resolve(message.sent_to),
            meta=group_meta,
            is_streaming=False,
            created_at=created_at,
        )
        session.add(group)

        for order_index, item in enumerate(message.items):
            if isinstance(item, ImportTextItem):
                session.add(_create_text(group, order_index, item.content or ''))
            elif isinstance(item, ImportCanvasItem):
                session.add(_create_canvas(group, order_index, item))
            else:
                session.add(_create_attachment(group, order_index, item, uploader))

        is_user = author_id == user_id
        if message.uuid:
            groups_by_uuid[message.uuid] = group
        replies.append((group, message.reply_to_uuid, None if is_user else last_user_group))
        if is_user:
            last_user_group = group

    session.flush()
    for group, reply_uuid, fallback in replies:
        target = groups_by_uuid.get(reply_uuid) if reply_uuid else None
        target = target if target is not None and target is not group else fallback
        if target is not None:
            group.reply_to_id = target.id
    session.flush()
    return len(replies)


def _delete_conversation(project_id: int, conversation_id: int | None) -> None:
    from tools import db  # pylint: disable=C0415
    if conversation_id is None:
        return
    try:
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(Conversation.id == conversation_id).first()
            if conversation is not None:
                session.delete(conversation)
                session.commit()
    except Exception:  # pylint: disable=W0703
        log.exception("Chat import: failed to remove partially imported conversation %s", conversation_id)


def commit_import(session, project_id: int, user_id: int, import_id: str, selected_attachments: list[str]):
    """Create the conversation from a staging. Returns (conversation_id, context_strategy, failed_names)."""
    staging, meta = _load_staging_meta(import_id, project_id, user_id)
    limits = get_import_limits(project_id)
    envelope, messages, zf = _load_staged_payload(staging, meta, limits)

    conversation_id = None
    uploader = None
    try:
        parsed = ConversationCreate(
            name=resolve_import_name(envelope.conversation.name, meta.get('file_name')),
            is_private=True,
            author_id=user_id,
            source='elitea',
            instructions=envelope.conversation.instructions or '',
            meta={'imported': {
                'from_file': meta.get('file_name'),
                'original_uuid': envelope.conversation.uuid,
                'original_project_id': envelope.export_info.project_id,
                'exported_at': envelope.export_info.exported_at,
                'imported_at': datetime.now(timezone.utc).isoformat(),
                'format_version': envelope.export_info.format_version,
            }},
        )
        conversation, context_strategy = create_conversation(session, project_id, user_id, parsed)
        conversation_id = conversation.id
        session.refresh(conversation)

        by_type = {}
        for participant in conversation.participants:
            if participant.entity_name == ParticipantTypes.user.value \
                    and (participant.entity_meta or {}).get('id') == user_id:
                by_type['user'] = participant.id
            elif participant.entity_name == ParticipantTypes.dummy.value:
                by_type['dummy'] = participant.id
        by_type['ai'] = _link_ai_participants(session, project_id, user_id, conversation, envelope)

        uploader = _AttachmentUploader(project_id, conversation.uuid, zf, set(selected_attachments or []), limits)
        messages_count = _import_messages(session, conversation, messages, by_type, uploader)
        conversation.updated_at = _utcnow()
        session.commit()
    except Exception as exc:
        session.rollback()
        if uploader is not None:
            uploader.cleanup()
        _delete_conversation(project_id, conversation_id)
        if isinstance(exc, ChatImportError):
            reason = str(exc)
        else:
            log.exception("Chat import failed for project %s", project_id)
            reason = 'unexpected server error'
        raise ChatImportError(f'Import failed: {reason}. Nothing was imported.') from exc
    finally:
        if zf is not None:
            zf.close()
        discard_staging(import_id)

    failed = list(uploader.failed.values())
    log.info(
        "chat_imported: project=%s conversation=%s file=%s messages=%s attachments=%s failed=%s",
        project_id, conversation_id, meta.get('file_name'), messages_count, len(uploader.uploaded), len(failed),
    )
    return conversation_id, context_strategy, failed
