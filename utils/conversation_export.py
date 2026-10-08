import json
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath

from pylon.core.tools import log
from tools import MinioClient, this

from ..models.enums.all import ParticipantTypes
from ..models.folder import ConversationFolder
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.base import MessageItem
from .authors import get_authors_data

EXPORT_FORMAT_VERSION = '1.0'
ATTACHMENTS_DIR = 'attachments'
CANVASES_DIR = 'canvases'
_ZIP_SPOOL_MAX_SIZE = 50 * 1024 * 1024

_UNSAFE_NAME_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]')

_CODE_LANGUAGE_EXTENSIONS = {
    'python': 'py', 'javascript': 'js', 'typescript': 'ts', 'jsx': 'jsx', 'tsx': 'tsx',
    'java': 'java', 'kotlin': 'kt', 'go': 'go', 'rust': 'rs', 'ruby': 'rb', 'php': 'php',
    'c': 'c', 'cpp': 'cpp', 'csharp': 'cs', 'swift': 'swift', 'scala': 'scala',
    'html': 'html', 'css': 'css', 'scss': 'scss', 'json': 'json', 'yaml': 'yaml', 'yml': 'yml',
    'xml': 'xml', 'sql': 'sql', 'bash': 'sh', 'shell': 'sh', 'sh': 'sh', 'powershell': 'ps1',
    'markdown': 'md', 'md': 'md', 'text': 'txt', 'plaintext': 'txt', 'mermaid': 'mmd',
    'dockerfile': 'dockerfile', 'groovy': 'groovy', 'r': 'r', 'lua': 'lua', 'perl': 'pl',
}


def sanitize_export_name(name: str, fallback: str = 'chat') -> str:
    cleaned = _UNSAFE_NAME_CHARS.sub('_', name or '').strip(' .')
    return cleaned[:200] or fallback


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


class UniqueNameResolver:
    """Hands out `name`, `name (1).ext`, `name (2).ext`... within one ZIP folder."""

    def __init__(self):
        self._taken: set[str] = set()

    def resolve(self, name: str) -> str:
        path = PurePosixPath(name)
        stem, suffix = path.stem, path.suffix
        candidate, index = name, 0
        while candidate.lower() in self._taken:
            index += 1
            candidate = f'{stem} ({index}){suffix}'
        self._taken.add(candidate.lower())
        return candidate


def canvas_file_name(name: str, canvas_type: str | None, code_language: str | None) -> str:
    base = sanitize_export_name(name, fallback='canvas')
    if PurePosixPath(base).suffix:
        return base
    if code_language:
        ext = _CODE_LANGUAGE_EXTENSIONS.get(code_language.lower(), 'txt')
    else:
        ext = 'md' if canvas_type == 'text' else 'txt'
    return f'{base}.{ext}'


def participant_display_name(participant: dict) -> str | None:
    meta = participant.get('meta') or {}
    entity_meta = participant.get('entity_meta') or {}
    return meta.get('name') or meta.get('user_name') or entity_meta.get('model_name')


def _participant_ref(participant: dict | None) -> dict | None:
    if not participant:
        return None
    return {
        'participant_id': participant['id'],
        'entity_name': participant['entity_name'],
        'name': participant_display_name(participant),
    }


def _attachment_item(item: dict, is_generated: bool) -> dict:
    result = {
        'type': 'generated_file' if is_generated else 'attachment_message',
        'name': PurePosixPath(item['name']).name,
        'attachment_type': item.get('attachment_type'),
        'filepath': item['filepath'],
    }
    if item.get('file_size') is not None:
        result['file_size'] = item['file_size']
    return result


def build_export_payload(
        conversation: dict,
        participants: list[dict],
        groups: list[dict],
        include_attachments: bool,
        exported_by: dict,
        project_id: int,
        exported_at: datetime | None = None,
) -> dict:
    """Pure mapper from plain dicts to the export JSON (no DB, no storage access)."""
    participants_by_id = {p['id']: p for p in participants}
    messages = []
    for group in groups:
        author = participants_by_id.get(group['author_participant_id'])
        is_generated = bool(author) and author['entity_name'] != ParticipantTypes.user.value
        items = []
        for item in group['items']:
            match item['item_type']:
                case 'text_message':
                    items.append({'type': 'text_message', 'content': item.get('content')})
                case 'canvas_message':
                    items.append({
                        'type': 'canvas_message',
                        'name': item.get('name'),
                        'canvas_type': item.get('canvas_type'),
                        'code_language': item.get('code_language'),
                        'content': item.get('content'),
                        '_ref': item['id'],
                    })
                case 'attachment_message':
                    entry = _attachment_item(item, is_generated)
                    entry['_ref'] = item['id']
                    items.append(entry)
        messages.append({
            'uuid': str(group['uuid']),
            'created_at': _isoformat(group['created_at']),
            'author': _participant_ref(author),
            'sent_to': _participant_ref(participants_by_id.get(group.get('sent_to_id'))),
            'reply_to_uuid': group.get('reply_to_uuid'),
            'items': items,
        })

    return {
        'export_info': {
            'format_version': EXPORT_FORMAT_VERSION,
            'exported_at': _isoformat(exported_at or datetime.now(timezone.utc)),
            'exported_by': exported_by,
            'include_attachments': include_attachments,
            'source': 'elitea',
            'project_id': project_id,
        },
        'conversation': {
            'id': conversation['id'],
            'uuid': str(conversation['uuid']),
            'name': conversation['name'],
            'is_private': conversation['is_private'],
            'author_id': conversation['author_id'],
            'instructions': conversation.get('instructions'),
            'folder': conversation.get('folder'),
            'created_at': _isoformat(conversation.get('created_at')),
            'updated_at': _isoformat(conversation.get('updated_at')),
            'participants': [
                {
                    'id': p['id'],
                    'entity_name': p['entity_name'],
                    'name': participant_display_name(p),
                    **({'version': p['version']} if p.get('version') else {}),
                }
                for p in participants
            ],
        },
        'messages': messages,
    }


def iter_export_items(payload: dict, item_type: str):
    for message in payload['messages']:
        for item in message['items']:
            if item_type == 'attachment' and item['type'] in ('attachment_message', 'generated_file'):
                yield item
            elif item['type'] == item_type:
                yield item


def strip_internal_refs(payload: dict) -> dict:
    for message in payload['messages']:
        for item in message['items']:
            item.pop('_ref', None)
    return payload


def load_conversation_export_data(session, project_id: int, conversation) -> tuple[dict, list[dict], list[dict]]:
    """Read everything the export needs into plain dicts while the session is open."""
    folder_name = None
    if conversation.folder_id:
        folder = session.query(ConversationFolder).filter(ConversationFolder.id == conversation.folder_id).first()
        folder_name = folder.name if folder else None

    conversation_dict = {
        'id': conversation.id,
        'uuid': conversation.uuid,
        'name': conversation.name,
        'is_private': conversation.is_private,
        'author_id': conversation.author_id,
        'instructions': conversation.instructions,
        'folder': folder_name,
        'created_at': conversation.created_at,
        'updated_at': conversation.updated_at,
    }
    participants = _resolve_participants(conversation.participants)

    groups = session.query(ConversationMessageGroup).filter(
        ConversationMessageGroup.conversation_id == conversation.id
    ).order_by(
        ConversationMessageGroup.created_at.asc(),
        ConversationMessageGroup.id.asc(),
    ).all()
    group_ids = [g.id for g in groups]
    uuid_by_id = {g.id: str(g.uuid) for g in groups}

    items_by_group = {}
    if group_ids:
        all_items = session.query(MessageItem).filter(
            MessageItem.message_group_id.in_(group_ids),
            MessageItem.item_type != 'context_message',
        ).order_by(MessageItem.order_index.asc(), MessageItem.id.asc()).all()
        for item in all_items:
            items_by_group.setdefault(item.message_group_id, []).append(_item_to_dict(item))

    group_dicts = [
        {
            'uuid': g.uuid,
            'author_participant_id': g.author_participant_id,
            'sent_to_id': g.sent_to_id,
            'reply_to_uuid': uuid_by_id.get(g.reply_to_id),
            'created_at': g.created_at,
            'items': items_by_group.get(g.id, []),
        }
        for g in groups
    ]
    return conversation_dict, participants, group_dicts


def _item_to_dict(item) -> dict:
    result = {'id': item.id, 'item_type': item.item_type}
    match item.item_type:
        case 'text_message':
            result['content'] = item.content
        case 'canvas_message':
            latest = item.latest_version
            result.update({
                'name': item.name,
                'canvas_type': item.canvas_type,
                'code_language': latest.code_language if latest else None,
                'content': latest.canvas_content if latest else '',
            })
        case 'attachment_message':
            result.update({
                'name': item.name,
                'bucket': item.bucket,
                'attachment_type': item.attachment_type,
                'filepath': item.filepath,
            })
    return result


def _resolve_participants(participants) -> list[dict]:
    result = [
        {
            'id': p.id,
            'entity_name': p.entity_name,
            'entity_meta': dict(p.entity_meta or {}),
            'meta': {},
        }
        for p in participants
    ]
    user_ids = [p['entity_meta'].get('id') for p in result if p['entity_name'] == ParticipantTypes.user.value]
    authors_by_id = {a['id']: a for a in (get_authors_data(user_ids) if user_ids else [])}

    for p in result:
        entity_meta = p['entity_meta']
        try:
            match p['entity_name']:
                case ParticipantTypes.user.value:
                    author = authors_by_id.get(entity_meta.get('id'))
                    if author:
                        p['meta']['user_name'] = author.get('name')
                case ParticipantTypes.application.value:
                    details = this.module.get_application_by_id(
                        project_id=entity_meta.get('project_id'),
                        application_id=entity_meta.get('id'),
                        first_existing_version=True,
                    ) or {}
                    p['meta']['name'] = details.get('name')
                    version = (details.get('version_details') or {}).get('name')
                    if version:
                        p['version'] = version
                case ParticipantTypes.toolkit.value:
                    details = this.module.get_toolkit_by_id(
                        project_id=entity_meta.get('project_id'),
                        toolkit_id=entity_meta.get('id'),
                    ) or {}
                    p['meta']['name'] = details.get('name') or details.get('toolkit_name')
        except Exception as e:  # pylint: disable=W0703
            log.warning("Chat export: failed to resolve participant %s name: %s", p['id'], e)
    return result


def collect_attachment_sizes(project_id: int, attachments: list[dict]) -> dict[str, int]:
    """Best-effort {filepath: size}, one listing per distinct bucket."""
    names_by_bucket = {}
    for item in attachments:
        names_by_bucket.setdefault(item['bucket'], set()).add(item['name'])
    sizes = {}
    if not names_by_bucket:
        return sizes
    mc = MinioClient.from_project_id(project_id)
    for bucket, names in names_by_bucket.items():
        try:
            for file_info in mc.list_files(bucket):
                if file_info['name'] in names:
                    sizes[f"/{bucket}/{file_info['name']}"] = file_info['size']
        except Exception as e:  # pylint: disable=W0703
            log.warning("Chat export: failed to list bucket %s: %s", bucket, e)
    return sizes


def build_export_zip(project_id: int, payload: dict, groups: list[dict], json_file_name: str):
    """Write JSON + attachments + canvases into a spooled temp file. Returns (file, missing_count)."""
    items_by_id = {item['id']: item for group in groups for item in group['items']}
    attachment_names = UniqueNameResolver()
    canvas_names = UniqueNameResolver()
    zip_file = tempfile.SpooledTemporaryFile(max_size=_ZIP_SPOOL_MAX_SIZE)
    missing_count = 0
    exported_paths = {}
    mc = None

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for entry in iter_export_items(payload, 'attachment'):
            source = items_by_id[entry['_ref']]
            filepath = source['filepath']
            if filepath in exported_paths:
                entry.update(exported_paths[filepath])
                continue
            try:
                if mc is None:
                    mc = MinioClient.from_project_id(project_id)
                data = mc.download_file(source['bucket'], source['name'])
                if not isinstance(data, (bytes, bytearray)):
                    data = bytes(data)
                export_path = f"{ATTACHMENTS_DIR}/{attachment_names.resolve(sanitize_export_name(entry['name'], 'file'))}"
                zf.writestr(export_path, data)
                result = {'export_path': export_path, 'export_status': 'included'}
            except Exception as e:  # pylint: disable=W0703
                log.warning("Chat export: failed to download %s: %s", filepath, e)
                missing_count += 1
                result = {'export_status': 'missing', 'error': 'File could not be retrieved'}
            exported_paths[filepath] = result
            entry.update(result)

        for entry in iter_export_items(payload, 'canvas_message'):
            file_name = canvas_names.resolve(
                canvas_file_name(entry.get('name'), entry.get('canvas_type'), entry.get('code_language'))
            )
            export_path = f'{CANVASES_DIR}/{file_name}'
            zf.writestr(export_path, entry.get('content') or '')
            entry['export_path'] = export_path

        zf.writestr(
            f'{json_file_name}.json',
            json.dumps(strip_internal_refs(payload), ensure_ascii=False, indent=2),
        )

    zip_file.seek(0)
    return zip_file, missing_count
