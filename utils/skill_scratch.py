"""The working files of a conversation's skill scripts (#7185): where the runtime keeps them, and their removal."""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

from sqlalchemy import func

from pylon.core.tools import log
from tools import MinioClient, db

from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from .internal_tools import get_default_attachment_bucket

#: Twin of elitea_sdk runtime/tools/skill_script_runner.SCRATCH_ROOT_PREFIX: the runtime writes, this module removes
SCRATCH_ROOT_PREFIX = '_skill_scratch'
DEFAULT_IDLE_DAYS = 30


def scratch_prefix(conversation_uuid: str) -> str:
    """Every object of one conversation's scratch starts with this key prefix."""
    return f'{SCRATCH_ROOT_PREFIX}/{conversation_uuid}/'


def list_objects_under(mc, bucket: str, prefix: str) -> List[str]:
    """The object names under a key prefix: paged by prefix on S3, filtered from the bucket listing on the other
    engines, which have no prefix filter; empty when the project has no attachment bucket yet."""
    if bucket not in mc.list_bucket():
        return []
    s3_client = getattr(mc, 's3_client', None)
    if not callable(getattr(s3_client, 'list_objects_v2', None)):
        return [item['name'] for item in mc.list_files(bucket) if item.get('name', '').startswith(prefix)]
    names: List[str] = []
    continuation_token = None
    while True:
        request = {'Bucket': mc.format_bucket_name(bucket), 'Prefix': prefix}
        if continuation_token:
            request['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**request)
        names.extend(item['Key'] for item in response.get('Contents', []))
        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            return names


def list_scratch_objects(mc, bucket: str) -> Dict[str, list]:
    """The scratch object names of the bucket, by the conversation they belong to; empty when the bucket has none."""
    root = f'{SCRATCH_ROOT_PREFIX}/'
    by_conversation: Dict[str, list] = {}
    for name in list_objects_under(mc, bucket, root):
        conversation_uuid = name[len(root):].split('/', 1)[0]
        if conversation_uuid:
            by_conversation.setdefault(conversation_uuid, []).append(name)
    return by_conversation


def remove_scratch_objects(mc, bucket: str, conversation_uuid: str) -> int:
    """Delete every scratch object of the conversation; the number removed."""
    names = list_objects_under(mc, bucket, scratch_prefix(conversation_uuid))
    for name in names:
        mc.remove_file(bucket, name)
    return len(names)


def index_conversation_scratch(mc, bucket: str) -> Dict[str, list]:
    """One listing of every conversation's scratch in the bucket, for a bulk delete to remove from per conversation."""
    return list_scratch_objects(mc, bucket)


def remove_indexed_scratch(mc, bucket: str, index: Dict[str, list], conversation_uuid: str) -> int:
    """Delete the conversation's scratch objects already found by ``index_conversation_scratch``; the number removed."""
    names = index.pop(conversation_uuid, [])
    for name in names:
        mc.remove_file(bucket, name)
    return len(names)


def remove_conversation_scratch(project_id: int, conversation_uuid: str, mc=None) -> int:
    """Remove a conversation's scratch from the project's attachment bucket; used when the conversation is deleted."""
    bucket = get_default_attachment_bucket(project_id)
    return remove_scratch_objects(mc or MinioClient.from_project_id(project_id), bucket, conversation_uuid)


def conversation_last_activity(project_id: int, conversation_uuids: list) -> Dict[str, Optional[datetime]]:
    """When each conversation last got a message (its creation when it has none); a missing conversation maps to None."""
    activity: Dict[str, Optional[datetime]] = {uuid: None for uuid in conversation_uuids}
    with db.get_session(project_id) as session:
        rows = session.query(
            Conversation.uuid, Conversation.created_at, func.max(ConversationMessageGroup.created_at),
        ).outerjoin(
            ConversationMessageGroup, ConversationMessageGroup.conversation_id == Conversation.id,
        ).filter(
            Conversation.uuid.in_(conversation_uuids),
        ).group_by(Conversation.id).all()
    for conversation_uuid, created_at, last_message_at in rows:
        activity[str(conversation_uuid)] = last_message_at or created_at
    return activity


def sweep_idle_scratch(
    project_id: int,
    idle_days: int = DEFAULT_IDLE_DAYS,
    now: Optional[datetime] = None,
    mc=None,
    last_activity_of: Optional[Callable[[int, list], Dict[str, Optional[datetime]]]] = None,
) -> dict:
    """Remove the scratch of conversations idle for ``idle_days`` or gone; the attachments they shared are left alone."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=idle_days)
    bucket = get_default_attachment_bucket(project_id)
    mc = mc or MinioClient.from_project_id(project_id)
    by_conversation = list_scratch_objects(mc, bucket)
    if not by_conversation:
        return {'removed': 0, 'kept': 0}
    conversation_uuids = sorted(key for key in by_conversation if is_conversation_uuid(key))
    activity = (last_activity_of or conversation_last_activity)(project_id, conversation_uuids) if conversation_uuids else {}
    removed = kept = 0
    for conversation_uuid, names in by_conversation.items():
        last_activity = activity.get(conversation_uuid)
        if last_activity is not None and _as_utc(last_activity) > cutoff:
            kept += 1
            continue
        for name in names:
            mc.remove_file(bucket, name)
        removed += 1
    log.info('skill scratch sweep: project %s removed %s conversation(s), kept %s', project_id, removed, kept)
    return {'removed': removed, 'kept': kept}


def is_conversation_uuid(key: str) -> bool:
    """Whether a scratch folder name can be a conversation; a run keyed otherwise has no conversation to keep it alive."""
    try:
        uuid.UUID(key)
    except ValueError:
        return False
    return True


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
