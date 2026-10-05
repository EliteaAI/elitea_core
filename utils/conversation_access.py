from sqlalchemy import Integer
from tools import rpc_tools

from ..models.enums.all import ParticipantTypes
from ..models.folder import ConversationFolder
from ..models.participants import Participant, ParticipantMapping
from .support_utils import get_support_config


NOT_FOUND = ({'error': 'Conversation not found'}, 404)
NOT_PARTICIPANT = ({'error': 'Only conversation participants can do this'}, 403)
NOT_PRIVILEGED = ({'error': 'Only the conversation author or a project admin can do this'}, 403)


def get_user_participant_id(session, conversation_id: int, user_id: int) -> int | None:
    row = session.query(Participant.id).join(
        ParticipantMapping, ParticipantMapping.participant_id == Participant.id
    ).filter(
        ParticipantMapping.conversation_id == conversation_id,
        Participant.entity_name == ParticipantTypes.user.value,
        Participant.entity_meta['id'].astext.cast(Integer) == user_id,
    ).first()
    return row[0] if row else None


def decide_access(is_private: bool, is_author: bool, is_participant: bool, is_admin, needs_privilege: bool):
    # is_admin is a callable so the admin RPC only runs when author/participant rules are not enough
    if is_author:
        return None
    if is_participant and not needs_privilege:
        return None
    if is_admin():
        return None
    if not is_participant:
        return NOT_FOUND if is_private else NOT_PARTICIPANT
    return NOT_PRIVILEGED


def check_conversation_access(session, project_id: int, conversation, user_id: int, needs_privilege: bool = False):
    if get_support_config().get('project_id') == project_id:
        return None
    return decide_access(
        is_private=bool(conversation.is_private),
        is_author=conversation.author_id == user_id,
        is_participant=get_user_participant_id(session, conversation.id, user_id) is not None,
        is_admin=lambda: bool(rpc_tools.RpcMixin().rpc.timeout(3).admin_check_user_is_admin(project_id, user_id)),
        needs_privilege=needs_privilege,
    )


def is_privileged_update(conversation, data: dict) -> bool:
    # UI resends unchanged fields (e.g. full meta), so only real changes count
    if data.get('instructions') is not None and data['instructions'] != conversation.instructions:
        return True
    if data.get('is_private') is not None and bool(data['is_private']) != bool(conversation.is_private):
        return True
    current_meta = conversation.meta or {}
    if data.get('is_hidden') is not None and bool(data['is_hidden']) != bool(current_meta.get('is_hidden')):
        return True
    new_meta = data.get('meta') or {}
    if 'persona' in new_meta and new_meta['persona'] != current_meta.get('persona'):
        return True
    if 'folder_id' in data and data['folder_id'] != conversation.folder_id:
        return True
    return False


def is_own_folder(session, folder_id: int, user_id: int) -> bool:
    return session.query(ConversationFolder.id).filter(
        ConversationFolder.id == folder_id,
        ConversationFolder.owner_id == user_id,
    ).first() is not None
