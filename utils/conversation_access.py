from pylon.core.tools import log
from sqlalchemy import Integer, or_
from tools import rpc_tools

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.folder import ConversationFolder
from ..models.participants import Participant, ParticipantMapping
from .support_utils import get_support_config


NOT_FOUND = ({'error': 'Conversation not found'}, 404)
NOT_PARTICIPANT = ({'error': 'Only conversation participants can do this'}, 403)
NOT_PRIVILEGED = ({'error': 'Only the conversation author or a project admin can do this'}, 403)


def visible_conversation_ids(session, user_id: int, is_admin: bool):
    participant_subquery_filters = [Participant.entity_name == ParticipantTypes.user.value]
    if not is_admin:
        participant_subquery_filters.append(
            Participant.entity_meta['id'].astext.cast(Integer) == user_id,
        )

    participant_subquery = session.query(Participant.id).filter(
        *participant_subquery_filters
    ).subquery()

    return session.query(Conversation.id).distinct().join(
        ParticipantMapping,
        Conversation.id == ParticipantMapping.conversation_id
    ).join(
        Participant,
        Participant.id == ParticipantMapping.participant_id
    ).filter(
        or_(
            Conversation.is_private == False,
            Participant.id.in_(participant_subquery)
        )
    ).subquery()


def find_user_participant_id(participants, user_id: int) -> int | None:
    # str() on both sides: entity_meta ids are JSON and may be stored as "5" instead of 5
    for p in participants:
        if p.entity_name == ParticipantTypes.user.value and str((p.entity_meta or {}).get('id')) == str(user_id):
            return p.id
    return None


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


def _admin_checker(project_id: int, user_id: int):
    return lambda: bool(rpc_tools.RpcMixin().rpc.timeout(3).admin_check_user_is_admin(project_id, user_id))


def _is_support_project(project_id: int) -> bool:
    return get_support_config().get('project_id') == project_id


def check_conversation_access(project_id: int, conversation, user_id: int, needs_privilege: bool = False):
    # Author is the common case: answer before any RPC
    if conversation.author_id == user_id:
        return None
    denied = decide_access(
        is_private=bool(conversation.is_private),
        is_author=False,
        is_participant=find_user_participant_id(conversation.participants, user_id) is not None,
        is_admin=_admin_checker(project_id, user_id),
        needs_privilege=needs_privilege,
    )
    # Support-project lookup is an RPC, so only pay for it when about to deny
    if denied and _is_support_project(project_id):
        return None
    return denied


def check_post_access(project_id: int, conversation, user_id: int):
    if conversation is None:
        return NOT_FOUND
    # Public conversations keep auto-join on first message; private ones need membership
    if not conversation.is_private or find_user_participant_id(conversation.participants, user_id) is not None:
        return None
    if _is_support_project(project_id):
        return None
    return decide_access(
        is_private=True,
        is_author=conversation.author_id == user_id,
        is_participant=False,
        is_admin=_admin_checker(project_id, user_id),
        needs_privilege=False,
    )


def is_conversation_participant(session, conversation_id: int, user_id: int) -> bool:
    users = session.query(Participant).join(
        ParticipantMapping, ParticipantMapping.participant_id == Participant.id
    ).filter(
        ParticipantMapping.conversation_id == conversation_id,
        Participant.entity_name == ParticipantTypes.user.value,
    ).all()
    return find_user_participant_id(users, user_id) is not None


def room_access_facts(session, conversation_id: int, is_private: bool, author_id: int, user_id: int) -> dict:
    # Participant lookup only matters for private conversations the user does not own
    is_author = author_id == user_id
    is_participant = bool(is_private) and not is_author and is_conversation_participant(
        session, conversation_id, user_id
    )
    return {'is_private': bool(is_private), 'is_author': is_author, 'is_participant': is_participant}


def can_join_room(project_id: int, user_id: int, is_private: bool, is_author: bool, is_participant: bool) -> bool:
    if not is_private:
        return True

    def is_admin():
        try:
            return _admin_checker(project_id, user_id)()
        except Exception as e:  # pylint: disable=W0703
            log.warning("Admin check failed for user %s in project %s: %s", user_id, project_id, e)
            return False

    denied = decide_access(
        is_private=True, is_author=is_author, is_participant=is_participant,
        is_admin=is_admin, needs_privilege=False,
    )
    # Support-project lookup is an RPC, so only pay for it when about to deny
    return denied is None or _is_support_project(project_id)


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
