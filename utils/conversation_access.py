from pylon.core.tools import log
from tools import auth, db, rpc_tools, this
from tools import config as c

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.folder import ConversationFolder
from ..models.participants import Participant, ParticipantMapping
from .support_utils import get_support_config


NOT_FOUND = ({'error': 'Conversation not found'}, 404)
NOT_MANAGER = ({'error': 'Only the chat owner or an editor with access can do this'}, 403)
NOT_OWNER = ({'error': 'Only the chat owner can do this'}, 403)
EXTERNAL_SHARING_OFF = ({'error': 'External chat sharing is turned off'}, 403)
OWNER_NOT_REMOVABLE = ({'error': 'The chat owner cannot be removed'}, 400)
PEOPLE_IN_RUN = ({'error': 'A run cannot be shared with other people'}, 400)
MENTION_REFUSED = "{names} can't be mentioned: they don't have access to this chat."

# Lets someone the owner shared the chat with manage it as well (#7757)
MANAGE_SHARED = 'models.chat.conversations.manage_shared'


def register_manage_shared_permission() -> None:
    roles = {"admin": True, "editor": True, "viewer": False}
    auth.register_permissions({
        "permissions": [MANAGE_SHARED],
        "recommended_roles": {c.ADMINISTRATION_MODE: roles, c.DEFAULT_MODE: roles},
    })


def is_external_sharing_enabled() -> bool:
    # The admin switch Features › Chat › Chat Sharing; the UI reads the same key from extra_ui_config
    sharing = this.module.descriptor.config.get('extra_ui_config', {}).get('chat_sharing', {}) or {}
    return sharing.get('external_enabled', True) is not False


def find_user_participant_id(participants, user_id: int) -> int | None:
    # str() on both sides: entity_meta ids are JSON and may be stored as "5" instead of 5
    for p in participants:
        if p.entity_name == ParticipantTypes.user.value and str((p.entity_meta or {}).get('id')) == str(user_id):
            return p.id
    return None


def decide_access(is_private: bool, is_author: bool, is_participant: bool, can_manage, needs_privilege: bool):
    # can_manage is a callable so permissions are only resolved when author/participant rules are not enough.
    # A project role alone opens nothing: a private chat is for its author and the people it was shared with (#7757)
    if is_author:
        return None
    if is_private and not is_participant:
        return NOT_FOUND
    if needs_privilege and not can_manage():
        return NOT_MANAGER
    return None


def _manage_checker(project_id: int):
    return lambda: MANAGE_SHARED in auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id)


def _admin_checker(project_id: int, user_id: int):
    return lambda: bool(rpc_tools.RpcMixin().rpc.timeout(3).admin_check_user_is_admin(project_id, user_id))


def is_run_conversation(meta) -> bool:
    # An agent / skill run, not a chat: project admins read the run history of everyone (#6927)
    return bool((meta or {}).get('single_participant'))


def _is_support_project(project_id: int) -> bool:
    return get_support_config().get('project_id') == project_id


def check_conversation_access(project_id: int, conversation, user_id: int, needs_privilege: bool = False):
    # Author is the common case: answer before any lookup
    if conversation.author_id == user_id:
        return None
    denied = decide_access(
        is_private=bool(conversation.is_private),
        is_author=False,
        is_participant=find_user_participant_id(conversation.participants, user_id) is not None,
        can_manage=_manage_checker(project_id),
        needs_privilege=needs_privilege,
    )
    # Support-project lookup is an RPC, so only pay for it when about to deny
    if denied and _is_support_project(project_id):
        return None
    return denied


def check_access_by_id(project_id: int, conversation_id: int, user_id: int, needs_privilege: bool = False):
    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter(Conversation.id == conversation_id).first()
        if conversation is None:
            return NOT_FOUND
        return check_conversation_access(project_id, conversation, user_id, needs_privilege=needs_privilege)


def managed_link_conversation(project_id: int, token: str, user_id: int) -> int | None:
    """The chat of a share link, when the caller is its owner or a manager; else None (#7757)."""
    from ..models.all import ConversationShareToken  # pylint: disable=C0415
    with db.get_session(project_id) as session:
        share = session.query(ConversationShareToken.conversation_id).filter_by(token=token, is_revoked=False).first()
    if share is None:
        return None
    if check_access_by_id(project_id, share.conversation_id, user_id, needs_privilege=True):
        return None
    return share.conversation_id


def check_read_access(project_id: int, conversation, user_id: int):
    if conversation is None:
        return NOT_FOUND
    # Whoever can open the chat can read it: public chats are open to the whole project (#7165)
    if not conversation.is_private:
        return None
    denied = check_conversation_access(project_id, conversation, user_id)
    if denied and is_run_conversation(conversation.meta) and _admin_checker(project_id, user_id)():
        return None
    return denied


def check_post_access(project_id: int, conversation, user_id: int):
    if conversation is None:
        return NOT_FOUND
    # Public conversations keep auto-join on first message; private ones need membership
    if not conversation.is_private or find_user_participant_id(conversation.participants, user_id) is not None:
        return None
    if conversation.author_id == user_id or _is_support_project(project_id):
        return None
    return NOT_FOUND


def check_owner(conversation, user_id: int, project_id: int | None = None):
    # Deleting the chat and filing it in a folder stay with the owner (#7757)
    if conversation.author_id == user_id:
        return None
    if project_id is not None and _is_support_project(project_id):
        return None
    return NOT_OWNER


def is_conversation_participant(session, conversation_id: int, user_id: int) -> bool:
    users = session.query(Participant).join(
        ParticipantMapping, ParticipantMapping.participant_id == Participant.id
    ).filter(
        ParticipantMapping.conversation_id == conversation_id,
        Participant.entity_name == ParticipantTypes.user.value,
    ).all()
    return find_user_participant_id(users, user_id) is not None


def room_access_facts(session, conversation_id: int, is_private: bool, author_id: int, user_id: int,
                      meta=None) -> dict:
    # Participant lookup only matters for private conversations the user does not own
    is_author = author_id == user_id
    is_participant = bool(is_private) and not is_author and is_conversation_participant(
        session, conversation_id, user_id
    )
    return {
        'is_private': bool(is_private), 'is_author': is_author, 'is_participant': is_participant,
        'is_run': is_run_conversation(meta),
    }


def can_join_room(project_id: int, user_id: int, is_private: bool, is_author: bool, is_participant: bool,
                  is_run: bool = False) -> bool:
    if not is_private or is_author or is_participant:
        return True
    if is_run:
        try:
            if _admin_checker(project_id, user_id)():
                return True
        except Exception as e:  # pylint: disable=W0703
            log.warning("Admin check failed for user %s in project %s: %s", user_id, project_id, e)
    # Support-project lookup is an RPC, so only pay for it when about to deny
    return _is_support_project(project_id)


def changes_privacy(conversation, data: dict) -> bool:
    return data.get('is_private') is not None and bool(data['is_private']) != bool(conversation.is_private)


def moves_folder(conversation, data: dict) -> bool:
    return 'folder_id' in data and data['folder_id'] != conversation.folder_id


def removes_other_user(conversation, participant_id: int, user_id: int) -> bool:
    for p in conversation.participants:
        if p.id == participant_id:
            return p.entity_name == ParticipantTypes.user.value and str((p.entity_meta or {}).get('id')) != str(user_id)
    return False


def is_owner_participant(conversation, participant_id: int) -> bool:
    return find_user_participant_id(conversation.participants, conversation.author_id) == participant_id


def users_outside_project(project_id: int, participants, caller_id: int) -> list[int]:
    """Ids of the people among new participants who are not members of the project (#7757)."""
    user_ids = [
        p.entity_meta.id for p in participants
        if getattr(p.entity_name, 'value', p.entity_name) == ParticipantTypes.user.value
        and str(p.entity_meta.id) != str(caller_id)
    ]
    if not user_ids or _is_support_project(project_id):
        return []
    members = {
        str(uid) for uid in rpc_tools.RpcMixin().rpc.timeout(5).admin_get_users_ids_in_project(project_id=project_id) or []
    }
    return [uid for uid in user_ids if str(uid) not in members]


def invites_people(participants, caller_id: int) -> bool:
    return any(
        getattr(p.entity_name, 'value', p.entity_name) == ParticipantTypes.user.value
        and str(p.entity_meta.id) != str(caller_id)
        for p in participants or []
    )


def check_run_people(meta, participants, caller_id: int):
    # Project admins read every run (#6927), so a run holds no one else's messages
    if is_run_conversation(meta) and invites_people(participants, caller_id):
        return PEOPLE_IN_RUN
    return None


def outside_project_error(user_ids: list[int]):
    return {'error': f"Users {', '.join(map(str, user_ids))} are not members of this project"}, 400


def is_privileged_update(conversation, data: dict) -> bool:
    # UI resends unchanged fields (e.g. full meta), so only real changes count.
    # Folder moves are owner-only and checked apart (moves_folder)
    if data.get('name') is not None and data['name'] != conversation.name:
        return True
    if data.get('instructions') is not None and data['instructions'] != conversation.instructions:
        return True
    if changes_privacy(conversation, data):
        return True
    current_meta = conversation.meta or {}
    if data.get('is_hidden') is not None and bool(data['is_hidden']) != bool(current_meta.get('is_hidden')):
        return True
    new_meta = data.get('meta') or {}
    for key in ('persona', 'internal_tools'):
        if key in new_meta and new_meta[key] != current_meta.get(key):
            return True
    return False


def is_own_folder(session, folder_id: int, user_id: int) -> bool:
    return session.query(ConversationFolder.id).filter(
        ConversationFolder.id == folder_id,
        ConversationFolder.owner_id == user_id,
    ).first() is not None


def check_mentions(project_id: int, conversation, sender_id: int, mentioned_user_ids) -> tuple[list[int], dict | None]:
    """Split mentioned project user ids into people to mention and a refusal naming those who cannot open the chat."""
    candidates = [uid for uid in dict.fromkeys(mentioned_user_ids or []) if uid != sender_id]
    if not candidates:
        return [], None
    project_members = {
        str(uid) for uid in rpc_tools.RpcMixin().rpc.timeout(5).admin_get_users_ids_in_project(project_id=project_id) or []
    }
    selected_people = {str(conversation.author_id)} | {
        str((p.entity_meta or {}).get('id'))
        for p in conversation.participants
        if p.entity_name == ParticipantTypes.user.value
    }
    can_open = project_members & selected_people if conversation.is_private else project_members
    rejected = [uid for uid in candidates if str(uid) not in can_open]
    if not rejected:
        return candidates, None
    return [], {
        'error': MENTION_REFUSED.format(names=format_refused_mention_names(rejected, project_members)),
        'rejected_user_ids': rejected,
    }


def format_refused_mention_names(user_ids: list[int], project_members: set[str]) -> str:
    """Names of project members among the ids; anyone else stays a bare id so an id never reveals its owner."""
    members = [uid for uid in user_ids if str(uid) in project_members]
    names = {}
    if members:
        try:
            names = {user['id']: user.get('name') or user.get('email') for user in auth.list_users(user_ids=members)}
        except Exception as e:  # pylint: disable=W0703
            log.warning("Mentioned users not resolved: %s", e)
    return ', '.join(names.get(uid) or f'User {uid}' for uid in user_ids)
