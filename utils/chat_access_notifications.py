"""Tell people when their access to a shared chat changes (#7757).

One notification per person, only when their access really changed: adding someone to a chat open
to the whole project changes nothing for them, switching Whole team on or off changes it for every
member who is not in the chat. The person who made the change is never told.
"""
import types

from pylon.core.tools import log
from tools import rpc_tools

from ..models.enums.all import NotificationEventTypes, ParticipantTypes

ADDED_MESSAGE = 'You were added to the [{chat}]() in {project}.'
REMOVED_MESSAGE = 'You were removed from {chat} in {project}.'
# When the project name cannot be read
ADDED_SHORT_MESSAGE = 'You were added to the [{chat}]().'
REMOVED_SHORT_MESSAGE = 'You were removed from {chat}.'


def snapshot_conversation(conversation):
    """What the notifications need, read while the session is open."""
    return types.SimpleNamespace(
        id=conversation.id,
        name=conversation.name,
        author_id=conversation.author_id,
        is_private=bool(conversation.is_private),
        participants=[
            types.SimpleNamespace(id=p.id, entity_name=p.entity_name, entity_meta=dict(p.entity_meta or {}))
            for p in conversation.participants
        ],
    )


def removed_user_id(conversation, participant_id: int):
    for p in conversation.participants:
        if p.id == participant_id and p.entity_name == ParticipantTypes.user.value:
            return (p.entity_meta or {}).get('id')
    return None


def chat_user_ids(participants) -> set[int]:
    ids = set()
    for p in participants:
        if p.entity_name == ParticipantTypes.user.value:
            try:
                ids.add(int((p.entity_meta or {}).get('id')))
            except (TypeError, ValueError):
                continue
    return ids


def project_member_ids(project_id: int) -> set[int]:
    try:
        members = rpc_tools.RpcMixin().rpc.timeout(5).admin_get_users_ids_in_project(
            project_id=project_id, filter_system_user=True,
        )
    except Exception as e:  # pylint: disable=W0703
        log.warning("Project members not resolved for chat access notifications: %s", e)
        return set()
    return {int(uid) for uid in members or []}


def _project_name(project_id: int) -> str:
    try:
        project = rpc_tools.RpcMixin().rpc.timeout(5).project_get_by_id(project_id)
    except Exception as e:  # pylint: disable=W0703
        log.warning("Project %s not resolved for chat access notifications: %s", project_id, e)
        return ''
    if not isinstance(project, dict):
        return ''
    return project.get('name') or ''


def project_phrase(project_name: str) -> str:
    # «the Alpha project», but «the Team project» rather than «the Team project project»
    name = project_name.strip()
    return f'the {name}' if name.lower().endswith('project') else f'the {name} project'


def build_access_message(added: bool, chat_name: str, project_name: str) -> str:
    if project_name and project_name.strip():
        template = ADDED_MESSAGE if added else REMOVED_MESSAGE
        return template.format(chat=chat_name or 'chat', project=project_phrase(project_name))
    template = ADDED_SHORT_MESSAGE if added else REMOVED_SHORT_MESSAGE
    return template.format(chat=chat_name or 'chat')


def notify_chat_access(project_id: int, conversation, user_ids, added: bool, initiator_id=None) -> None:
    recipients = sorted({int(uid) for uid in user_ids if uid is not None and str(uid) != str(initiator_id)})
    if not recipients:
        return
    project_name = _project_name(project_id)
    event_type = NotificationEventTypes.chat_user_added if added else NotificationEventTypes.chat_user_removed
    meta = {
        'conversation_id': conversation.id,
        'conversation_name': conversation.name,
        'project_name': project_name,
        'message': build_access_message(added, conversation.name, project_name),
    }
    event_manager = rpc_tools.EventManagerMixin().event_manager
    for user_id in recipients:
        try:
            event_manager.fire_event('notifications_stream', {
                'project_id': project_id,
                'user_id': user_id,
                'meta': dict(meta),
                'event_type': event_type.value,
            })
        except Exception:  # pylint: disable=W0703
            log.exception("Chat access notification failed for user %s in project %s", user_id, project_id)


def team_access_recipients(member_ids: set[int], conversation) -> set[int]:
    """Members whose access Whole team changes: everyone but the chat's people and its owner."""
    return set(member_ids) - chat_user_ids(conversation.participants) - {conversation.author_id}


def notify_team_access_change(project_id: int, conversation, made_public: bool, initiator_id=None) -> None:
    """Whole team switched on or off: tell the members who are not in the chat."""
    from .conversation_access import _is_support_project  # pylint: disable=C0415
    # The support project's chats are not shared with its members
    if _is_support_project(project_id):
        return
    outside = team_access_recipients(project_member_ids(project_id), conversation)
    notify_chat_access(project_id, conversation, outside, added=made_public, initiator_id=initiator_id)
