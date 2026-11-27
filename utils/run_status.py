"""Run state of a chat for the chat lists of its users (EL-7385).

A chat list row can't see the chat's room, so the state goes to each user's own room
(``room_notifications_<user_id>``, joined on connect). The audience is the conversation author and its
user participants: the users ``can_join_room`` lets in for a private chat. Members of a public chat who
never posted see the state on their next list load (``has_running_task``).

The payload carries ids and a status only, never message content.
"""
from pylon.core.tools import log  # pylint: disable=E0611,E0401

from ..models.enums.all import ParticipantTypes
from ..models.participants import Participant, ParticipantMapping
from .sio_utils import SioEvents, get_user_room


class RunStatus:
    running = 'running'
    waiting = 'waiting'  # paused on a question to the user (HITL, tool confirmation, MCP sign-in)
    finished = 'finished'
    failed = 'failed'
    stopped = 'stopped'


def _as_user_id(value):
    # entity_meta ids are JSON and may be stored as "5" instead of 5
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def run_status_audience(session, conversation_id: int, author_id) -> set[int]:
    """User ids that see this chat in their list and may join its room."""
    audience = set()
    author = _as_user_id(author_id)
    if author is not None:
        audience.add(author)
    rows = session.query(Participant.entity_meta).join(
        ParticipantMapping, ParticipantMapping.participant_id == Participant.id
    ).filter(
        ParticipantMapping.conversation_id == conversation_id,
        Participant.entity_name == ParticipantTypes.user.value,
    ).all()
    for (entity_meta,) in rows:
        user_id = _as_user_id((entity_meta or {}).get('id'))
        if user_id is not None:
            audience.add(user_id)
    return audience


def run_status_payload(project_id, conversation_id, conversation_uuid, message_group_uuid, status) -> dict:
    return {
        'project_id': project_id,
        'conversation_id': conversation_id,
        'conversation_uuid': str(conversation_uuid),
        'message_group_uuid': str(message_group_uuid),
        'status': status,
    }


class RunStatusNotice:
    """What to announce, read inside the session; sent after it closes so no emit holds the row lock."""

    def __init__(self, project_id, conversation_id, conversation_uuid, message_group_uuid, status, audience):
        self.payload = run_status_payload(project_id, conversation_id, conversation_uuid, message_group_uuid, status)
        self.audience = set(audience or ())

    @classmethod
    def for_group(cls, session, project_id, msg_group, status):
        conversation = msg_group.conversation
        if conversation is None:
            return None
        return cls(
            project_id, conversation.id, conversation.uuid, msg_group.uuid, status,
            run_status_audience(session, conversation.id, conversation.author_id),
        )

    def send(self, sio) -> None:
        for user_id in sorted(self.audience):
            try:
                sio.emit(
                    event=SioEvents.chat_run_status.value,
                    data=self.payload,
                    room=get_user_room(user_id),
                )
            except Exception:  # pylint: disable=W0703
                # A lost list notice must never break the run; the list catches up on its next load
                log.exception('chat_run_status emit failed for user %s', user_id)


def build_notice(session, project_id, msg_group, status):
    """A notice for a message group, or None when it can't be built; never raises."""
    try:
        return RunStatusNotice.for_group(session, project_id, msg_group, status)
    except Exception:  # pylint: disable=W0703
        log.exception('chat_run_status: cannot read the audience of message %s', getattr(msg_group, 'uuid', None))
        return None


def send_notice(sio, notice) -> None:
    if notice is not None:
        notice.send(sio)
