"""Which chats have a run in progress (EL-7385): the list spinner and the room catch-up.

Every chat run start writes the task id to its response row (``applications_predict_task_id``). A row
still streaming without one past a short grace never started: the send broke before dispatch and left
``is_streaming`` behind. Such a row ends nowhere on its own, so it is not "running".
"""
from datetime import datetime, timedelta

from sqlalchemy import and_, or_

from ..models.message_group import ConversationMessageGroup

NOT_STARTED_GRACE = timedelta(minutes=5)


def _utc_now():
    # created_at is naive UTC: server_default now() on a UTC database, or datetime.now(timezone.utc) from code
    return datetime.utcnow()


def is_live_run():
    """Filter for a streaming row whose run started, or may still be starting."""
    return and_(
        ConversationMessageGroup.is_streaming.is_(True),
        or_(
            ConversationMessageGroup.task_id.isnot(None),
            ConversationMessageGroup.created_at > _utc_now() - NOT_STARTED_GRACE,
        ),
    )


def is_never_started_run():
    """Filter for a streaming row whose run never started and never will."""
    return and_(
        ConversationMessageGroup.is_streaming.is_(True),
        ConversationMessageGroup.task_id.is_(None),
        ConversationMessageGroup.created_at <= _utc_now() - NOT_STARTED_GRACE,
    )


def running_conversation_ids(session, conversation_ids) -> set[int]:
    """Ids among ``conversation_ids`` with a live run; one query, bounded by the page."""
    ids = [i for i in conversation_ids or [] if i is not None]
    if not ids:
        return set()
    rows = session.query(ConversationMessageGroup.conversation_id).filter(
        ConversationMessageGroup.conversation_id.in_(ids),
        is_live_run(),
    ).distinct().all()
    return {row[0] for row in rows}


def sync_ended_runs(sio, session, project_id, conversation_uuid, rows) -> None:
    """Tell the chat room the rows ended, and their users' lists the spinner is gone."""
    from tools import serialize  # pylint: disable=C0415,E0401 -- keeps the queries above runtime-free
    from ..models.pd.message import MessageGroupDetail  # pylint: disable=C0415
    from .run_status import RunStatus, build_notice, send_notice  # pylint: disable=C0415
    from .sio_utils import SioEvents, get_chat_room  # pylint: disable=C0415

    room = get_chat_room(conversation_uuid)
    for row in rows:
        session.refresh(row)
        sio.emit(
            event=SioEvents.chat_message_sync.value,
            data=serialize(MessageGroupDetail.model_validate(row)),
            room=room,
        )
        send_notice(sio, build_notice(session, project_id, row, RunStatus.failed))


def close_never_started_runs(session, conversation_id: int) -> list:
    """End the conversation's rows whose run never started; returns them, committed, for a client sync."""
    rows = session.query(ConversationMessageGroup).filter(
        ConversationMessageGroup.conversation_id == conversation_id,
        is_never_started_run(),
    ).all()
    for row in rows:
        row.is_streaming = False
    if rows:
        session.commit()
    return rows
