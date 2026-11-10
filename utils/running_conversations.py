"""Which chats have a run in progress (EL-7385): the list spinner, and the admin cleanup of stuck rows.

Every chat run start writes the task id to its response row (``applications_predict_task_id``). A row
still streaming without one past a short grace never started: the send broke before dispatch and left
``is_streaming`` behind. Such a row ends nowhere on its own, so it is not "running"; the admin task
``close_never_started_chat_runs`` ends the old ones.
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


def is_never_started_run(grace: timedelta = NOT_STARTED_GRACE):
    """Filter for a streaming row without a task id, created more than ``grace`` ago."""
    return and_(
        ConversationMessageGroup.is_streaming.is_(True),
        ConversationMessageGroup.task_id.is_(None),
        ConversationMessageGroup.created_at <= _utc_now() - grace,
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


def count_never_started_runs(session, grace: timedelta = NOT_STARTED_GRACE) -> int:
    """How many rows of the project ``close_never_started_runs`` would end."""
    return session.query(ConversationMessageGroup.id).filter(is_never_started_run(grace)).count()


def close_never_started_runs(session, conversation_id: int | None = None,
                             grace: timedelta = NOT_STARTED_GRACE) -> list:
    """End the rows whose run never started, in one conversation or the whole project; returns them, committed."""
    query = session.query(ConversationMessageGroup).filter(is_never_started_run(grace))
    if conversation_id is not None:
        query = query.filter(ConversationMessageGroup.conversation_id == conversation_id)
    rows = query.all()
    for row in rows:
        row.is_streaming = False
    if rows:
        session.commit()
    return rows


# The admin cleanup waits far longer than the list grace: a run whose task-id event was lost is still live, and an
# agent run can take hours. A day is past any real run, so only rows that can no longer end are touched.
CLEANUP_DEFAULT_HOURS = 24


def cleanup_hours(param) -> int | None:
    """``older_than_hours`` of an admin task param; the default when absent, None when not a positive integer."""
    for segment in (param or "").split(";"):
        key, _, value = segment.strip().partition("=")
        if key.strip().lower() == "older_than_hours":
            try:
                hours = int(value.strip())
            except ValueError:
                return None
            return hours if hours > 0 else None
    return CLEANUP_DEFAULT_HOURS


def run_never_started_cleanup(get_session, project_ids, hours: int, dry_run: bool) -> dict:
    """End (or, on a dry run, count) never-started rows per project; one failing project doesn't stop the rest."""
    grace = timedelta(hours=hours)
    projects, failed = {}, []
    for pid in project_ids:
        try:
            with get_session(pid) as session:
                projects[pid] = (
                    count_never_started_runs(session, grace) if dry_run
                    else len(close_never_started_runs(session, None, grace))
                )
        except Exception:  # pylint: disable=W0703
            failed.append({"project_id": pid})
    return {
        "closed": sum(projects.values()),
        "projects": {pid: n for pid, n in projects.items() if n},
        "failed": len(failed),
        "failed_projects": failed,
        "older_than_hours": hours,
        "dry_run": dry_run,
    }
