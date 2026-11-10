"""Which chats have a run in progress (EL-7385): the list spinner, and the admin cleanup of stuck rows.

Every chat run start writes the task id to its response row (``applications_predict_task_id``). A row is stuck
"streaming" when nothing will end it any more:
- it has no task id past a short grace: the send broke before dispatch;
- it is older than any real run: the task died without a stream end (indexer crash, restart).
Neither counts as "running"; the admin task ``close_stuck_chat_runs`` ends the old ones in the database.
"""
from datetime import datetime, timedelta

from sqlalchemy import and_, or_

from ..models.message_group import ConversationMessageGroup

NOT_STARTED_GRACE = timedelta(minutes=5)
# Past the longest run the platform allows (a mandate's max_runtime_s is at most 86400 s)
RUN_MAX_AGE = timedelta(hours=24)


def _utc_now():
    # created_at is naive UTC: server_default now() on a UTC database, or datetime.now(timezone.utc) from code
    return datetime.utcnow()


def _is_streaming():
    # `= true`, not `IS true`: Postgres matches only this form to the partial index `WHERE is_streaming`
    return ConversationMessageGroup.is_streaming == True  # noqa: E712  pylint: disable=C0121


def is_live_run():
    """Filter for a streaming row whose run started, or may still be starting, and is not older than any run."""
    now = _utc_now()
    return and_(
        _is_streaming(),
        ConversationMessageGroup.created_at > now - RUN_MAX_AGE,
        or_(
            ConversationMessageGroup.task_id.isnot(None),
            ConversationMessageGroup.created_at > now - NOT_STARTED_GRACE,
        ),
    )


def is_stuck_run(older_than: timedelta):
    """Filter for a streaming row created more than ``older_than`` ago, with a task id or without."""
    return and_(
        _is_streaming(),
        ConversationMessageGroup.created_at <= _utc_now() - older_than,
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


def count_stuck_runs(session, older_than: timedelta) -> int:
    """How many rows of the project ``close_stuck_runs`` would end."""
    return session.query(ConversationMessageGroup.id).filter(is_stuck_run(older_than)).count()


def close_stuck_runs(session, older_than: timedelta) -> int:
    """End the stuck rows of the project in one UPDATE, committed; returns how many."""
    closed = session.query(ConversationMessageGroup).filter(is_stuck_run(older_than)).update(
        {ConversationMessageGroup.is_streaming: False}, synchronize_session=False,
    )
    if closed:
        session.commit()
    return closed


# A day is past any real run (see RUN_MAX_AGE), so by default only rows that can no longer end are touched
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


def run_stuck_cleanup(get_session, project_ids, hours: int, dry_run: bool) -> dict:
    """End (or, on a dry run, count) stuck rows per project; one failing project doesn't stop the rest."""
    older_than = timedelta(hours=hours)
    projects, failed = {}, []
    for pid in project_ids:
        try:
            with get_session(pid) as session:
                projects[pid] = (
                    count_stuck_runs(session, older_than) if dry_run else close_stuck_runs(session, older_than)
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
