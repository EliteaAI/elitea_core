"""Due query, claim and cron run insert of the schedule tick, and the event list of a run (EL-7011)."""
from datetime import UTC, datetime

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun

# Must stay stable across deploys: the legacy step of the tick and the migration (US-24) take the same keys
LEGACY_LOCK_BASE = 7011 << 32
# Stored events per run. A run writes about ten; a long approval loop keeps its first 199 and its newest one
EVENT_LIMIT = 200
TEXT_LIMIT = 300
FINISH_LEVELS = {'success': 'success', 'failed': 'error'}  # blocked and skipped are warnings


def run_event(kind: str, text: str, level: str = 'info', **data) -> dict:
    """One event of the run log: time, kind, a short text, the level the log colours it by, and small data."""
    return {
        'at': datetime.now(UTC).isoformat(), 'kind': kind, 'text': str(text)[:TEXT_LIMIT], 'level': level,
        **{key: value for key, value in data.items() if value is not None},
    }


def finished_event(status: str, reason: str | None = None, level: str | None = None) -> dict:
    """The last event of a run: its final status and the reason; `level` overrides the level of the status."""
    text = f'Run finished: {status}' + (f' ({reason})' if reason else '')
    return run_event('finished', text, level or FINISH_LEVELS.get(status, 'warning'), status=status)


def merge_events(stored: list | None, tool_events: list[dict]) -> list[dict]:
    """The stored events and the tool calls in time order, at most `EVENT_LIMIT`; the first stay and the newest ends it."""
    # UTC ISO times sort as text; the stable sort keeps the stored order of equal times; no time last
    merged = sorted([*(stored or []), *tool_events], key=lambda event: (event["at"] is None, event["at"] or ""))
    return merged if len(merged) <= EVENT_LIMIT else [*merged[:EVENT_LIMIT - 1], merged[-1]]


def append_run_events(session, run_id: int, *events: dict, **columns) -> None:
    """Add events to a run, and set `columns`, in the caller's transaction; the row is locked first.

    Over `EVENT_LIMIT` the first events stay and the newest takes the last place, so the end is never lost.
    """
    run = session.scalar(
        select(EntityScheduleRun).where(EntityScheduleRun.id == run_id)
        .with_for_update().execution_options(populate_existing=True)
    )
    if run is None:
        return
    merged = [*(run.events or []), *events]
    run.events = merged if len(merged) <= EVENT_LIMIT else [*merged[:EVENT_LIMIT - 1], merged[-1]]
    for name, value in columns.items():
        setattr(run, name, value)


def select_due_schedules(session, entity_types, limit: int) -> list[EntitySchedule]:
    """Enabled schedules of `entity_types` due by now or past their stop date, oldest `next_run` first, at most `limit`.

    A schedule past its stop date is picked even when its next run is far away, so the tick disables it on time.
    """
    return list(session.scalars(
        select(EntitySchedule)
        .where(
            EntitySchedule.enabled.is_(True),
            EntitySchedule.entity_type.in_(entity_types),
            or_(EntitySchedule.next_run <= func.now(), EntitySchedule.expires_at <= func.now()),
        )
        .order_by(EntitySchedule.next_run, EntitySchedule.id)
        .limit(limit)
    ))


def claim_schedule(session, schedule_id: int, seen_next_run: datetime, new_next_run: datetime) -> bool:
    """Move `next_run` from the value this replica saw; true only when this call changed the row."""
    result = session.execute(
        update(EntitySchedule)
        .where(EntitySchedule.id == schedule_id, EntitySchedule.next_run == seen_next_run)
        .values(next_run=new_next_run, last_run=func.now())
    )
    return result.rowcount == 1


def insert_run(session, schedule_id: int, trigger: str, requested_by: int | None = None) -> int | None:
    """Insert a running run and copy `running` to its schedule; None when the active-run unique index already holds one.

    The copy is in the same transaction, so the list never shows the previous status for a running run (R1.5).
    """
    run = EntityScheduleRun(schedule_id=schedule_id, trigger=trigger, status='running', requested_by=requested_by)
    try:
        # A savepoint, so the conflict rolls back the insert only and the claim stays
        with session.begin_nested():
            session.add(run)
    except IntegrityError:
        return None
    session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule_id).values(last_run_status='running'))
    return run.id


def insert_cron_run(session, schedule_id: int) -> int | None:
    """Insert a running cron run; None when the active-run unique index already holds one."""
    return insert_run(session, schedule_id, 'cron')


def fail_run(session, run_id: int, reason: str) -> int | None:
    """Close a running run as failed and copy the status to its schedule; the schedule id, None when not running.

    A running run that never ends would make the active-run index block the schedule.
    """
    schedule_id = session.scalar(
        update(EntityScheduleRun)
        .where(EntityScheduleRun.id == run_id, EntityScheduleRun.status == 'running')
        .values(status='failed', reason=reason, finished_at=func.now())
        .returning(EntityScheduleRun.schedule_id)
    )
    if schedule_id is not None:
        session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule_id).values(last_run_status='failed'))
        append_run_events(session, run_id, finished_event('failed', reason))
    return schedule_id


def legacy_lock_key(project_id: int) -> int:
    """The advisory lock key of one project, shared by the legacy step and the migration."""
    return LEGACY_LOCK_BASE + project_id
