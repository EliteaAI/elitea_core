"""Due query, claim and cron run insert of the schedule tick (EL-7011)."""
from datetime import datetime

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun

# Must stay stable across deploys: the legacy step of the tick and the migration (US-24) take the same keys
LEGACY_LOCK_BASE = 7011 << 32


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


def insert_cron_run(session, schedule_id: int) -> int | None:
    """Insert a running cron run; None when the active-run unique index already holds one."""
    run = EntityScheduleRun(schedule_id=schedule_id, trigger='cron', status='running')
    try:
        # A savepoint, so the conflict rolls back the insert only and the claim stays
        with session.begin_nested():
            session.add(run)
    except IntegrityError:
        return None
    return run.id


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
    return schedule_id


def legacy_lock_key(project_id: int) -> int:
    """The advisory lock key of one project, shared by the legacy step and the migration."""
    return LEGACY_LOCK_BASE + project_id
