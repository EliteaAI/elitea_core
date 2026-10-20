"""Run status and schedule disable writers of the schedule runs (EL-7011)."""
from datetime import datetime, timedelta

from sqlalchemy import func, update
from tools import db

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from .schedule_claim import fail_run

STALE_REASON = 'the run did not finish in time'
PAUSE_REASON = 'the run stopped for input that no one can give'
# The notice of each final status a notice follows; skipped and blocked runs send none (R4.3)
NOTICE_EVENTS = {'failed': 'schedule_run_failed', 'success': 'schedule_run_succeeded'}


def fail_schedule_run(project_id: int, run_id: int, reason: str) -> None:
    """Close a running run as failed with `reason` and copy the status to its schedule; one transaction."""
    with db.get_session(project_id) as session:
        fail_run(session, run_id, reason)
        session.commit()


def disable_schedule(project_id: int, schedule_id: int, reason: str) -> None:
    """Switch one schedule off with `disabled_reason`; other schedules stay as they are."""
    with db.get_session(project_id) as session:
        session.execute(
            update(EntitySchedule)
            .where(EntitySchedule.id == schedule_id)
            .values(enabled=False, disabled_reason=reason)
        )
        session.commit()


def finish_schedule_run(project_id: int, run_id: int, status: str, reason: str | None = None,
                        conversation_id: int | None = None) -> bool:
    """End a running run as `success` or `failed`, copy it to its schedule and tell the owner.

    False, with nothing changed or sent, when the run is not running any more.
    """
    with db.get_session(project_id) as session:
        ended = _end_runs(session, EntityScheduleRun.id == run_id, status, reason, conversation_id)
        session.commit()
        _announce(project_id, session, ended)
    return bool(ended)


def pause_schedule_run(project_id: int, run_id: int, interrupt) -> None:
    """Handle an interrupt of a schedule run; no branch claims it yet, so the run fails (R5.2)."""
    # lean: `interrupt` is unused here; US-18 and US-19 read it to route their pauses
    finish_schedule_run(project_id, run_id, 'failed', PAUSE_REASON)


def record_skipped_run(session, schedule_id: int, reason: str) -> int:
    """Insert a finished `skipped` cron run without a conversation and copy the status; the run id."""
    run = EntityScheduleRun(
        schedule_id=schedule_id, trigger='cron', status='skipped', reason=reason, finished_at=func.now())
    session.add(run)
    session.flush()
    session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule_id).values(last_run_status='skipped'))
    return run.id


def block_schedule_run(project_id: int, run_id: int, reason: str) -> None:
    """End a running run as `blocked` with `reason` and copy the status; no notice."""
    with db.get_session(project_id) as session:
        _end_runs(session, EntityScheduleRun.id == run_id, 'blocked', reason)
        session.commit()


def fail_stale_runs(project_id: int, now: datetime, stale_minutes: int) -> int:
    """Fail every running run quiet for more than `stale_minutes`, tell the owners; the number failed.

    A run that never ends would block its schedule through the active-run index. A resumed run counts
    from `resumed_at`. The rows are the running runs only, one per schedule at most.
    """
    quiet_since = func.coalesce(EntityScheduleRun.resumed_at, EntityScheduleRun.started_at)
    with db.get_session(project_id) as session:
        ended = _end_runs(session, quiet_since < now - timedelta(minutes=stale_minutes), 'failed', STALE_REASON)
        session.commit()
        _announce(project_id, session, ended)
    return len(ended)


def _end_runs(session, condition, status: str, reason: str | None, conversation_id: int | None = None) -> list[int]:
    """End the running runs that match `condition` and copy the status to their schedules; the run ids."""
    values = {'status': status, 'reason': reason, 'finished_at': func.now()}
    schedule_values = {'last_run_status': status}
    if conversation_id:
        values['conversation_id'] = conversation_id
        schedule_values['last_conversation_id'] = conversation_id
    ended = session.execute(
        update(EntityScheduleRun)
        .where(EntityScheduleRun.status == 'running', condition)
        .values(**values)
        .returning(EntityScheduleRun.id, EntityScheduleRun.schedule_id)
    ).all()
    if ended:
        session.execute(
            update(EntitySchedule).where(EntitySchedule.id.in_({schedule_id for _, schedule_id in ended}))
            .values(**schedule_values)
        )
    return [run_id for run_id, _ in ended]


def _announce(project_id: int, session, run_ids: list[int]) -> None:
    """Send the notice of each ended run to its owner; an agent run also tells the owner's open chats."""
    # Late import: the notice stack is needed only here, so the writers above load without it
    from .schedule_notifications import (
        emit_schedule_run_finished,
        notify_schedule_event,
    )
    for run_id in run_ids:
        run = session.get(EntityScheduleRun, run_id)
        event = NOTICE_EVENTS.get(run.status)
        if event is None:  # the status moved on after the commit; that writer owns the notice
            continue
        schedule = session.get(EntitySchedule, run.schedule_id)
        notify_schedule_event(project_id, schedule.created_by, event, schedule, run)
        if schedule.entity_type == 'agent':
            emit_schedule_run_finished(
                schedule.created_by, schedule.id, run.id, run.status, schedule.application_id, run.conversation_id)
