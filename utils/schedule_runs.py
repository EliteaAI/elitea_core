"""Run status and schedule disable writers of the schedule runs (EL-7011)."""
from datetime import UTC, datetime, timedelta

from pylon.core.tools import log
from sqlalchemy import func, select, update
from tools import db

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from .schedule_claim import fail_run

STALE_REASON = 'the run did not finish in time'
EXPIRED_REASON = 'approval expired'
EXPIRE_BATCH = 100
NAME_LIMIT = 100  # a step or toolkit name in a run reason, shown to the owner
PAUSE_REASON = 'the run needs a human answer'
# The notice of each final status a notice follows; skipped and blocked runs send none (R4.3)
NOTICE_EVENTS = {'failed': 'schedule_run_failed', 'success': 'schedule_run_succeeded'}
APPROVAL_WAIT_HOURS = 24
REJECT_REASON = 'rejected by the owner'
# The HITL actions of a decision: `reject` ends a waiting run, the others let it go on (R2.1, R2.2)
DECISION_ACTIONS = ('approve', 'edit', 'answer', 'block_with_comment', 'reject')


class ScheduleDecisionForbidden(Exception):
    """The caller of an approve or reject is not the schedule owner; an admin included (R2.3)."""


class ScheduleDecisionInvalid(Exception):
    """An action of an approve or reject is empty or unknown."""


class ScheduleRunNotWaiting(Exception):
    """The run of an approve or reject is not waiting for approval (R2.4)."""


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
                        conversation_id: int | None = None, expected_status: str = 'running',
                        notice: bool = True) -> bool:
    """End a run in `expected_status` as `success` or `failed`, copy it to its schedule and tell the owner.

    False, with nothing changed or sent, when the run is not in `expected_status` any more.
    """
    with db.get_session(project_id) as session:
        ended = _end_runs(session, EntityScheduleRun.id == run_id, status, reason, conversation_id, expected_status)
        session.commit()
        if notice:
            _announce(project_id, session, ended)
    return bool(ended)


def pause_schedule_run(project_id: int, run_id: int, interrupt: dict) -> None:
    """A sensitive-tool pause makes a running run wait for the owner (R1.2, R1.3); any other pause fails it."""
    # A parallel pause carries its interrupts in `hitl_interrupts`
    interrupts = [interrupt.get('hitl_interrupt') or {}, *(interrupt.get('hitl_interrupts') or [])]
    if not any(item.get('guardrail_type') == 'sensitive_tool' for item in interrupts):
        finish_schedule_run(project_id, run_id, 'failed', _human_step_reason(interrupt, interrupts))
        return
    # Late import, as in _announce
    from .schedule_notifications import notify_schedule_event
    with db.get_session(project_id) as session:
        paused = session.execute(
            update(EntityScheduleRun)
            .where(EntityScheduleRun.id == run_id, EntityScheduleRun.status == 'running')
            # resumed_at is cleared, so a stale fan-out decision of an earlier pause never matches this one
            .values(status='waiting_for_approval', resumed_at=None,
                    approval_expires_at=datetime.now(UTC) + timedelta(hours=APPROVAL_WAIT_HOURS))
        ).rowcount
        session.commit()
        if not paused:  # a run that is not running stays as it is, with no notice (R1.4)
            return
        try:
            run = session.get(EntityScheduleRun, run_id)
            schedule = session.get(EntitySchedule, run.schedule_id)
            notify_schedule_event(project_id, schedule.created_by, 'schedule_approval_needed', schedule, run)
        except Exception:  # noqa: BLE001 -- the pause is committed; a lost notice must not undo it (R1.3)
            log.exception('Schedule run %s: cannot send the approval notice', run_id)


def _human_step_reason(interrupt: dict, interrupts: list[dict]) -> str:
    """`PAUSE_REASON` plus the toolkit or step the pause names; the toolkit first, as an MCP sign-in node says little (R2.4)."""
    for key, label in (('toolkit_name', 'toolkit'), ('node_name', 'step')):
        name = next((item[key] for item in (interrupt, *interrupts) if item.get(key)), None)
        if name:
            return f"{PAUSE_REASON} ({label} '{str(name)[:NAME_LIMIT]}')"
    return PAUSE_REASON


def check_schedule_decision(project_id: int, run_id: int, caller_id: int, actions: list,
                            fan_out: bool = False) -> str | None:
    """Check the owner's decision on a waiting run without changing it; `reject` or `approve`.

    Any `reject` in `actions` rejects. `ScheduleDecisionForbidden` for anyone but `created_by`, an admin
    included (R2.3); then `ScheduleDecisionInvalid` for an empty or unknown action; then
    `ScheduleRunNotWaiting` when the run is not waiting (R2.4). None when a fan-out child decides a run
    that an earlier decision of the same pause resumed: nothing to change.
    """
    with db.get_session(project_id) as session:
        run = session.get(EntityScheduleRun, run_id)
        if run is None:
            raise ScheduleRunNotWaiting(run_id)
        if session.get(EntitySchedule, run.schedule_id).created_by != caller_id:
            raise ScheduleDecisionForbidden(run_id)
        status, resumed_at = run.status, run.resumed_at
    if not actions or not all(action in DECISION_ACTIONS for action in actions):
        raise ScheduleDecisionInvalid(actions)
    if fan_out and status == 'running' and resumed_at is not None:
        return None
    if status != 'waiting_for_approval':
        raise ScheduleRunNotWaiting(run_id)
    return 'reject' if 'reject' in actions else 'approve'


def apply_schedule_decision(project_id: int, run_id: int, decision: str) -> None:
    """Write a checked decision: `approve` sets the run running, `reject` fails it (R2.1, R2.2).

    `ScheduleRunNotWaiting` when another decision or the expiry changed the run since the check (R2.4).
    """
    with db.get_session(project_id) as session:
        if decision == 'approve':
            resumed = session.execute(
                update(EntityScheduleRun)
                .where(EntityScheduleRun.id == run_id, EntityScheduleRun.status == 'waiting_for_approval')
                .values(status='running', resumed_at=func.now(), approval_expires_at=None)
            ).rowcount
            session.commit()
            if not resumed:
                raise ScheduleRunNotWaiting(run_id)
            return
        conversation_id = session.scalar(
            select(EntityScheduleRun.conversation_id).where(EntityScheduleRun.id == run_id))
    if not finish_schedule_run(project_id, run_id, 'failed', REJECT_REASON, conversation_id=conversation_id,
                               expected_status='waiting_for_approval', notice=False):
        raise ScheduleRunNotWaiting(run_id)


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


def expire_waiting_runs(project_id: int, now: datetime) -> int:
    """Fail the runs whose approval wait ended before `now`, tell the owners; the number failed.

    At most `EXPIRE_BATCH` runs per call; the rest go in the next tick. The update keeps the
    `waiting_for_approval` condition, so an owner decision that wins the race is never overwritten (R1.1).
    """
    expired = (select(EntityScheduleRun.id)
               .where(EntityScheduleRun.status == 'waiting_for_approval', EntityScheduleRun.approval_expires_at < now)
               .order_by(EntityScheduleRun.approval_expires_at, EntityScheduleRun.id)
               .limit(EXPIRE_BATCH))
    with db.get_session(project_id) as session:
        ended = _end_runs(session, EntityScheduleRun.id.in_(expired), 'failed', EXPIRED_REASON,
                          expected_status='waiting_for_approval')
        session.commit()
        # Only the expiry notice: a plain failure notice would be a second one (R1.3)
        _announce(project_id, session, ended, {'failed': 'schedule_approval_expired'})
    return len(ended)


def _end_runs(session, condition, status: str, reason: str | None, conversation_id: int | None = None,
              expected_status: str = 'running') -> list[int]:
    """End the runs in `expected_status` that match `condition` and copy the status to their schedules; the run ids."""
    values = {'status': status, 'reason': reason, 'finished_at': func.now()}
    schedule_values = {'last_run_status': status}
    if conversation_id:
        values['conversation_id'] = conversation_id
        schedule_values['last_conversation_id'] = conversation_id
    ended = session.execute(
        update(EntityScheduleRun)
        .where(EntityScheduleRun.status == expected_status, condition)
        .values(**values)
        .returning(EntityScheduleRun.id, EntityScheduleRun.schedule_id)
    ).all()
    if ended:
        session.execute(
            update(EntitySchedule).where(EntitySchedule.id.in_({schedule_id for _, schedule_id in ended}))
            .values(**schedule_values)
        )
    return [run_id for run_id, _ in ended]


def _announce(project_id: int, session, run_ids: list[int], events: dict = NOTICE_EVENTS) -> None:
    """Send the notice of each ended run to its owner; an agent run also tells the owner's open chats."""
    # Late import: the notice stack is needed only here, so the writers above load without it
    from .schedule_notifications import (
        emit_schedule_run_finished,
        notify_schedule_event,
    )
    for run_id in run_ids:
        run = session.get(EntityScheduleRun, run_id)
        event = events.get(run.status)
        if event is None:  # the status moved on after the commit; that writer owns the notice
            continue
        schedule = session.get(EntitySchedule, run.schedule_id)
        # One failed notice must not leave the other runs of a batch without theirs
        try:
            notify_schedule_event(project_id, schedule.created_by, event, schedule, run)
            if schedule.entity_type == 'agent':
                emit_schedule_run_finished(
                    schedule.created_by, schedule.id, run.id, run.status, schedule.application_id, run.conversation_id)
        except Exception:  # noqa: BLE001 - the run already ended; only its notice is lost
            log.exception('Schedule run %s: cannot send the %s notice', run_id, event)
