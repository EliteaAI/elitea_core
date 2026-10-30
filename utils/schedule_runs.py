"""Run status and schedule disable writers of the schedule runs (EL-7011)."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from pylon.core.tools import log
from sqlalchemy import and_, func, or_, select, update
from tools import db

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from .error_text import FAILED_REASON, GENERIC_FAILED_REASON, hide_locations, is_technical, last_exception_line, readable_reason, without_markup
from .schedule_claim import append_run_events, fail_run, finished_event, run_event
from .schedule_timing import at_least_one

STALE_REASON = 'the run did not finish in time'
EXPIRED_REASON = 'approval expired'
EXPIRE_BATCH = 100
NAME_LIMIT = 100  # a step or toolkit name in a run reason, shown to the owner
PAUSE_REASON = 'the run needs a human answer'
# The notice of each final status a notice follows; skipped and blocked runs send none (R4.3)
NOTICE_EVENTS = {'failed': 'schedule_run_failed', 'success': 'schedule_run_succeeded'}
DEFAULT_APPROVAL_WAIT_HOURS = 24
MAX_APPROVAL_WAIT_HOURS = 720
REJECT_REASON = 'rejected by the owner'
# The HITL actions of a decision: `reject` ends a waiting run, the others let it go on (R2.1, R2.2)
DECISION_ACTIONS = ('approve', 'edit', 'answer', 'block_with_comment', 'reject')
# A schedule pauses itself at this many governance refusals in a row (#7254); an admitted start or a resume ends a streak
REFUSAL_PAUSE_AFTER = 3
GOVERNANCE_REFUSED = 'governance_refused'
PAUSED_AFTER_REFUSALS = '{count} runs in a row were refused ({reason}). Fix the mandate and resume it'


class ScheduleDecisionForbidden(Exception):
    """The caller of an approve or reject is not the schedule owner; an admin included (R2.3)."""


class ScheduleDecisionInvalid(Exception):
    """An action of an approve or reject is empty or unknown."""


class ScheduleRunNotWaiting(Exception):
    """The run of an approve or reject is not waiting for approval (R2.4)."""


def approval_wait_hours() -> int:
    """The configured wait of a paused run; a missing, 0, negative or over-720 value is bounded here, as config.yml skips the admin schema."""
    from tools import this  # late: the writers load without the plugin handle
    section = this.module.descriptor.config.get('scheduler', {}).get('schedules', {})
    return at_least_one(section, 'approval_wait_hours', DEFAULT_APPROVAL_WAIT_HOURS, MAX_APPROVAL_WAIT_HOURS)


def has_waiting_runs(project_id: int) -> bool:
    """True when the project has a run that waits for approval; its checkpoint must outlive the cleanup."""
    with db.get_session(project_id) as session:
        return session.scalar(
            select(EntityScheduleRun.id).where(EntityScheduleRun.status == 'waiting_for_approval').limit(1)
        ) is not None


def delete_run_checkpoints(project_id: int, conversation_id: int | None) -> None:
    """Drop the checkpoints of the conversation of a run that ended while it waited; a failure is logged.

    A rejected or expired run is never resumed, so its paused state would stay until the age cleanup.
    """
    if not conversation_id:
        return
    try:
        # Imported here: the writers above load without the chat models and the vector store helper
        from tools import this

        from ..models.conversation import Conversation
        from ..models.message_group import ConversationMessageGroup
        from .vectorstore import get_pgvector_connection_string
        with db.get_session(project_id) as session:
            thread_ids = {str(uuid) for (uuid,) in session.query(Conversation.uuid).filter(
                Conversation.id == conversation_id)}
            thread_ids.update(
                meta['thread_id'] for (meta,) in session.query(ConversationMessageGroup.meta).filter(
                    ConversationMessageGroup.conversation_id == conversation_id,
                    ConversationMessageGroup.meta.isnot(None))
                if isinstance(meta, dict) and meta.get('thread_id')
            )
        this.module.event_node.emit('indexer_delete_checkpoint', {
            'thread_ids': list(thread_ids), 'pgvector_connstr': get_pgvector_connection_string(project_id),
        })
    except Exception:  # noqa: BLE001 -- the run already ended; the age cleanup removes the checkpoints later
        log.exception('Schedule run: cannot delete the checkpoints of conversation %s', conversation_id)


def record_run_events(project_id: int, run_id: int, *events: dict, **columns) -> None:
    """Add events, and set `columns`, on one run in its own transaction; a failure is logged, the run goes on."""
    try:
        with db.get_session(project_id) as session:
            append_run_events(session, run_id, *events, **columns)
            session.commit()
    except Exception:  # noqa: BLE001 -- the log is a view of the run; it must never stop the run
        log.exception('Schedule run %s: cannot write the run log', run_id)


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


def count_refusal(project_id: int, schedule, run_id: int, reason: str) -> None:
    """One more governance refusal of a run of `schedule` (#7254), Run now and Test run included. The
    `REFUSAL_PAUSE_AFTER`-th in a row pauses the schedule (`governance_refused`) in the same transaction and tells
    the owner once. A failure is logged: the refused run stays refused."""
    # Imported here, as in run_prerun_check: the notice helper pulls the event manager into every importer
    from ..models.enums.all import NotificationEventTypes as Types
    from .schedule_notifications import notify_schedule_event
    try:
        with db.get_session(project_id) as session:
            streak = session.execute(
                update(EntitySchedule).where(EntitySchedule.id == schedule.id)
                .values(refusal_streak=func.coalesce(EntitySchedule.refusal_streak, 0) + 1)
                .returning(EntitySchedule.refusal_streak)
            ).scalar()
            if streak == REFUSAL_PAUSE_AFTER:
                session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule.id)
                                .values(enabled=False, disabled_reason=GOVERNANCE_REFUSED))
            session.commit()
    except Exception:  # noqa: BLE001 -- the run is already refused and ended; only the streak is lost
        log.exception('Schedule %s: cannot count the governance refusal of run %s', schedule.id, run_id)
        return
    if streak == REFUSAL_PAUSE_AFTER:
        text = PAUSED_AFTER_REFUSALS.format(count=REFUSAL_PAUSE_AFTER, reason=reason)
        run = SimpleNamespace(id=run_id, reason=text, conversation_id=None)
        notify_schedule_event(project_id, schedule.created_by, Types.schedule_disabled, schedule, run)


def end_refusal_streak(project_id: int, schedule_id: int) -> None:
    """An admitted governed start ends a refusal streak (#7254); writes only a streak there is. A failure is logged."""
    try:
        with db.get_session(project_id) as session:
            session.execute(update(EntitySchedule)
                            .where(EntitySchedule.id == schedule_id, EntitySchedule.refusal_streak > 0)
                            .values(refusal_streak=0))
            session.commit()
    except Exception:  # noqa: BLE001 -- the run goes on; the next refusal counts from the old streak
        log.exception('Schedule %s: cannot end the refusal streak', schedule_id)


REASON_MAX_LENGTH = 300


def run_failure_reason(error) -> str:
    """The stored reason of a failed run: one short plain sentence. An engineer's text (exception class, stack, path) becomes the generic sentence."""
    if not error or not str(error).strip():
        return FAILED_REASON
    if is_technical(error):
        log.warning('A schedule run failed with a technical error: %r', last_exception_line(error))
        return GENERIC_FAILED_REASON
    reason = f'{FAILED_REASON}: {hide_locations(without_markup(readable_reason(last_exception_line(error))))}'
    if len(reason) > REASON_MAX_LENGTH:
        reason = reason[:REASON_MAX_LENGTH - 3] + '...'
    return reason


def _own_conversation(conversation_id: int | None, author_id: int | None) -> list:
    """Conditions that tie a chat hook to its own run: the run is linked to this conversation, or has none yet
    and the chat author owns the schedule. The run id comes from conversation meta, which a client can write.

    The linked conversation is the proof, so a change of the schedule owner does not cut off a running run.
    No author (a server-side caller such as the stale sweep) adds no condition.
    """
    if author_id is None:
        return []
    unlinked = and_(
        EntityScheduleRun.conversation_id.is_(None),
        EntityScheduleRun.schedule_id.in_(select(EntitySchedule.id).where(EntitySchedule.created_by == author_id)),
    )
    if conversation_id is None:
        return [unlinked]
    return [or_(EntityScheduleRun.conversation_id == conversation_id, unlinked)]


def finish_schedule_run(project_id: int, run_id: int, status: str, reason: str | None = None,
                        conversation_id: int | None = None, expected_status: str = 'running',
                        notice: bool = True, author_id: int | None = None) -> bool:
    """End a run in `expected_status` as `success` or `failed`, copy it to its schedule and tell the owner.

    A chat hook passes `author_id`: the run then ends only for the conversation of its own owner.
    False, with nothing changed or sent, when the run is not in `expected_status` any more.
    """
    condition = and_(EntityScheduleRun.id == run_id, *_own_conversation(conversation_id, author_id))
    with db.get_session(project_id) as session:
        ended = _end_runs(session, condition, status, reason, conversation_id, expected_status)
        session.commit()
        if notice:
            _announce(project_id, session, ended)
    return bool(ended)


def pause_schedule_run(project_id: int, run_id: int, interrupt: dict, conversation_id: int | None = None,
                       author_id: int | None = None) -> None:
    """A sensitive-tool pause makes a running run wait for the owner; any other pause fails it.

    `conversation_id` and `author_id` of the paused chat tie the pause to its own run (see `_own_conversation`).
    """
    # A parallel pause carries its interrupts in `hitl_interrupts`
    interrupts = [interrupt.get('hitl_interrupt') or {}, *(interrupt.get('hitl_interrupts') or [])]
    sensitive = next((item for item in interrupts if item.get('guardrail_type') == 'sensitive_tool'), None)
    if sensitive is None:
        finish_schedule_run(project_id, run_id, 'failed', _human_step_reason(interrupt, interrupts),
                            conversation_id, author_id=author_id)
        return
    # Late import, as in _announce
    from .schedule_notifications import notify_schedule_event
    with db.get_session(project_id) as session:
        paused = session.execute(
            update(EntityScheduleRun)
            .where(EntityScheduleRun.id == run_id, EntityScheduleRun.status == 'running',
                   *_own_conversation(conversation_id, author_id))
            # resumed_at is cleared, so a stale fan-out decision of an earlier pause never matches this one
            .values(status='waiting_for_approval', resumed_at=None,
                    approval_expires_at=datetime.now(UTC) + timedelta(hours=approval_wait_hours()))
        ).rowcount
        if paused:
            _copy_status(session, run_id, 'waiting_for_approval')  # the list shows the wait (R1.6)
            append_run_events(session, run_id, _paused_event(sensitive, sum(
                item.get('guardrail_type') == 'sensitive_tool' for item in interrupts) - 1))
        session.commit()
        if not paused:  # a run that is not running stays as it is, with no notice (R1.4)
            return
        try:
            run = session.get(EntityScheduleRun, run_id)
            schedule = session.get(EntitySchedule, run.schedule_id)
            notify_schedule_event(project_id, schedule.created_by, 'schedule_approval_needed', schedule, run)
        except Exception:  # noqa: BLE001 -- the pause is committed; a lost notice must not undo it (R1.3)
            log.exception('Schedule run %s: cannot send the approval notice', run_id)


def _copy_status(session, run_id: int, status: str) -> None:
    """Copy a run status that is not an end to its schedule, so the Schedules list follows the run (R1.6)."""
    schedule_id = select(EntityScheduleRun.schedule_id).where(EntityScheduleRun.id == run_id).scalar_subquery()
    session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule_id).values(last_run_status=status))


def _paused_event(item: dict, more: int = 0) -> dict:
    """The pause line of the run log: toolkit and tool only, never the tool arguments; `more` counts the other tools."""
    toolkit, tool = (str(item.get(key) or '?')[:NAME_LIMIT] for key in ('toolkit_name', 'tool_name'))
    text = f'Paused for approval: {toolkit} · {tool}' + (f' (+{more} more)' if more > 0 else '')
    return run_event('paused', text, 'warning', toolkit=toolkit, tool=tool)


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
    The run log gets the decision; only the owner decides (the check), so the owner is who decided.
    """
    conversation_id = None
    with db.get_session(project_id) as session:
        # The lock holds the run in waiting_for_approval until the commit, so the writes below always apply
        run = session.scalar(
            select(EntityScheduleRun)
            .where(EntityScheduleRun.id == run_id, EntityScheduleRun.status == 'waiting_for_approval')
            .with_for_update()
        )
        if run is None:
            raise ScheduleRunNotWaiting(run_id)
        owner_id = session.scalar(select(EntitySchedule.created_by).where(EntitySchedule.id == run.schedule_id))
        approved = decision == 'approve'
        append_run_events(session, run_id, run_event(
            'decision', f"{'Approved' if approved else 'Rejected'} by the schedule owner",
            'info' if approved else 'warning', action=decision, by=owner_id))
        if approved:
            session.execute(
                update(EntityScheduleRun).where(EntityScheduleRun.id == run_id)
                .values(status='running', resumed_at=func.now(), approval_expires_at=None)
            )
            _copy_status(session, run_id, 'running')  # the list shows the run again (R1.6)
        else:  # no notice: the owner made this end (R2.2)
            _end_runs(session, EntityScheduleRun.id == run_id, 'failed', REJECT_REASON, run.conversation_id,
                      expected_status='waiting_for_approval')
            conversation_id = run.conversation_id
        session.commit()
    delete_run_checkpoints(project_id, conversation_id)


def record_skipped_run(session, schedule_id: int, reason: str) -> int:
    """Insert a finished `skipped` cron run without a conversation and copy the status; the run id."""
    run = EntityScheduleRun(
        schedule_id=schedule_id, trigger='cron', status='skipped', reason=reason, finished_at=func.now(),
        events=[finished_event('skipped', reason)])
    session.add(run)
    session.flush()
    session.execute(update(EntitySchedule).where(EntitySchedule.id == schedule_id).values(last_run_status='skipped'))
    return run.id


def block_schedule_run(project_id: int, run_id: int, reason: str, level: str | None = None) -> None:
    """End a running run as `blocked` with `reason` and copy the status; no notice.

    `level` overrides the level of the `finished` event: a blocking risk finding is an `error`.
    """
    with db.get_session(project_id) as session:
        _end_runs(session, EntityScheduleRun.id == run_id, 'blocked', reason, level=level)
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
        conversation_ids = [session.get(EntityScheduleRun, run_id).conversation_id for run_id in ended]
    for conversation_id in conversation_ids:
        delete_run_checkpoints(project_id, conversation_id)
    return len(ended)


def _end_runs(session, condition, status: str, reason: str | None, conversation_id: int | None = None,
              expected_status: str = 'running', level: str | None = None) -> list[int]:
    """End the runs in `expected_status` that match `condition`, copy the status to their schedules and log the end.

    Returns the ids of the ended runs.
    """
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
    # lean: one locked read per ended run; a sweep ends at most one batch (EXPIRE_BATCH); batch the read if it grows
    for run_id, _ in ended:
        append_run_events(session, run_id, finished_event(status, reason, level))
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
