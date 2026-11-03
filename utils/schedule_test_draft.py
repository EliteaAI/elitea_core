"""The hidden draft row that holds the runs of "Test" before a schedule is saved.

A run needs a schedule row (the run table points to one, and the run starter reads its prompt and version there).
A draft is that row: `draft` true, never enabled, no `next_run`, no `last_run`. There is one per owner and agent or
pipeline, and each test run rewrites it from the form. Every other reader of schedules skips it (`not_draft`).
"""
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, exists, func, select
from sqlalchemy.exc import IntegrityError

from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from ..models.pd.entity_schedule import ScheduleCreate

IDLE_HOURS = 24
ACTIVE = ('running', 'waiting_for_approval')
# A draft with runs is never deleted and every click on Test adds a run, so the table would grow without end.
# The newest runs are the ones Run History shows; an older run is no use to the owner.
KEEP_TEST_RUNS = 50


def find_draft(session, owner_id: int, application_id: int) -> EntitySchedule | None:
    return session.scalars(select(EntitySchedule).where(
        EntitySchedule.draft.is_(True), EntitySchedule.created_by == owner_id,
        EntitySchedule.application_id == application_id)).one_or_none()


def active_run_id(session, schedule_id: int) -> int | None:
    return session.scalar(select(EntityScheduleRun.id).where(
        EntityScheduleRun.schedule_id == schedule_id, EntityScheduleRun.status.in_(ACTIVE)))


def write_draft(session, owner_id: int, body: ScheduleCreate) -> EntitySchedule:
    """Store the form values in the owner's draft row of this entity, creating the row on the first test run.

    The row never gets a stop date, a next run or a risk confirmation: a test run asks as an unconfirmed schedule.
    Two first test runs at once both find no row; the unique index lets one insert win. The other reuses the winner.
    """
    values = body.model_dump(exclude={'risk_ack', 'enabled'})
    row = find_draft(session, owner_id, body.application_id)
    if row is None:
        row = _fill(EntitySchedule(draft=True, created_by=owner_id), owner_id, values)
        try:
            with session.begin_nested():  # a savepoint: the conflict undoes this insert only
                session.add(row)
        except IntegrityError:
            row = find_draft(session, owner_id, body.application_id)
            if row is None:
                raise  # the index fired for another reason than a draft of this owner and entity
    _fill(row, owner_id, values)
    session.flush()
    return row


def _fill(row: EntitySchedule, owner_id: int, values: dict) -> EntitySchedule:
    for name, value in values.items():
        setattr(row, name, value)
    row.enabled = False
    row.next_run = row.expires_at = None
    row.updated_by = owner_id
    row.principal = {'kind': 'delegation', 'ref': owner_id}  # a new dict: JSONB is not mutation-tracked
    row.approval_confirmed_by = row.approval_confirmed_at = row.approval_fingerprint = None
    row.updated_at = func.now()  # an edit with equal values would otherwise keep the old idle time
    return row


def trim_test_runs(session, schedule_id: int) -> int:
    """Delete the runs of the draft beyond the newest `KEEP_TEST_RUNS`; an active run stays. The number deleted."""
    old = (select(EntityScheduleRun.id).where(EntityScheduleRun.schedule_id == schedule_id)
           .order_by(EntityScheduleRun.started_at.desc(), EntityScheduleRun.id.desc()).offset(KEEP_TEST_RUNS))
    result = session.execute(delete(EntityScheduleRun).where(
        EntityScheduleRun.id.in_(old), EntityScheduleRun.status.not_in(ACTIVE)))
    return result.rowcount


def drop_draft(session, owner_id: int, application_id: int) -> bool:
    """Delete the owner's draft of this entity when it has no run; True when a row was deleted.

    A draft with runs stays: its runs are the test records of Run History, and they are deleted with their
    schedule row. The row is hidden and there is one per owner and entity, so it does not grow.
    """
    return _delete_unused(session, EntitySchedule.created_by == owner_id,
                          EntitySchedule.application_id == application_id) > 0


def sweep_idle_drafts(session, owner_id: int) -> int:
    """Delete this owner's drafts that no one used for a day and that have no run; the number deleted.

    The sweep runs inside the owner's next test run, so it needs no periodic task. A form that is never tested
    again leaves its draft until then, and a draft is invisible, so nothing else reads it.
    """
    idle_before = datetime.now(UTC) - timedelta(hours=IDLE_HOURS)  # a day is far above any clock skew
    return _delete_unused(session, EntitySchedule.created_by == owner_id, EntitySchedule.updated_at < idle_before)


def _delete_unused(session, *conditions) -> int:
    has_run = exists().where(EntityScheduleRun.schedule_id == EntitySchedule.id)
    result = session.execute(delete(EntitySchedule).where(EntitySchedule.draft.is_(True), ~has_run, *conditions))
    return result.rowcount
