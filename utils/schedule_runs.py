"""Run failure and schedule disable writers of the pre-run owner check (EL-7011)."""
from sqlalchemy import update
from tools import db

from ..models.entity_schedule import EntitySchedule
from .schedule_claim import fail_run


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
