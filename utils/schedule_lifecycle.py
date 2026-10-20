from sqlalchemy import func, update

from ..models.entity_schedule import EntitySchedule


def disable_version_schedules(session, version_ids) -> list[dict]:
    """Disable every schedule of the given versions, paused ones too, and return `id`, `name`, `created_by` of each.

    Runs one UPDATE in the caller's transaction and never commits: a refused or rolled back delete keeps the schedules.
    """
    if not version_ids:
        return []
    statement = (
        update(EntitySchedule)
        .where(EntitySchedule.version_id.in_(version_ids))
        .values(enabled=False, disabled_reason='version_deleted', updated_at=func.now())
        .returning(EntitySchedule.id, EntitySchedule.name, EntitySchedule.created_by)
    )
    return [dict(row) for row in session.execute(statement).mappings().all()]
