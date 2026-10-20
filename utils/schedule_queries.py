"""Schedule list query (EL-7011, US-05)."""
from sqlalchemy import func, or_, select

from ..models.all import Application, ApplicationVersion
from ..models.entity_schedule import EntitySchedule as S
from ..models.entity_schedule import EntityScheduleRun as Run


def list_schedules(session, scope, filters, limit: int, offset: int):
    """Return `(total, rows)` of one page; rows come in `next_run` order, nulls last, then id.

    A caller without admin scope sees only own rows and `owner_ids` is ignored.
    """
    last_reason = (
        select(Run.reason).where(Run.schedule_id == S.id).order_by(Run.started_at.desc(), Run.id.desc()).limit(1)
    ).scalar_subquery()
    conditions = []
    if not scope.is_admin:
        conditions.append(S.created_by == scope.user_id)
    elif filters.owner_ids:
        conditions.append(S.created_by.in_(filters.owner_ids))
    for column, value in (
        (S.entity_type, filters.entity_type), (S.last_run_status, filters.status),
        (S.enabled, filters.enabled), (S.application_id, filters.entity_id),
    ):
        if value is not None:
            conditions.append(column == value)
    if filters.search:
        conditions.append(or_(
            S.name.icontains(filters.search, autoescape=True),
            Application.name.icontains(filters.search, autoescape=True),
        ))
    joined = select(S).join(Application, Application.id == S.application_id).where(*conditions)
    total = session.scalar(select(func.count()).select_from(joined.subquery()))
    page = (
        select(
            S.id, S.name, S.description, S.entity_type, S.run_mode, S.application_id,
            Application.name.label("entity_name"), S.version_id, ApplicationVersion.name.label("version_name"),
            S.cron, S.timezone, S.enabled, S.disabled_reason, S.next_run, S.last_run, S.last_run_status,
            last_reason.label("last_run_reason"), S.last_conversation_id, S.expires_at, S.created_by,
        )
        .join(Application, Application.id == S.application_id)
        .outerjoin(ApplicationVersion, ApplicationVersion.id == S.version_id)
        .where(*conditions)
        .order_by(S.next_run.asc().nulls_last(), S.id)
        .limit(limit).offset(offset)
    )
    return total, session.execute(page).all()
