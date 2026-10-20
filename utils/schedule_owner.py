"""Owner change of a schedule (EL-7011 US-10, ADR-0004)."""
from pylon.core.tools import log
from tools import rpc_tools

from .schedule_access import SCHEDULE_PERMISSIONS
from .schedule_prerun import (
    ACCESS_CHECK_UNAVAILABLE,
    AUTH_TIMEOUT,
    OWNER_LOST_ACCESS,
    check_owner_access,
)

NOT_ACTIVE_MEMBER = 'the new owner is not an active project member'
CANNOT_USE_SCHEDULES = 'the new owner cannot use schedules'


def validate_new_owner(project_id: int, owner_id: int) -> str | None:
    """None when the new owner is an active member with schedules.list (R2.1, R2.2); else the reason.

    ACCESS_CHECK_UNAVAILABLE means the auth check itself failed: the caller answers 503, never 400.
    """
    access = check_owner_access(project_id, owner_id)
    if access is not None:
        return NOT_ACTIVE_MEMBER if access == OWNER_LOST_ACCESS else ACCESS_CHECK_UNAVAILABLE
    try:
        permissions = rpc_tools.RpcMixin().rpc.timeout(AUTH_TIMEOUT).auth_get_project_user_permissions(
            project_id, owner_id)
    except Exception:  # noqa: BLE001 -- a timeout (Empty) or an RPC error must not read as "not eligible"
        log.exception('New owner %s of project %s: the permission check failed', owner_id, project_id)
        return ACCESS_CHECK_UNAVAILABLE
    return None if SCHEDULE_PERMISSIONS['list'] in permissions else CANNOT_USE_SCHEDULES


def change_schedule_owner(session, row, owner_id: int, admin_id: int) -> dict | None:
    """Move created_by and the principal to `owner_id` in one commit (R1.1, R1.2); enabled stays (R1.4).

    Returns the old and new owner ids for the notices, or None when `owner_id` already owns the row.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: When the commit fails; closing the session rolls both fields back.
    """
    old_owner_id = row.created_by
    if old_owner_id == owner_id:
        return None
    row.created_by = owner_id
    row.principal = {'kind': 'delegation', 'ref': owner_id}  # a new dict: JSONB is not mutation-tracked
    row.updated_by = admin_id
    session.commit()
    return {'old_owner_id': old_owner_id, 'new_owner_id': owner_id}


def mark_owner_change_span(schedule) -> None:
    """Name the schedule on the request span, so the tracing trail writes one audit record (R3.1)."""
    from .schedule_service import (
        mark_audit_span,  # late: the service pulls in the models
    )

    mark_audit_span(schedule)
