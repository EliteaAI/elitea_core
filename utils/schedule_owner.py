"""Owner change of a schedule (EL-7011 US-10, ADR-0004)."""
from .schedule_prerun import (
    ACCESS_CHECK_UNAVAILABLE,
    OWNER_LOST_ACCESS,
    check_owner_access,
    check_owner_rights,
)

NOT_ACTIVE_MEMBER = 'the new owner is not an active project member'
CANNOT_USE_SCHEDULES = 'the new owner cannot use schedules'


def validate_new_owner(project_id: int, owner_id: int, entity_type: str | None = None,
                       application_id: int | None = None) -> str | None:
    """None when the new owner is an active member who may list and run schedules and open the entity.

    ACCESS_CHECK_UNAVAILABLE means the auth check itself failed: the caller answers 503, never 400.
    """
    access = check_owner_access(project_id, owner_id)
    if access is not None:
        return NOT_ACTIVE_MEMBER if access == OWNER_LOST_ACCESS else ACCESS_CHECK_UNAVAILABLE
    rights = check_owner_rights(project_id, owner_id, entity_type, application_id, verbs=('list', 'run'))
    if rights is None:
        return None
    return CANNOT_USE_SCHEDULES if rights == OWNER_LOST_ACCESS else ACCESS_CHECK_UNAVAILABLE


def change_schedule_owner(session, row, owner_id: int, admin_id: int) -> dict | None:
    """Move created_by and the principal to `owner_id` in one commit (R1.1, R1.2); enabled stays (R1.4).

    The approval rule goes back to "Pause and ask me", the confirmation is cleared
    and the delivery goes back to Run History.

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
    # The consent belongs to the old owner: the new owner confirms again before a run approves by itself
    row.approval_rule = 'pause_and_ask'
    row.approval_fingerprint = row.approval_confirmed_by = row.approval_confirmed_at = None
    # The chat target was checked against the old owner's membership: the new owner starts with Run History only
    row.delivery_mode = 'run_history'
    row.delivery_conversation_uuid = row.delivery_participant_id = None
    row.delivery_include_history = 0
    session.commit()
    return {'old_owner_id': old_owner_id, 'new_owner_id': owner_id}


def mark_owner_change_span(schedule) -> None:
    """Name the schedule on the request span, so the tracing trail writes one audit record (R3.1)."""
    from .schedule_service import (
        mark_audit_span,  # late: the service pulls in the models
    )

    mark_audit_span(schedule)
