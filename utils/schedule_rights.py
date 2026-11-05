"""The one rule of who may Run now, approve and reject on a schedule run: the owner or a holder of `schedules.admin`."""


def may_run_or_decide(project_id: int, owner_id: int | None, caller_id: int, is_admin: bool | None = None) -> bool:
    """True for the owner, or for a caller who holds the permission `schedules.admin`.

    An item route passes `scope.is_admin`. The chat path has no scope: with `is_admin` None the permission is read
    for `caller_id` by RPC, so call it with no database session open. A failed read means no (fail closed).
    """
    if owner_id is not None and owner_id == caller_id:
        return True
    if is_admin is not None:
        return is_admin
    # late: the writers of the schedule runs load without the RPC layer
    from .schedule_prerun import check_owner_rights
    return check_owner_rights(project_id, caller_id, verbs=('admin',)) is None
