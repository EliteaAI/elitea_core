"""The one rule of who may Run now on a schedule: the owner or a holder of `schedules.admin`."""


def may_run(owner_id: int | None, caller_id: int, is_admin: bool) -> bool:
    """True for the owner, or for a caller who holds the permission `schedules.admin` (`scope.is_admin`)."""
    return (owner_id is not None and owner_id == caller_id) or is_admin
