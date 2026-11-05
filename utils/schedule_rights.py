"""The one rule of who may Run now, Test run, approve and reject on a schedule run: the owner or a project admin."""
from pylon.core.tools import log

ADMIN_CHECK_TIMEOUT_S = 3


def is_project_admin(project_id: int, user_id: int) -> bool:
    """True when the user holds the admin role of the project. Fail closed: an RPC error is logged and means no."""
    try:
        from tools import rpc_tools  # late: the writers of the schedule runs load without the RPC layer
        return bool(rpc_tools.RpcMixin().rpc.timeout(ADMIN_CHECK_TIMEOUT_S).admin_check_user_is_admin(project_id, user_id))
    except Exception:  # noqa: BLE001 -- a lookup failure must never widen rights
        log.exception("Schedule rights: cannot read the admin role of user %s in project %s", user_id, project_id)
        return False


def may_run_or_decide(project_id: int, owner_id: int | None, caller_id: int) -> bool:
    """True for the owner, or for a project admin. The admin lookup is an RPC and runs only for a caller who is
    not the owner, so call this with no database session open."""
    if owner_id is not None and owner_id == caller_id:
        return True
    return is_project_admin(project_id, caller_id)
