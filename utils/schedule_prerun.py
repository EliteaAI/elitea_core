"""Pre-run admission of a schedule run as its owner (EL-7011 US-09, ADR-0002)."""
from queue import Empty

from tools import rpc_tools

AUTH_TIMEOUT = 3  # seconds; a check never hangs the starter thread
PRINCIPAL_MISMATCH = 'principal does not match the owner'
OWNER_LOST_ACCESS = 'owner lost access'
ACCESS_CHECK_UNAVAILABLE = 'access check unavailable'


def check_principal(schedule) -> str | None:
    """None when the row delegates to its owner (R1.3); runs before any auth call."""
    principal = schedule.principal or {}
    if principal.get('kind') != 'delegation' or principal.get('ref') != schedule.created_by:
        return PRINCIPAL_MISMATCH
    return None


def check_owner_access(project_id: int, owner_id: int) -> str | None:
    """None when the owner is a project member who is not suspended (R1.1, R1.2); else the run reason."""
    rpc = rpc_tools.RpcMixin().rpc
    try:
        if not rpc.timeout(AUTH_TIMEOUT).auth_check_user_in_project(project_id, owner_id):
            return OWNER_LOST_ACCESS
        user = rpc.timeout(AUTH_TIMEOUT).auth_get_user(user_id=owner_id)
    except RuntimeError as error:
        # auth_core wraps every error in RuntimeError; only a missing record means lost access
        return OWNER_LOST_ACCESS if str(error).startswith('No such user found') else ACCESS_CHECK_UNAVAILABLE
    except Empty:  # the RPC timed out: keep the schedule enabled
        return ACCESS_CHECK_UNAVAILABLE
    return OWNER_LOST_ACCESS if user.get('suspended') else None
