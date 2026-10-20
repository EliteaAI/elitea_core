"""Pre-run admission of a schedule run as its owner (EL-7011 US-09, ADR-0002)."""
import re
from queue import Empty
from types import SimpleNamespace
from typing import NamedTuple

from tools import rpc_tools

AUTH_TIMEOUT = 3  # seconds; a check never hangs the starter thread
PRINCIPAL_MISMATCH = 'principal does not match the owner'
OWNER_LOST_ACCESS = 'owner lost access'
ACCESS_CHECK_UNAVAILABLE = 'access check unavailable'
DISABLED_OWNER_LOST_ACCESS = 'owner_lost_access'
NO_TOKEN = 'no token for the owner'
VERSION_NOT_EXPANDED = 'version could not be expanded for the owner'
# Error types of application_tools.expand_toolkit_settings; they reach us as JSON inside the error text
CREDENTIAL_REASONS = {
    'private_credential_not_found': 'private credential {} not found',
    'credential_not_found': 'credential {} not found',
    'credential_type_mismatch': 'credential {} has the wrong type',
}
_CREDENTIAL_ERROR = re.compile(r'"error_type": "(\w+)", "credential_id": "([^"]*)"')


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


def check_owner_credentials(project_id: int, schedule) -> str | None:
    """None when a token resolves for the owner with no socket and the version expands as the owner (R2.1-R2.4)."""
    # Imported here: predict_utils pulls flask and the payload models into every importer
    from .predict_utils import PredictPayloadError, get_predict_token_and_session
    from .subagent_prefetch import expand_version_for_runtime

    owner_id = schedule.created_by
    try:
        get_predict_token_and_session(project_id, owner_id, sid=None)
    except PredictPayloadError:
        return NO_TOKEN
    expanded = expand_version_for_runtime(project_id, schedule.application_id, schedule.version_id, owner_id)
    if 'error' not in expanded:
        return None
    match = _CREDENTIAL_ERROR.search(str(expanded['error']))
    if match and match[1] in CREDENTIAL_REASONS:
        return CREDENTIAL_REASONS[match[1]].format(match[2])
    return VERSION_NOT_EXPANDED


class PreRunResult(NamedTuple):
    ok: bool
    owner_id: int | None = None
    reason: str | None = None


def run_prerun_check(project_id: int, schedule, run_id: int) -> PreRunResult:
    """Admit one run as its owner, or fail it and tell the owner (R1.4, R1.5, R2.5, R3.1-R3.3).

    The caller stops on `ok=False`; nothing here or after it falls back to the system user (R3.2).
    """
    # Imported here: the writers and the notice helper pull the models and the event manager into every importer
    from ..models.enums.all import NotificationEventTypes as Types
    from .schedule_notifications import notify_schedule_event
    from .schedule_runs import disable_schedule, fail_schedule_run

    owner_id = schedule.created_by
    reason = (check_principal(schedule)
              or check_owner_access(project_id, owner_id)
              or check_owner_credentials(project_id, schedule))
    if reason is None:
        return PreRunResult(True, owner_id)
    fail_schedule_run(project_id, run_id, reason)
    event = Types.schedule_run_failed
    if reason == OWNER_LOST_ACCESS:
        disable_schedule(project_id, schedule.id, DISABLED_OWNER_LOST_ACCESS)
        event = Types.schedule_disabled
    run = SimpleNamespace(id=run_id, reason=reason, conversation_id=None)
    notify_schedule_event(project_id, owner_id, event, schedule, run)
    return PreRunResult(False, None, reason)
