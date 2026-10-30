"""Pre-run admission of a schedule run as its owner (EL-7011 US-09, ADR-0002)."""
import json
from queue import Empty
from types import SimpleNamespace
from typing import NamedTuple

from pylon.core.tools import log
from tools import rpc_tools

AUTH_TIMEOUT = 3  # seconds; a check never hangs the starter thread
PRINCIPAL_MISMATCH = 'principal does not match the owner'
OWNER_LOST_ACCESS = 'owner lost access'
ACCESS_CHECK_UNAVAILABLE = 'access check unavailable'
DISABLED_OWNER_LOST_ACCESS = 'owner_lost_access'
NO_TOKEN = 'no token for the owner'
VERSION_NOT_EXPANDED = 'version could not be expanded for the owner'
PRERUN_FAILED = 'the pre-run check failed'
# Error types of application_tools.expand_toolkit_settings
CREDENTIAL_REASONS = {
    'private_credential_not_found': 'private credential {} not found',
    'credential_not_found': 'credential {} not found',
    'credential_type_mismatch': 'credential {} has the wrong type',
}


class MissingCredential(str):
    """The reason text of a missing credential; `credential` is the title of the first one.

    The expansion error names no toolkit; `toolkit` comes from a lookup in the version and may stay None.
    """

    credential: str | None = None
    toolkit: str | None = None


def credential_wait(reason: str | None) -> bool:
    """True when the reason is a missing credential, which the owner can fix; a lost token or a broken version is not."""
    return reason not in (None, NO_TOKEN, VERSION_NOT_EXPANDED)


def credential_errors_of(error) -> list[dict]:
    """The credential entries of an expansion ValidationError: type and name only, never the input values."""
    found = []
    for entry in error.errors(include_input=False):
        try:
            detail = json.loads((entry.get('ctx') or {}).get('error', ''))
        except (TypeError, ValueError):
            continue
        if isinstance(detail, dict) and detail.get('error_type') in CREDENTIAL_REASONS:
            found.append({'error_type': detail['error_type'], 'credential_id': detail.get('credential_id', 'unknown')})
    return found


def check_principal(schedule) -> str | None:
    """None when the row delegates to its owner (R1.3); runs before any auth call."""
    principal = schedule.principal or {}
    if (principal.get('kind') != 'delegation' or schedule.created_by is None
            or principal.get('ref') != schedule.created_by):
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
    except Exception:  # noqa: BLE001 -- any other auth failure must not leave the run running
        log.exception('Owner %s of project %s: the access check failed', owner_id, project_id)
        return ACCESS_CHECK_UNAVAILABLE
    return OWNER_LOST_ACCESS if not user or user.get('suspended') else None


def check_owner_rights(project_id: int, owner_id: int, entity_type: str | None = None,
                       entity_id: int | None = None, verbs: tuple = ('run',)) -> str | None:
    """None when the user holds the schedule permissions `verbs` and may open the entity's folder.

    OWNER_LOST_ACCESS when a permission or the folder access is missing; ACCESS_CHECK_UNAVAILABLE when a
    check fails. No entity skips the folder check. The pre-run step and the owner change share this.
    """
    # Late: the permission table pulls in the auth tools, the folder check the social RPCs
    from .folder_access import entity_access_error
    from .schedule_access import SCHEDULE_PERMISSIONS
    try:
        permissions = rpc_tools.RpcMixin().rpc.timeout(AUTH_TIMEOUT).auth_get_project_user_permissions(
            project_id, owner_id)
        if not all(SCHEDULE_PERMISSIONS[verb] in permissions for verb in verbs):
            return OWNER_LOST_ACCESS
        if entity_id is not None and entity_access_error(project_id, entity_type, entity_id, user_id=owner_id):
            return OWNER_LOST_ACCESS
    except Exception:  # noqa: BLE001 -- a timeout or an RPC error must not read as "no rights"
        log.exception('User %s of project %s: the schedule rights check failed', owner_id, project_id)
        return ACCESS_CHECK_UNAVAILABLE
    return None


def toolkit_using_credential(project_id: int, version_id: int, credential: str) -> str | None:
    """The name of the first toolkit of the version whose settings reference the credential title; None if unknown.

    The expansion error names only the credential, and the owner needs the toolkit to know what to fix.
    Never raises: the name only improves the message.
    """
    try:
        # Late, as the other DB users here: the models pull the whole plugin into every importer
        from tools import db

        from ..models.all import ApplicationVersion
        with db.get_session(project_id) as session:
            version = session.get(ApplicationVersion, version_id)
            for tool in (version.tools if version else []):
                references = (value for value in (tool.settings or {}).values() if isinstance(value, dict))
                if any(str(value.get('elitea_title')) == credential for value in references):
                    return tool.name or tool.type
    except Exception:  # noqa: BLE001 - a lookup failure must not change the pre-run result
        log.exception('Version %s of project %s: the toolkit of credential %s was not found',
                      version_id, project_id, credential)
    return None


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
    errors = expanded.get('credential_errors') or []
    reasons = [CREDENTIAL_REASONS[entry['error_type']].format(entry['credential_id']) for entry in errors]
    if not reasons:
        return VERSION_NOT_EXPANDED
    reason = MissingCredential('; '.join(reasons))
    reason.credential = str(errors[0]['credential_id'])
    reason.toolkit = toolkit_using_credential(project_id, schedule.version_id, reason.credential)
    return reason


def _park(project_id: int, run_id: int, reason: str) -> bool:
    """Make the run wait for the owner to add the credentials; False when it must fail."""
    from .schedule_runs import park_for_credentials  # late, as the writers in run_prerun_check
    return park_for_credentials(project_id, run_id, reason, toolkit=getattr(reason, 'toolkit', None),
                                credential=getattr(reason, 'credential', None))


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
    waits_for_owner = False
    try:
        reason = (check_principal(schedule)
                  or check_owner_access(project_id, owner_id)
                  or check_owner_rights(project_id, owner_id, schedule.entity_type, schedule.application_id))
        if reason is None:
            reason = check_owner_credentials(project_id, schedule)
            # Only a missing credential can wait for the owner; a lost token or a broken version cannot
            waits_for_owner = credential_wait(reason)
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule (R2.5)
        log.exception('Schedule %s run %s: the pre-run check failed', schedule.id, run_id)
        reason = PRERUN_FAILED
    if reason is None:
        return PreRunResult(True, owner_id)
    if waits_for_owner and _park(project_id, run_id, reason):
        return PreRunResult(False, None, reason)
    fail_schedule_run(project_id, run_id, reason)
    event = Types.schedule_run_failed
    if reason == OWNER_LOST_ACCESS:
        disable_schedule(project_id, schedule.id, DISABLED_OWNER_LOST_ACCESS)
        event = Types.schedule_disabled
    run = SimpleNamespace(id=run_id, reason=reason, conversation_id=None)
    notify_schedule_event(project_id, owner_id, event, schedule, run)  # logs its own failure, never raises
    return PreRunResult(False, None, reason)
