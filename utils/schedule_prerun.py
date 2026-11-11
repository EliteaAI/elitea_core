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
LOGIN_LOST = 'the saved login of credential {} stopped working; the triggers using it were switched off'
# Outages neither count nor end a streak; a lost owner or saved login switches the row off at once


class LoginLost(str):
    """The reason text of an expired or removed saved login; the triggers using it are already off."""


UNCOUNTED_PRERUN = frozenset({ACCESS_CHECK_UNAVAILABLE, PRERUN_FAILED, OWNER_LOST_ACCESS})
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


def is_webhook(schedule) -> bool:
    # A Test Run's namespace has no trigger_kind; it is never a webhook
    return getattr(schedule, 'trigger_kind', None) == 'webhook'


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


def credential_toolkit_refs(version) -> list:
    """The (toolkit name, credential titles) of each toolkit of a loaded version, as plain values.

    The run starter reads the version once; this keeps the toolkit lookup of a missing credential
    from opening a second database session.
    """
    return [(tool.name or tool.type,
             {str(value.get('elitea_title')) for value in (tool.settings or {}).values() if isinstance(value, dict)})
            for tool in version.tools]


def toolkit_using_credential(toolkits, credential: str) -> str | None:
    """The name of the first toolkit whose settings reference the credential title; None if unknown.

    The expansion error names only the credential, and the owner needs the toolkit to know what to fix.
    """
    return next((name for name, titles in toolkits or [] if credential in titles), None)


# Runs whose owner approval already passed the credential check in the request; the restart skips the repeat
_CHECKED_RUNS: set = set()


def mark_credentials_checked(run_id: int) -> None:
    _CHECKED_RUNS.add(run_id)


def forget_credentials_checked(run_id: int) -> None:
    _CHECKED_RUNS.discard(run_id)


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
        return check_saved_logins(project_id, schedule, expanded)
    errors = expanded.get('credential_errors') or []
    reasons = [CREDENTIAL_REASONS[entry['error_type']].format(entry['credential_id']) for entry in errors]
    if not reasons:
        return VERSION_NOT_EXPANDED
    reason = MissingCredential('; '.join(reasons))
    reason.credential = str(errors[0]['credential_id'])
    reason.toolkit = toolkit_using_credential(getattr(schedule, 'credential_toolkits', None), reason.credential)
    return reason


def check_saved_logins(project_id: int, schedule, expanded: dict) -> str | None:
    """None, or `LoginLost` after switching off the triggers on an expired or removed saved login (#7401)."""
    # A transient refresh error lets the run go on: the predict step tries the login again
    if getattr(schedule, 'id', None) is None:  # a Test Run: no trigger to switch off; the run reports the sign-in
        return None
    from .stored_oauth_tokens import UNATTENDED_LOGIN_TYPES
    seen = set()
    tools = expanded.get('tools') or []
    if not any(key in UNATTENDED_LOGIN_TYPES for tool in tools for key, _ in _credentials(tool)) \
            and not any((tool or {}).get('type') == 'application' for tool in tools):
        return None
    for tool in _runtime_tools(project_id, expanded, schedule.created_by):
        for key, credential in _credentials(tool):
            uuid_ = credential['configuration_uuid']
            if key not in UNATTENDED_LOGIN_TYPES or uuid_ in seen:
                continue
            seen.add(uuid_)
            row = _connection_row(project_id, schedule.created_by, {**tool, 'settings': {key: credential}}, True)
            # A login never saved is not "lost": a schedule may wait for it (#7304); a webhook was refused at save
            if row.get('login') == LOGIN_EXPIRED or (row.get('login') == LOGIN_GONE and _was_tracked(schedule, uuid_)):
                return switch_off_for_login(project_id, schedule.created_by, row, schedule.id)
    return None


def _was_tracked(schedule, credential_uuid: str) -> bool:
    # A webhook saves only with every login present, and a refreshed login sets the refs: a gone one was removed
    return is_webhook(schedule) or credential_uuid in (getattr(schedule, 'credential_refs', None) or ())


def switch_off_for_login(project_id: int, owner_id: int, row: dict, also_id: int | None = None) -> str:
    """Switch off every trigger of the owner using the row's credential; the run reason naming it."""
    from .schedule_runs import disable_by_credential  # late, as the writers in run_prerun_check
    title = row.get('credential_title') or row['credential_uuid']
    disable_by_credential(project_id, owner_id, row['credential_uuid'], CREDENTIAL_EXPIRED, title,
                          bool(row.get('private')), also_id)
    return LoginLost(LOGIN_LOST.format(title))


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
        # A mark in _CHECKED_RUNS means the owner's approval just ran the credential check
        if reason is None and run_id not in _CHECKED_RUNS:
            reason = check_owner_credentials(project_id, schedule)
            # Only a missing credential can wait for the owner; a lost token or a broken version cannot
            # A broken entity fails a webhook run; its body is not kept, so an approval could not restart it (#7454)
            waits_for_owner = credential_wait(reason) and not is_webhook(schedule) and not isinstance(reason, LoginLost)
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule (R2.5)
        log.exception('Schedule %s run %s: the pre-run check failed', schedule.id, run_id)
        reason = PRERUN_FAILED
    finally:
        _CHECKED_RUNS.discard(run_id)  # the mark serves this one start, whatever way the checks end
    if reason is None:
        return PreRunResult(True, owner_id)
    if waits_for_owner and _park(project_id, run_id, reason):
        return PreRunResult(False, None, reason)
    fail_schedule_run(project_id, run_id, reason)
    if schedule.id is None:  # a Test Run has no schedule to disable and no schedule page to link a notice to
        return PreRunResult(False, None, reason)
    event = Types.schedule_run_failed
    if reason == OWNER_LOST_ACCESS:
        disable_schedule(project_id, schedule.id, DISABLED_OWNER_LOST_ACCESS)
        event = Types.schedule_disabled
    if isinstance(reason, LoginLost):  # the switch-off already sent the one notice naming the credential
        return PreRunResult(False, None, reason)
    run = SimpleNamespace(id=run_id, reason=reason, conversation_id=None)
    notify_schedule_event(project_id, owner_id, event, schedule, run)  # logs its own failure, never raises
    # A webhook fires on every delivery: a lasting failure switches it off at the third in a row (#7454)
    if is_webhook(schedule) and reason not in UNCOUNTED_PRERUN and not isinstance(reason, LoginLost):
        from .schedule_runs import PAUSED_AFTER_PRERUN, PRERUN_REFUSED, count_refusal
        count_refusal(project_id, schedule, run_id, reason, disabled_reason=PRERUN_REFUSED, text=PAUSED_AFTER_PRERUN)
    return PreRunResult(False, None, reason)


# --- Connections check (#7401): can each toolkit of a version run with no person signed in -------------------------

CONNECTION_OK, CONNECTION_SIGN_IN, CONNECTION_UNSUPPORTED = 'ok', 'sign_in_required', 'unsupported'
LOGIN_OK, LOGIN_EXPIRED, LOGIN_GONE = 'ok', 'expired', 'gone'
LOGIN_TRANSIENT, LOGIN_UNSUPPORTED = 'transient', 'unsupported'
CREDENTIAL_EXPIRED = 'credential_expired'
CONNECTIONS_CHANGED = 'connections_changed'
NO_SAVED_LOGIN = 'no saved login: sign in on the credential'
LOGIN_EXPIRED_TEXT = 'the saved login stopped working: sign in again'
LOGIN_TRANSIENT_TEXT = 'the saved login could not be checked now; try again'
LOGIN_UNSUPPORTED_TEXT = 'this credential cannot keep a saved login'
# Shared with the risk scan, so one condition has one wording
BROWSER_ONLY_TEXT = 'Webhooks cannot wait for a sign-in; use a toolkit with a saved login.'
DELEGATED_TEXT = "acts with the signed-in person's Microsoft account; unattended runs cannot use it"
LOGIN_TEXTS = {LOGIN_EXPIRED: LOGIN_EXPIRED_TEXT, LOGIN_GONE: NO_SAVED_LOGIN, LOGIN_TRANSIENT: LOGIN_TRANSIENT_TEXT,
               LOGIN_UNSUPPORTED: LOGIN_UNSUPPORTED_TEXT}


def check_stored_login(project_id: int, owner_id: int, settings_key: str, configuration_uuid: str,
                       refresh: bool = True) -> str:
    """A real refresh of the login saved on the server, through the run's own code; the token is cached for the run."""
    from .mcp_oauth import refresh_token  # late: pulls in requests
    from .stored_oauth_tokens import LOGIN_RESOLVERS, StoredLoginExpired, get_access_token
    credential_type = settings_key.removesuffix('_configuration')
    try:
        login_data = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_stored_login(
            project_id=project_id, user_id=owner_id, configuration_uuid=configuration_uuid,
            expected_type=credential_type)
    except Exception:  # noqa: BLE001 -- a lookup outage is never read as a logout
        log.exception('Stored login of credential %s: the lookup failed', configuration_uuid)
        return LOGIN_TRANSIENT
    if not login_data or not login_data.get('refresh_token'):
        return LOGIN_GONE
    login = LOGIN_RESOLVERS[settings_key](login_data)
    if login is None:
        return LOGIN_UNSUPPORTED
    if not refresh:
        return LOGIN_OK
    from tools import this  # pylint: disable=C0415
    try:
        token = get_access_token(this.module.get_redis_client(), login_data, login, refresh_token)
    except StoredLoginExpired:
        return LOGIN_EXPIRED
    return LOGIN_OK if token else LOGIN_TRANSIENT


def _browser_only(tool: dict) -> bool:
    """A toolkit that signs in only through a browser: remote MCP, OpenAPI with OAuth."""
    type_, settings = tool.get('type') or '', tool.get('settings') or {}
    if type_ == 'mcp' or type_.startswith('mcp_') or (tool.get('meta') or {}).get('mcp'):
        from .internal_tools import _INTERNAL_MCP_ENDPOINT_RE  # pylint: disable=C0415
        # the platform's own MCP servers authenticate with the run token, no person
        return not _INTERNAL_MCP_ENDPOINT_RE.search(str(settings.get('url') or ''))
    if type_ == 'openapi':
        return any(isinstance(value, dict) and value.get('oauth_discovery_endpoint') for value in settings.values()) \
            or bool(settings.get('oauth_discovery_endpoint'))
    return False


def _credentials(tool: dict) -> list:
    """(settings key, expanded credential) of each credential reference of a toolkit."""
    return [(key, value) for key, value in (tool.get('settings') or {}).items()
            if isinstance(value, dict) and value.get('configuration_uuid')]


def _connection_row(project_id: int, owner_id: int, tool: dict, refresh: bool) -> dict:
    from .credential_decision import DELEGATED_AUTH_MODE  # pylint: disable=C0415
    from .stored_oauth_tokens import UNATTENDED_LOGIN_TYPES  # pylint: disable=C0415
    name = tool.get('name') or tool.get('toolkit_name') or tool.get('type')
    row = {'toolkit': name, 'toolkit_type': tool.get('type'), 'status': CONNECTION_OK, 'reason': None}
    credentials = _credentials(tool)
    for key, credential in credentials:
        row.update(credential_uuid=credential['configuration_uuid'], credential_type=key.removesuffix('_configuration'),
                   private=credential.get('private') is True, credential_title=credential.get('elitea_title'))
        if credential.get('auth_mode') == DELEGATED_AUTH_MODE:
            return {**row, 'status': CONNECTION_UNSUPPORTED, 'reason': f'Toolkit "{name}" {DELEGATED_TEXT}'}
        login_field = 'tenant_id' if key == 'dataverse_configuration' else 'oauth_discovery_endpoint'
        if key in UNATTENDED_LOGIN_TYPES and credential.get(login_field):
            state = check_stored_login(project_id, owner_id, key, credential['configuration_uuid'], refresh)
            if state != LOGIN_OK:
                status = CONNECTION_UNSUPPORTED if state == LOGIN_UNSUPPORTED else CONNECTION_SIGN_IN
                return {**row, 'status': status, 'reason': LOGIN_TEXTS[state], 'login': state}
            row['login'] = LOGIN_OK
    if _browser_only(tool):
        return {**row, 'status': CONNECTION_UNSUPPORTED, 'reason': BROWSER_ONLY_TEXT}
    return row


def _runtime_tools(project_id: int, version: dict, owner_id: int) -> list:
    """The toolkits of the version and of its same-project sub-agents, as a run receives them."""
    from .stored_oauth_tokens import collect_payload_tools  # pylint: disable=C0415
    from .subagent_prefetch import collect_subagent_version_details  # pylint: disable=C0415
    subagents = collect_subagent_version_details(project_id, version.get('tools'), owner_id)
    payload = {'tools': version.get('tools') or [], 'application': {'subagent_version_details': subagents}}
    return [tool for tool in collect_payload_tools(payload)
            if isinstance(tool, dict) and tool.get('type') != 'application']


def check_connections(project_id: int, application_id: int, version_id: int, owner_id: int,
                      refresh: bool = True) -> dict:
    """`rows` (ok, sign_in_required or unsupported per toolkit) and `credential_refs`; never raises."""
    # refresh=False skips the token calls (a version save)
    from .subagent_prefetch import expand_version_for_runtime  # pylint: disable=C0415
    try:
        expanded = expand_version_for_runtime(project_id, application_id, version_id, owner_id)
        if 'error' in expanded:
            errors = expanded.get('credential_errors') or []
            rows = [{'toolkit': None, 'credential_title': str(entry['credential_id']), 'status': CONNECTION_SIGN_IN,
                     'reason': CREDENTIAL_REASONS[entry['error_type']].format(entry['credential_id'])}
                    for entry in errors] or [{'toolkit': None, 'status': CONNECTION_SIGN_IN,
                                              'reason': VERSION_NOT_EXPANDED}]
            return {'rows': rows, 'credential_refs': None}
        tools = _runtime_tools(project_id, expanded, owner_id)
        rows = [_connection_row(project_id, owner_id, tool, refresh) for tool in tools]
        refs = sorted({credential['configuration_uuid'] for tool in tools for _, credential in _credentials(tool)})
        return {'rows': rows, 'credential_refs': refs}
    except Exception:  # noqa: BLE001 -- the panel shows a retry, a save stays blocked
        log.exception('Connections check of version %s failed', version_id)
        return {'rows': [{'toolkit': None, 'status': CONNECTION_SIGN_IN, 'reason': PRERUN_FAILED}],
                'credential_refs': None}


def connections_ready(result: dict) -> bool:
    return all(row['status'] == CONNECTION_OK for row in result['rows'])


def lost_logins(result: dict) -> list:
    """The rows whose saved login expired or was removed: these switch the owner's triggers off."""
    return [row for row in result['rows']
            if row.get('login') in (LOGIN_EXPIRED, LOGIN_GONE) and row.get('credential_uuid')]


def recheck_version_triggers(project_id: int, version_id: int) -> None:
    """After a version save: refresh the triggers' `credential_refs`; switch off a webhook no longer ready."""
    # Static (no token call) and best effort: a failure is logged, never fails the save
    from ..models.entity_schedule import EntitySchedule  # pylint: disable=C0415
    from ..models.enums.all import NotificationEventTypes as Types  # pylint: disable=C0415
    from .schedule_notifications import notify_schedule_event  # pylint: disable=C0415
    from tools import db  # pylint: disable=C0415
    try:
        with db.get_session(project_id) as session:
            rows = session.query(EntitySchedule.id, EntitySchedule.application_id, EntitySchedule.created_by,
                                 EntitySchedule.trigger_kind, EntitySchedule.name).filter(
                EntitySchedule.version_id == version_id, EntitySchedule.enabled.is_(True)).all()
        for row in rows:
            result = check_connections(project_id, row.application_id, version_id, row.created_by, refresh=False)
            if result['credential_refs'] is None:  # the version did not expand: an outage, nothing is known
                continue
            broken = [item for item in result['rows'] if item['status'] == CONNECTION_UNSUPPORTED]
            switch_off = row.trigger_kind == 'webhook' and bool(broken)
            values = {} if result['credential_refs'] is None else {'credential_refs': result['credential_refs']}
            if switch_off:
                values.update(enabled=False, disabled_reason=CONNECTIONS_CHANGED, next_run=None)
            if values:
                with db.get_session(project_id) as session:
                    session.query(EntitySchedule).filter(EntitySchedule.id == row.id,
                                                         EntitySchedule.enabled.is_(True)).update(values)
                    session.commit()
            if switch_off:
                reason = broken[0].get('reason') or 'a toolkit cannot run without a person signed in'
                notify_schedule_event(project_id, row.created_by, Types.schedule_disabled, row,
                                      extra={'reason': f'the agent changed: {reason}'})
    except Exception:  # noqa: BLE001 -- the version is saved; the next run or check catches the rest
        log.exception('Version %s: the triggers using it could not be checked again', version_id)


def recheck_version_triggers_later(project_id: int, version_id: int | None) -> None:
    import threading  # pylint: disable=C0415
    if not version_id:
        return
    threading.Thread(target=recheck_version_triggers, args=(project_id, version_id),
                     name='trigger-connections-recheck', daemon=True).start()


def _recheck_toolkit_triggers(project_id: int, toolkit_id: int) -> None:
    from ..models.elitea_tools import EntityToolMapping  # pylint: disable=C0415
    from ..models.entity_schedule import EntitySchedule  # pylint: disable=C0415
    from tools import db  # pylint: disable=C0415
    try:
        with db.get_session(project_id) as session:
            version_ids = [row[0] for row in session.query(EntityToolMapping.entity_version_id).join(
                EntitySchedule, EntitySchedule.version_id == EntityToolMapping.entity_version_id).filter(
                EntityToolMapping.tool_id == toolkit_id, EntitySchedule.enabled.is_(True)).distinct().all()]
    except Exception:  # noqa: BLE001 -- the toolkit is saved; the next run or check catches the rest
        log.exception('Toolkit %s: the versions using it could not be read', toolkit_id)
        return
    for version_id in version_ids:
        recheck_version_triggers(project_id, version_id)


def recheck_toolkit_triggers_later(project_id: int, toolkit_id: int) -> None:
    """A toolkit edit or link: one background pass over the versions using it that have an enabled trigger."""
    import threading  # pylint: disable=C0415
    threading.Thread(target=_recheck_toolkit_triggers, args=(project_id, toolkit_id),
                     name='trigger-connections-recheck', daemon=True).start()
