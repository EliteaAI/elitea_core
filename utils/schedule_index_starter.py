"""Starter of a Toolkit Index schedule run, and the close of that run when its index task ends."""
from types import SimpleNamespace

from pylon.core.tools import log
from sqlalchemy import and_, or_, select
from tools import VaultClient, db, this

from ..models.elitea_tools import EliteATool
from ..models.entity_schedule import RUN_FORM, EntitySchedule, EntityScheduleRun
from ..models.enums.all import InitiatorType
from ..models.indexer import EmbeddingStore
from .application_tools import (
    DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC,
    IndexRunInProgressError,
    get_pending_index_run_heartbeat,
    get_session_for_schema,
    resolve_index_staleness,
    start_index_task,
)
from .budget_door import closed_budget_scope
from .exceptions import AdmissionDenied, PoolSaturationError
from .governance import is_auditing, is_governed
from .index_scheduling import resolve_index_llm_settings
from .predict_utils import PredictPayloadError, get_predict_base_url, get_predict_token_and_session, get_toolkit_config
from .schedule_prerun import NO_TOKEN, run_prerun_check
from .schedule_run_starter import BUDGET_CLOSED, NOT_FOUND, START_FAILED
from .schedule_runs import (
    _end_runs,
    block_schedule_run,
    count_refusal,
    fail_schedule_run,
    finish_schedule_run,
    record_run_events,
    run_failure_reason,
)
from .schedule_claim import run_event

TOOLKIT_CONFIG_FAILED = 'the toolkit settings could not be read'
CREDENTIAL_MISSING = 'the toolkit credential or its vector store connection is missing'
INDEX_UNAVAILABLE = 'the index could not be read'
INDEX_NOT_SCHEDULABLE = 'the index has no saved settings to run with'
INDEX_RUN_ACTIVE = 'a run of this index is active'
LLM_UNAVAILABLE = 'no LLM model could be used for indexing'
POOL_SATURATED = 'no indexer worker was free; the next run tries again'
PARTLY_INDEXED = 'partly indexed'
CANCELLED = 'the index run was cancelled'
INDEX_TASK = 'index_task'
# The index states that end a run; `created` and `in_progress` are not in it, the stale sweep closes a run they leave open
CLOSING_STATES = {
    'completed': ('success', None),
    'scheduled_reindex': ('success', None),
    'partly_indexed': ('success', PARTLY_INDEXED),
    'failed': ('failed', None),
    'cancelled': ('failed', None),
}


class _Ended(Exception):
    """A step ended the run without a dispatch: ``status`` is ``failed`` or ``skipped``."""

    def __init__(self, status: str, reason: str):
        self.status, self.reason = status, reason
        super().__init__(reason)


def _load(project_id: int, run_id: int) -> SimpleNamespace | None:
    """Plain values of the schedule row, or of the form of a Test run, and the run trigger; None when any target is gone."""
    with db.get_session(project_id) as session:
        run_row = session.get(EntityScheduleRun, run_id)
        if run_row is None:
            return None
        if run_row.schedule_id is None:  # a Test run: the target is in the form stored on the run
            form = session.execute(select(RUN_FORM).where(EntityScheduleRun.id == run_id)).scalar() or {}
            owner_id = run_row.requested_by
            target = SimpleNamespace(
                id=None, name=form.get('name'), toolkit_id=form.get('toolkit_id'), index_name=form.get('index_name'),
                entity_type='toolkit_index', created_by=owner_id, principal={'kind': 'delegation', 'ref': owner_id})
        else:
            schedule = session.get(EntitySchedule, run_row.schedule_id)
            if schedule is None:
                return None
            target = SimpleNamespace(
                id=schedule.id, name=schedule.name, toolkit_id=schedule.toolkit_id, index_name=schedule.index_name,
                entity_type='toolkit_index', created_by=schedule.created_by, principal=schedule.principal)
        if target.toolkit_id is None or not target.index_name or session.get(EliteATool, target.toolkit_id) is None:
            return None
        target.trigger = run_row.trigger
        return target


def _started_event(target) -> dict:
    label = 'Test run' if target.trigger == 'test' else 'Run'
    return run_event('started', f'{label} started · trigger: {target.trigger} · index {target.index_name}',
                     trigger=target.trigger)


def _cron_admission(project_id: int, run_id: int, target, governed: bool):
    """The mandate admission of a cron firing, or None; ends the run (raises `_Ended`) when enforce refuses it.

    Audit only: a firing enforce would refuse leaves one would-deny and goes on, so the second value is its record.
    """
    # Imported here: only a governed or auditing cron firing reads mandates
    from .admission import (
        index_schedule_refusal_code,
        index_schedule_refusal_text,
        record_index_schedule_refusal,
    )
    from .mandate_admission import audit_only_refusal, index_schedule_trigger_ref, toolkit_mandate_request
    from .trigger_principal import (
        OPERATION_OF,
        covering_toolkit_mandate,
        latest_toolkit_mandates,
        principal_of,
        resave_needed,
    )
    schedule_key = str(target.created_by)  # one key for the mandate lookup and the trigger reference
    with db.get_session(project_id) as session:
        mandates = latest_toolkit_mandates(session, project_id)
    mandate = covering_toolkit_mandate(mandates, target.toolkit_id, schedule_key, target.created_by)
    trigger_ref = index_schedule_trigger_ref(target.toolkit_id, target.index_name, schedule_key)
    resave = False
    if mandate is None:
        code = index_schedule_refusal_code(principal_of('index_schedule', schedule_key=schedule_key))
        resave = resave_needed(mandates, target.toolkit_id, schedule_key, target.created_by)
    else:
        code = audit_only_refusal(mandate, OPERATION_OF['index_schedule'])
    if code is None:
        return (toolkit_mandate_request(project_id=project_id, toolkit_id=target.toolkit_id, mandate=mandate,
                                        trigger_ref=trigger_ref) if governed else None), None
    if not governed:
        return None, (code, trigger_ref)
    reason = run_failure_reason(index_schedule_refusal_text(code, resave))
    finish_schedule_run(project_id, run_id, 'failed', reason)
    try:
        record_index_schedule_refusal(this.module, code, project_id=project_id, toolkit_id=target.toolkit_id,
                                      mandate_ref=None if mandate is None else mandate.id)
    except Exception:  # noqa: BLE001 -- the run is already refused and ended; only its audit line is lost
        log.exception('Index schedule run %s: cannot record the refusal', run_id)
    count_refusal(project_id, target, run_id, reason, code)
    raise _Ended('refused', reason)


def _admission(project_id: int, run_id: int, target, owner_request):
    """``(admission, would_deny)`` of the run; Run now and Test run bring the request their endpoint built."""
    if owner_request is not None or target.trigger != 'cron':
        return owner_request, None
    module = this.module
    governed = is_governed(module)
    if not (governed or is_auditing(module)):
        return None, None
    return _cron_admission(project_id, run_id, target, governed)


def _token(project_id: int, owner_id: int, admission, target) -> str | None:
    """The owner's access token for the index worker; none under a run identity, which carries its own."""
    try:
        token, auth_session = get_predict_token_and_session(
            project_id, owner_id, sid=None, run_identity=admission is not None,
            mandate_run=admission is not None and target.trigger == 'cron')
    except PredictPayloadError as error:
        raise _Ended('failed', NO_TOKEN) from error
    if token is None and auth_session is not None:  # a sign-in session cannot serve a run nobody is signed in to
        raise _Ended('failed', NO_TOKEN)
    return token


def _toolkit(project_id: int, owner_id: int, toolkit_id: int) -> tuple[dict, str]:
    """The toolkit config with its settings expanded as the owner, and its vector store connection string."""
    try:
        config = get_toolkit_config(project_id, owner_id, toolkit_id)
    except LookupError as error:
        log.warning('Index schedule: toolkit %s settings not expanded: %r', toolkit_id, error)
        raise _Ended('failed', TOOLKIT_CONFIG_FAILED) from error
    if not isinstance(config, dict) or 'error' in config:
        log.warning('Index schedule: toolkit %s not read: %r', toolkit_id, config)
        raise _Ended('failed', TOOLKIT_CONFIG_FAILED)
    connection_string = ((config.get('settings') or {}).get('pgvector_configuration') or {}).get('connection_string')
    if not connection_string:
        raise _Ended('failed', CREDENTIAL_MISSING)
    return config, connection_string


def _read_index(connection_string: str, toolkit_id: int, index_name: str, with_heartbeat: bool = False):
    """The index_meta row of the index as plain values (``meta``, ``heartbeat``), or None when it is missing."""
    with get_session_for_schema(connection_string, str(toolkit_id)) as session:
        row = session.query(EmbeddingStore.id, EmbeddingStore.cmetadata).filter(
            EmbeddingStore.cmetadata['type'].astext == 'index_meta',
            EmbeddingStore.cmetadata['collection'].astext == index_name,
        ).first()
        if row is None:
            return None
        meta = dict(row.cmetadata or {})
        heartbeat = get_pending_index_run_heartbeat(session, index_name) if with_heartbeat else None
        return SimpleNamespace(meta=meta, heartbeat=heartbeat)


def _index_to_run(project_id: int, connection_string: str, target) -> dict:
    """The saved index settings; ends the run (raises `_Ended`) when the index is gone, unreadable or still running.

    A live `in_progress` row ends the firing as skipped. A stale one is stopped best effort and replaced: its worker
    died without a final write, and skipping it would starve the schedule.
    """
    try:
        index = _read_index(connection_string, target.toolkit_id, target.index_name, with_heartbeat=True)
    except Exception as error:  # noqa: BLE001 -- the connection to the vector store is a lookup outage
        log.warning('Index schedule: index %s of toolkit %s not read: %r', target.index_name, target.toolkit_id, error)
        raise _Ended('failed', INDEX_UNAVAILABLE) from error
    if index is None:
        raise _Ended('failed', NOT_FOUND)
    state = str(index.meta.get('state') or '')
    if state.lower() == 'in_progress':
        timeout = int(VaultClient(project_id).get_secrets().get(
            'task_disconnected_timeout_sec', DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC))
        # No heartbeat_horizon: superseding is a control decision and must agree with the dispatch guard
        if not resolve_index_staleness(state.lower(), index.meta.get('updated_on', 0), timeout, index.heartbeat):
            raise _Ended('skipped', INDEX_RUN_ACTIVE)
        _stop_stale_task(index.meta.get('task_id'))
    if not index.meta.get('index_configuration'):
        raise _Ended('failed', INDEX_NOT_SCHEDULABLE)
    return index.meta['index_configuration']


def _stop_stale_task(task_id) -> None:
    """A straggler that is somehow alive must not keep writing the collection beside the new run; a dead task makes this a no-op."""
    if not task_id:
        return
    try:
        this.module.task_node.stop_task(task_id)
    except Exception as error:  # noqa: BLE001 -- staleness is a heuristic; the new run goes on
        log.warning('Index schedule: could not stop the stale task %s: %s', task_id, error)


def _llm_settings(project_id: int) -> dict:
    llm_settings, issue, _ = resolve_index_llm_settings(project_id)
    if issue:  # the issue can hold exception text: it goes to the log only
        log.warning('Index schedule: no LLM settings for project %s: %s', project_id, issue)
        raise _Ended('failed', LLM_UNAVAILABLE)
    return llm_settings


def _payload(project_id: int, owner_id: int, config: dict, tool_params: dict, llm_settings: dict, token, admission):
    """The task data of the legacy dispatch: the worker looks the toolkit up by its type, never its display name."""
    toolkit_config = {'id': config['id'], 'toolkit_name': config['type'], 'settings': config['settings']}
    data = {
        'tool_name': 'index_data',
        'project_id': project_id,
        'toolkit_config': toolkit_config,
        'tool_params': tool_params,
        'llm_model': llm_settings['model_name'],
        'llm_settings': llm_settings,
        'user_id': owner_id,
        'project_auth_token': token,
        'deployment_url': get_predict_base_url(project_id),
    }
    if admission is not None:
        # Imported here: only a governed run reads the credential slots
        from .credential_slots import has_free_form_secret, has_secret_template, mark_free_form_secret
        mark_free_form_secret(data, has_free_form_secret(
            this.module, {'project_id': project_id, 'tools': [toolkit_config]})
            or has_secret_template(data.get('tool_params')))
    return data


def _dispatch(project_id: int, run_id: int, target, data: dict, admission) -> str:
    """Start the index task; returns its id. A refusal ends the run as the agent starter ends one."""
    try:
        return start_index_task(this.module.task_node, data, None, initiator=InitiatorType.schedule,
                                admission=admission)
    except IndexRunInProgressError as error:
        raise _Ended('skipped', INDEX_RUN_ACTIVE) from error
    except PoolSaturationError as error:
        raise _Ended('failed', POOL_SATURATED) from error
    except AdmissionDenied as denied:
        # Imported here, as in _cron_admission
        from .admission import index_schedule_refusal_text
        reason = run_failure_reason(index_schedule_refusal_text(denied.reason_code))
        finish_schedule_run(project_id, run_id, 'failed', reason)
        if target.trigger == 'cron':  # Run now and Test run are the owner's own runs; an outage code is not counted
            count_refusal(project_id, target, run_id, reason, denied.reason_code)
        raise _Ended('refused', reason) from denied


def _end_skipped(project_id: int, run_id: int, reason: str) -> None:
    """End the claimed run as skipped, as the tick ends an overlap; no notice."""
    with db.get_session(project_id) as session:
        _end_runs(session, EntityScheduleRun.id == run_id, 'skipped', reason)
        session.commit()


def _run(project_id: int, run_id: int, owner_request) -> None:
    target = _load(project_id, run_id)
    if target is None:
        raise _Ended('failed', NOT_FOUND)
    record_run_events(project_id, run_id, _started_event(target))
    check = run_prerun_check(project_id, target, run_id)
    if not check.ok:
        return
    owner_id = check.owner_id
    scope = closed_budget_scope(project_id, owner_id)  # fails open
    if scope:
        block_schedule_run(project_id, run_id, BUDGET_CLOSED.format(scope))
        return
    admission, would_deny = _admission(project_id, run_id, target, owner_request)
    token = _token(project_id, owner_id, admission, target)
    # The settings come first: the index row is read through their connection string
    config, connection_string = _toolkit(project_id, owner_id, target.toolkit_id)
    tool_params = _index_to_run(project_id, connection_string, target)
    data = _payload(project_id, owner_id, config, tool_params, _llm_settings(project_id), token, admission)
    task_id = _dispatch(project_id, run_id, target, data, admission)
    record_run_events(project_id, run_id, run_event(INDEX_TASK, 'Index task started', task_id=str(task_id)))
    if would_deny is not None:
        _record_would_deny(project_id, target, *would_deny)
    _close_if_ended(project_id, connection_string, target, task_id)


def _record_would_deny(project_id: int, target, code: str, trigger_ref: str) -> None:
    # Imported here: only an auditing cron firing records it
    from .admission import record_index_schedule_would_deny
    try:
        record_index_schedule_would_deny(this.module, code, project_id=project_id, toolkit_id=target.toolkit_id,
                                         trigger_ref=trigger_ref)
    except Exception:  # noqa: BLE001 -- audit only never stops a run
        log.exception('Index schedule %s: the audit-only evaluation failed', target.id)


def _close_if_ended(project_id: int, connection_string: str, target, task_id) -> None:
    """A terminal event that came before the task id was written found no run: read the index row once more."""
    try:
        index = _read_index(connection_string, target.toolkit_id, target.index_name)
        if index is not None and str(index.meta.get('task_id')) == str(task_id):
            close_index_schedule_run(project_id, target.toolkit_id, target.index_name, task_id,
                                     index.meta.get('state'), index.meta.get('error'))
    except Exception:  # noqa: BLE001 -- the stale sweep closes a run whose end was missed
        log.exception('Index schedule run: cannot re-read the index row of %s', target.index_name)


def run_index_schedule(project_id: int, schedule_id: int | None, run_id: int, owner_request=None) -> None:
    """Start one claimed Toolkit Index run as its owner and return without waiting for the index task.

    ``schedule_id`` is None for a Test run. Every step that cannot go on ends the run with a fixed reason; none
    of them pauses the schedule. Under governance a cron firing is admitted on its owner's toolkit mandate or refused;
    Run now and Test run bring the ``owner_request`` their endpoint built. The index task ends the run through
    ``close_index_schedule_run``.
    """
    try:
        _run(project_id, run_id, owner_request)
    except _Ended as ended:
        if ended.status == 'skipped':
            _end_skipped(project_id, run_id, ended.reason)
        elif ended.status == 'failed':
            fail_schedule_run(project_id, run_id, ended.reason)
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule; the text stays fixed
        log.exception('Index schedule %s run %s: the run could not start', schedule_id, run_id)
        fail_schedule_run(project_id, run_id, START_FAILED)


def _names_index(form, toolkit_id: int, index_name: str) -> bool:
    form = form or {}
    return str(form.get('toolkit_id')) == str(toolkit_id) and form.get('index_name') == index_name


def _run_of_task(project_id: int, toolkit_id: int, index_name: str, task_id) -> int | None:
    """The running run whose `index_task` event holds `task_id`: a run of the schedule of this index, or its Test run."""
    of_this_index = select(EntitySchedule.id).where(
        EntitySchedule.entity_type == 'toolkit_index', EntitySchedule.toolkit_id == toolkit_id,
        EntitySchedule.index_name == index_name)
    with db.get_session(project_id) as session:
        rows = session.execute(
            select(EntityScheduleRun.id, EntityScheduleRun.events, RUN_FORM, EntityScheduleRun.schedule_id)
            .where(EntityScheduleRun.status == 'running',
                   or_(EntityScheduleRun.schedule_id.in_(of_this_index),
                       and_(EntityScheduleRun.schedule_id.is_(None), EntityScheduleRun.trigger == 'test')))
        ).all()
    for run_id, events, form, schedule_id in rows:
        if schedule_id is None and not _names_index(form, toolkit_id, index_name):
            continue
        if any(event.get('kind') == INDEX_TASK and str(event.get('task_id')) == str(task_id)
               for event in events or []):
            return run_id
    return None


def close_index_schedule_run(project_id: int, toolkit_id, index_name: str, task_id, state, error=None) -> bool:
    """End the run that started index task `task_id` when the task reaches a final state; True when a run ended.

    An event with no task id, or with the id of another task, leaves every run open: the stale sweep closes a run
    whose event was lost. A run that is not running any more is not touched, so a second close sends no second notice.
    """
    state_name = str(getattr(state, 'value', state) or '').lower()
    closing = CLOSING_STATES.get(state_name)
    if closing is None or not task_id or not index_name or toolkit_id is None:
        return False
    run_id = _run_of_task(project_id, int(toolkit_id), index_name, task_id)
    if run_id is None:
        return False
    status, reason = closing
    if status == 'failed':
        reason = run_failure_reason(error or (CANCELLED if state_name == 'cancelled' else None))
    return finish_schedule_run(project_id, run_id, status, reason)
