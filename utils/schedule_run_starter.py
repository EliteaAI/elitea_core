"""Owner run starter of an agent or pipeline schedule (EL-7011 US-11, US-12, ADR-0002, ADR-0008)."""
import queue
import re
import threading
import time
from types import SimpleNamespace

from pylon.core.tools import log
from sqlalchemy import select
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.entity_schedule import RUN_APPLICATION_ID, RUN_FORM, EntitySchedule, EntityScheduleRun
from .budget_door import closed_budget_scope
from .exceptions import BudgetDoorClosedError
from .pipeline_execution import (
    TriggerType,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
    finish_trigger_run_refused,
    finish_trigger_run_with_error,
    is_trigger_refusal,
)
from .schedule_prerun import credential_toolkit_refs, run_prerun_check
from .schedule_runs import (
    block_schedule_run,
    count_refusal,
    end_refusal_streak,
    fail_schedule_run,
    finish_schedule_run,
    record_run_events,
    run_event,
    run_failure_reason,
)

NOT_FOUND = 'schedule or version not found'
RUN_NOT_FOUND = 'run not found'
START_FAILED = 'the run could not start'
PREDICT_FAILED = 'the run call failed'
ENTRY_POINT_MESSAGE = '[Scheduled execution triggered]'  # the text the legacy tick stores
BUDGET_CLOSED = 'the {} budget is used up'
BODY_GONE = 'the webhook body is not kept after a pause; send the delivery again'
BODY_UNREADABLE = 'the webhook body could not be read'
PLACEHOLDER = re.compile(r'\{\{\s*(\w+)\s*\}\}')


def fill_placeholders(text: str, variables: list[dict]) -> str:
    """Fill each {{ name }} once, in one pass; unknown names stay as written and no template logic runs (R2.3)."""
    values = {item['name']: str(item['value']) for item in variables}
    return PLACEHOLDER.sub(lambda match: values.get(match.group(1), match.group(0)), text)


def _load(project_id: int, schedule_id: int | None, run_id: int) -> SimpleNamespace | None:
    """Plain values of the schedule, its version, its agent or pipeline and the run trigger, read in one session.

    A run with no schedule (a Test Run) takes the same values from the form stored on the run.
    """
    with db.get_session(project_id) as session:
        run_row = session.get(EntityScheduleRun, run_id)
        if run_row is not None and run_row.schedule_id is None:
            return _load_test_run(session, run_row)
        schedule = session.get(EntitySchedule, schedule_id)
        version = schedule and schedule.version_id and session.get(ApplicationVersion, schedule.version_id)
        agent = version and session.get(Application, version.application_id)
        if not agent:
            return None
        trigger = getattr(run_row, 'trigger', 'cron')
        # Run now by an admin acts as the admin: the checks, the credentials and the run use the caller, never the owner
        requested_by = getattr(run_row, 'requested_by', None)
        as_caller = (trigger in ('manual', 'test') and requested_by is not None
                     and requested_by != schedule.created_by)
        return SimpleNamespace(
            schedule=SimpleNamespace(
                id=schedule.id, name=schedule.name,
                principal={'kind': 'delegation', 'ref': requested_by} if as_caller else schedule.principal,
                created_by=requested_by if as_caller else schedule.created_by, run_as_caller=as_caller,
                entity_type=schedule.entity_type, run_mode=schedule.run_mode,
                application_id=schedule.application_id, version_id=schedule.version_id,
                approval_rule=schedule.approval_rule, approval_fingerprint=schedule.approval_fingerprint,
                approval_confirmed_by=schedule.approval_confirmed_by,
                trigger_kind=schedule.trigger_kind, webhook_type=schedule.webhook_type,
                credential_toolkits=credential_toolkit_refs(version)),
            version=SimpleNamespace(id=version.id, application_id=version.application_id, name=version.name),
            entity_name=agent.name, prompt=schedule.prompt or '', variables=list(schedule.variables or []),
            trigger=trigger, resumed=getattr(run_row, 'resumed_at', None) is not None)


def _load_test_run(session, run_row) -> SimpleNamespace | None:
    """The values of a Test Run, built from its stored form; None when the version or the agent is gone.

    The form never holds a fingerprint or a confirmation, so a Test Run never runs without asking.
    """
    form, application_id = session.execute(
        select(RUN_FORM, RUN_APPLICATION_ID).where(EntityScheduleRun.id == run_row.id)).one()
    form = form or {}
    version_id = form.get('version_id')
    version = version_id and session.get(ApplicationVersion, version_id)
    agent = version and session.get(Application, version.application_id)
    if not agent or version.application_id != application_id:
        return None
    owner_id = run_row.requested_by
    return SimpleNamespace(
        schedule=SimpleNamespace(
            id=None, name=form.get('name'), principal={'kind': 'delegation', 'ref': owner_id}, created_by=owner_id,
            entity_type=form.get('entity_type'), run_mode=form.get('run_mode'),
            application_id=application_id, version_id=version.id,
            approval_rule=form.get('approval_rule') or 'pause_and_ask', approval_fingerprint=None,
            approval_confirmed_by=None, credential_toolkits=credential_toolkit_refs(version)),
        version=SimpleNamespace(id=version.id, application_id=version.application_id, name=version.name),
        entity_name=agent.name, prompt=form.get('prompt') or '', variables=list(form.get('variables') or []),
        trigger=run_row.trigger, resumed=run_row.resumed_at is not None)


def _link_conversation(project_id: int, run_id: int, conversation_uuid: str) -> bool:
    """Write the conversation id on the run row; False when the run row is gone."""
    with db.get_session(project_id) as session:
        run = session.get(EntityScheduleRun, run_id)
        if run is None:
            return False
        run.conversation_id = session.query(Conversation).filter_by(uuid=conversation_uuid).one().id
        session.commit()
        return True


def _setting_allows_auto_approve(project_id: int) -> bool:
    """The project setting "Allow Run without asking" (R3.2); off or unreadable means pause and ask.

    The late import keeps the settings store off every other run.
    """
    from .schedule_settings import is_run_without_asking_allowed
    return is_run_without_asking_allowed(project_id)


# The run log's owner name is kept this long per owner (#7294 P-07): the runs a tick starts for one owner ask the
# identity service once. A display name only; nothing is decided on it
OWNER_LABEL_TTL_S = 60.0
MAX_OWNER_LABELS = 1024
_OWNER_LABELS = {}   # user id -> (name, monotonic until)
_OWNER_LABELS_LOCK = threading.Lock()


def _owner_label(owner_id: int, role: str = 'schedule owner') -> str:
    """The account the run uses: by name, else email, else id, with its `role`.

    Only the name is kept per user; the role comes from the run, as one user is the owner of one schedule and an
    admin on another.
    """
    now = time.monotonic()
    with _OWNER_LABELS_LOCK:
        kept = _OWNER_LABELS.get(owner_id)
        if kept is not None and kept[1] > now:
            return f'{kept[0]} ({role})'
    try:
        # Late: only the run log needs the auth lookup
        from .authors import get_authors_data
        user = next(iter(get_authors_data([owner_id])), None) or {}
    except Exception as e:  # noqa: BLE001 -- a name must never stop a run
        log.warning('Schedule run owner %s: name lookup failed: %s', owner_id, e)
        user = None
    name = (user or {}).get('name') or (user or {}).get('email') or f'user {owner_id}'
    if user is not None:   # a failed lookup is asked again by the next run
        with _OWNER_LABELS_LOCK:
            if len(_OWNER_LABELS) >= MAX_OWNER_LABELS:
                _OWNER_LABELS.clear()
            _OWNER_LABELS[owner_id] = (name, now + OWNER_LABEL_TTL_S)
    return f'{name} ({role})'


def _may_auto_approve(project_id: int, schedule, review: dict) -> bool:
    """"Run without asking" holds only for the confirmed tools (owner CR 2026-10-21, R3.2).

    New sensitive tools since the confirmation, an incomplete scan or the setting off run as "Pause and ask me".
    """
    from .schedule_risk import scan_complete
    if schedule.approval_rule != 'run_without_asking' or not scan_complete(review):
        return False
    # The owner's confirmation does not cover another account's credentials: a run that acts as an admin
    # (created_by is the caller) never matches it, so it always pauses before a sensitive action.
    if review['sensitive'] and (review['fingerprint'] != schedule.approval_fingerprint
                                or schedule.approval_confirmed_by != schedule.created_by):
        return False
    return _setting_allows_auto_approve(project_id)


def _runtime_tool_name(name) -> str:
    """The tool name as the SDK guard compares it: the last alias after `___` or `:`, in lower case."""
    reduced = str(name or '').strip().lower()
    if '___' in reduced:
        reduced = reduced.split('___', 1)[1].strip()
    if ':' in reduced:
        reduced = reduced.split(':', 1)[1].strip()
    return reduced


def _confirmed_pairs(review: dict):
    """The confirmed (toolkit, tool) pairs a run may approve; a personal credential is (toolkit, '*'). False when none: an empty scan is no consent.

    An item without a toolkit name is left out: the SDK rejects it, and that tool pauses and asks. An item that
    comes from the global list may be spelled differently from the runtime name, which the SDK compares exactly.
    It is approved by its normalized name; only when the toolkit has no unconfirmed `may_apply` tool it is
    also approved as (toolkit, '*'), because (toolkit, '*') would let that unconfirmed tool run unasked.
    """
    unconfirmed = {item.get('toolkit') for item in review.get('notices', []) if item.get('may_apply')}
    pairs = []
    for item in review['sensitive']:
        toolkit = item.get('toolkit')
        if not toolkit:
            continue
        if not item.get('from_list'):
            pairs.append((toolkit, item.get('tool') or '*'))
            continue
        if item.get('tool'):
            pairs.append((toolkit, _runtime_tool_name(item['tool'])))
        if toolkit not in unconfirmed:
            pairs.append((toolkit, '*'))
    return list(dict.fromkeys(pairs)) or False


def _risk_check(review: dict, rule: str, auto_approve: bool) -> tuple[str, dict]:
    """The risk outcome of the run for the run log and its `risk_check` event (owner CR 2026-10-21).

    `blocked` (a blocking finding), `needs_approval` (listed sensitive tools, or "Run without asking" turned
    back to "Pause and ask me"), `unknown` (a tool could not be checked) or `ok`.
    """
    from .schedule_risk import scan_complete
    if review['blocking']:
        reasons = '; '.join(item['message'] for item in review['blocking'])
        return 'blocked', run_event('risk_check', f'Risk check: blocked: {reasons}', 'error')
    # A personal credential keeps its own item and confirmation; only a listed tool needs approval at run time
    if any(item.get('code') == 'sensitive_action' for item in review['sensitive']):
        tools = ', '.join(f"{item.get('toolkit')} · {item.get('tool') or '*'}" for item in review['sensitive'])
        text = f'Risk check: needs approval for {tools}'
        if auto_approve:
            text += '; Run without asking is on'
        elif rule == 'run_without_asking':
            text += '; Run without asking does not hold, so the run pauses and asks'
        return 'needs_approval', run_event('risk_check', text, 'warning')
    if not scan_complete(review):
        return 'unknown', run_event('risk_check', 'Risk check: not complete', 'warning')
    return 'ok', run_event('risk_check', 'Risk check: OK')


def _webhook_row(schedule) -> bool:
    return getattr(schedule, 'trigger_kind', None) == 'webhook'  # a Test Run form has no trigger_kind


def _started_event(loaded, trigger: str, delivery: str | None) -> dict:
    """The first run log line; a webhook run also names its type and delivery id."""
    label = 'Test run' if trigger == 'test' else 'Run'
    sample = trigger == 'test' and _webhook_row(loaded.schedule)
    if trigger != 'webhook' and not sample:
        return run_event('started', f'{label} started · trigger: {trigger} · version {loaded.version.name}',
                         trigger=trigger, version_id=loaded.version.id)
    webhook_type = loaded.schedule.webhook_type
    if sample:  # the sample body of the person who pressed the button (#7455)
        by = 'an admin' if getattr(loaded.schedule, 'run_as_caller', False) else 'the owner'
        return run_event('started', f"{label} started · trigger: webhook · type: {webhook_type} · "
                                    f"Sample body by {by} · version {loaded.version.name}",
                         trigger=trigger, version_id=loaded.version.id, webhook_type=webhook_type, sample=True)
    return run_event('started', f"{label} started · trigger: webhook · type: {webhook_type} · "
                                f"delivery {delivery or 'none'} · version {loaded.version.name}",
                     trigger=trigger, version_id=loaded.version.id, webhook_type=webhook_type, delivery_id=delivery)


def _webhook_input(loaded, payload: bytes) -> tuple[str, str]:
    """The run input and the redacted trigger message of a delivery."""
    from .webhook_input import parse_body, shape_input, trigger_message  # late: the guardrail loads its detectors
    body_json = parse_body(payload)  # serialized once, for the input and the preview
    prompt = fill_placeholders(loaded.prompt, loaded.variables)
    return (shape_input(loaded.schedule.entity_type, prompt, body_json),
            trigger_message(loaded.schedule.webhook_type, body_json))


def run_owner_schedule(project_id: int, schedule_id: int, run_id: int, owner_request=None,
                       payload: bytes | None = None, delivery: str | None = None) -> None:
    """Start one claimed agent or pipeline run as its owner and return without waiting for the answer.

    A closed budget or a blocking risk finding blocks the run. Every other failure after the pre-run check
    closes the run as failed: a running run blocks the schedule. Each step adds its line to the run log.
    Under governance (C-1009) a cron firing is admitted on its owner's mandate or refused; a refusal ends it at once.
    Run now and Test run (``owner_request``, built by the run endpoint) act as the person who pressed the button, the
    owner or an admin: no mandate (#7254).
    A webhook delivery (#7454) brings its body in ``payload`` (memory only) and its id in ``delivery``.
    """
    loaded = _load(project_id, schedule_id, run_id)
    if loaded is None:
        fail_schedule_run(project_id, run_id, NOT_FOUND)
        return
    trigger = getattr(loaded, 'trigger', 'cron')
    # a sample-body test run of a webhook row runs exactly as a delivery (#7455)
    webhook = trigger == 'webhook' or (trigger == 'test' and _webhook_row(loaded.schedule))
    if not getattr(loaded, 'resumed', False):  # an approved parked run starts again; its log already has the first line
        record_run_events(project_id, run_id, _started_event(loaded, trigger, delivery), version_id=loaded.version.id)
    check = run_prerun_check(project_id, loaded.schedule, run_id)
    if not check.ok:
        return
    # A run an admin started acts as the admin: the log names that account as the admin, not as the owner
    role = 'admin' if getattr(loaded.schedule, 'run_as_caller', False) else 'schedule owner'
    label = _owner_label(check.owner_id, role)
    record_run_events(project_id, run_id, run_event('account', f'Account: {label}'), principal_label=label)
    scope = closed_budget_scope(project_id, check.owner_id)  # fails open (R2.2)
    if scope:
        block_schedule_run(project_id, run_id, BUDGET_CLOSED.format(scope))
        return
    # late: the scan pulls in the toolkit expansion
    from .schedule_risk import scan_schedule
    review = scan_schedule(project_id, loaded.version.id, check.owner_id, loaded.schedule.approval_rule)
    auto_approve = not review['blocking'] and _may_auto_approve(project_id, loaded.schedule, review)
    approved = _confirmed_pairs(review) if auto_approve else False
    outcome, risk_line = _risk_check(review, loaded.schedule.approval_rule, bool(approved))
    record_run_events(project_id, run_id, risk_line, risk_outcome=outcome)
    if review['blocking']:  # a scheduled run cannot sign in or answer a person
        reasons = '; '.join(item['message'] for item in review['blocking'])
        if all(item['code'] != 'human_input_step' for item in review['blocking']):
            # "Run without asking" never waits: a missing sign-in or credential is a failure with a clear reason
            finish_schedule_run(project_id, run_id, 'failed', reasons)
        else:
            block_schedule_run(project_id, run_id, reasons, 'error')
        return
    source = loaded.schedule.entity_type
    if webhook and payload is None:  # approved after a pre-run park: the body was never kept (#7454)
        fail_schedule_run(project_id, run_id, BODY_GONE)
        return
    if webhook:
        try:
            user_input, message = _webhook_input(loaded, payload)
        except Exception:  # noqa: BLE001 -- a body the run cannot read must end it with a reason, not a bare error
            log.exception('Webhook %s run %s: cannot shape the delivery body', schedule_id, run_id)
            fail_schedule_run(project_id, run_id, BODY_UNREADABLE)
            return
    elif source == 'pipeline' and loaded.schedule.run_mode == 'entry_point':
        user_input, message = '', ENTRY_POINT_MESSAGE  # the pipeline starts at its YAML entry_point (R2.2)
    else:
        user_input = message = fill_placeholders(loaded.prompt, loaded.variables)
    manual = trigger in ('manual', 'test') and not webhook  # Run now and Test run are the caller's own runs
    trigger_type = TriggerType.SCHEDULE_MANUAL if manual else TriggerType.WEBHOOK if webhook else TriggerType.SCHEDULED
    extra_meta = {'schedule_id': loaded.schedule.id, 'schedule_run_id': run_id, 'execution_type': trigger_type}
    if webhook:
        extra_meta['webhook_type'] = loaded.schedule.webhook_type
    try:
        # lean: a conversation stays when the link fails after it; the run still closes as failed
        conversation_uuid, _, response_id = create_trigger_run_conversation(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            trigger_type=trigger_type, trigger_message=message,
            conversation_name=f"{'Manual' if manual else 'Webhook' if webhook else 'Scheduled'} run: {loaded.entity_name}",
            extra_meta=extra_meta,
            source=source,
        )
        linked = _link_conversation(project_id, run_id, conversation_uuid)
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule
        log.exception('Schedule %s run %s: cannot create the run conversation', schedule_id, run_id)
        fail_schedule_run(project_id, run_id, START_FAILED)
        return
    if not linked:
        fail_schedule_run(project_id, run_id, RUN_NOT_FOUND)
        return
    record_run_events(project_id, run_id, run_event(
        'agent_started', f"{'Pipeline' if source == 'pipeline' else 'Agent'} started: {loaded.entity_name}"))
    try:
        # late: only a governed cron firing reads its mandate. Either single-use ticket is minted right before the run
        # is sent: a manual run's from the owner's own request (none when governance is off)
        if manual:
            governance = {}
            if owner_request is not None:
                from .mandate_admission import mint_owner_run_ticket
                governance = {'mandate_ticket': mint_owner_run_ticket(owner_request)}
        else:
            from .mandate_admission import prepare_schedule_run
            governance = prepare_schedule_run(project_id=project_id, version=loaded.version, schedule_id=schedule_id,
                                              owner_id=check.owner_id, trigger_kind='webhook' if webhook else 'cron')
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule; the text stays fixed
        log.exception('Schedule %s run %s: cannot prepare the governed run', schedule_id, run_id)
        finish_trigger_run_with_error(project_id, response_id, START_FAILED, source)
        fail_schedule_run(project_id, run_id, START_FAILED)
        return
    try:
        result = execute_pipeline_via_predict_sio(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            conversation_uuid=conversation_uuid, response_message_id=response_id, user_input=user_input,
            trigger_source=trigger_type, is_system_user=False,
            auto_approve_sensitive_actions=approved, **governance,
        )
    except queue.Empty:  # the 5 s predict RPC timeout: the agent may still run, so the stale-run sweep ends it
        log.warning('Schedule %s run %s: no answer from the predict call within its timeout', schedule_id, run_id)
    except BudgetDoorClosedError as error:  # the door closed after the check (R2.3)
        finish_trigger_run_with_error(project_id, response_id, error, source)
        block_schedule_run(project_id, run_id, BUDGET_CLOSED.format(error.scope))
    except Exception as error:  # noqa: BLE001 -- the run message ends with the reason (R2.5)
        finish_trigger_run_with_error(project_id, response_id, error, source)
        fail_schedule_run(project_id, run_id, PREDICT_FAILED)
    else:
        if is_trigger_refusal(result):  # governance refused it (C-1009): it ends now, readably, and the owner is told
            reason = run_failure_reason(result.get('error'))
            try:  # ended first, so the chat stream-end hook finds it ended and sends no second notice
                finish_schedule_run(project_id, run_id, 'failed', reason)
            finally:
                finish_trigger_run_refused(project_id, response_id, result, source)
            # only cron firings count (#7254), the third in a row pauses it; an outage code is not counted. Run now
            # and Test run are the owner's own runs
            if not manual:
                count_refusal(project_id, loaded.schedule, run_id, reason, result.get('code'))
        elif (governance or webhook) and not manual:  # an admitted start ends a streak; a webhook counts pre-run failures too
            end_refusal_streak(project_id, schedule_id)
