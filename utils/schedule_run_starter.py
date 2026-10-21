"""Owner run starter of an agent or pipeline schedule (EL-7011 US-11, US-12, ADR-0002, ADR-0008)."""
import re
from types import SimpleNamespace

from pylon.core.tools import log
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from .budget_door import closed_budget_scope
from .exceptions import BudgetDoorClosedError
from .pipeline_execution import (
    TriggerType,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
    finish_trigger_run_with_error,
)
from .schedule_prerun import run_prerun_check
from .schedule_runs import (
    block_schedule_run,
    fail_schedule_run,
    record_run_events,
    run_event,
)

NOT_FOUND = 'schedule or version not found'
RUN_NOT_FOUND = 'run not found'
START_FAILED = 'the run could not start'
PREDICT_FAILED = 'the run call failed'
ENTRY_POINT_MESSAGE = '[Scheduled execution triggered]'  # the text the legacy tick stores
BUDGET_CLOSED = 'the {} budget is used up'
PLACEHOLDER = re.compile(r'\{\{\s*(\w+)\s*\}\}')


def fill_placeholders(text: str, variables: list[dict]) -> str:
    """Fill each {{ name }} once, in one pass; unknown names stay as written and no template logic runs (R2.3)."""
    values = {item['name']: str(item['value']) for item in variables}
    return PLACEHOLDER.sub(lambda match: values.get(match.group(1), match.group(0)), text)


def _load(project_id: int, schedule_id: int, run_id: int) -> SimpleNamespace | None:
    """Plain values of the schedule, its version, its agent or pipeline and the run trigger, read in one session."""
    with db.get_session(project_id) as session:
        schedule = session.get(EntitySchedule, schedule_id)
        version = schedule and schedule.version_id and session.get(ApplicationVersion, schedule.version_id)
        agent = version and session.get(Application, version.application_id)
        if not agent:
            return None
        return SimpleNamespace(
            schedule=SimpleNamespace(
                id=schedule.id, name=schedule.name, principal=schedule.principal, created_by=schedule.created_by,
                entity_type=schedule.entity_type, run_mode=schedule.run_mode,
                application_id=schedule.application_id, version_id=schedule.version_id,
                approval_rule=schedule.approval_rule),
            version=SimpleNamespace(id=version.id, application_id=version.application_id, name=version.name),
            entity_name=agent.name, prompt=schedule.prompt or '', variables=list(schedule.variables or []),
            trigger=getattr(session.get(EntityScheduleRun, run_id), 'trigger', 'cron'))


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


def _owner_label(owner_id: int) -> str:
    """The account the run uses: its owner by name, else email, else id."""
    try:
        # Late: only the run log needs the auth lookup
        from .authors import get_authors_data
        user = next(iter(get_authors_data([owner_id])), None) or {}
    except Exception as e:  # noqa: BLE001 -- a name must never stop a run
        log.warning('Schedule run owner %s: name lookup failed: %s', owner_id, e)
        user = {}
    return f"{user.get('name') or user.get('email') or f'user {owner_id}'} (schedule owner)"


def _risk_check(project_id: int, version_id: int, owner_id: int, auto_approve: bool) -> tuple[str, dict]:
    """The risk outcome of the version for the run log: `ok`, `needs_approval` or `unknown`, and its event."""
    # Late: the scan pulls the toolkit expansion
    from .schedule_risk import scan_schedule_risks
    # lean: the scan runs on every run start and may expand toolkits over the network; reuse the save-time result if latency shows
    warnings = scan_schedule_risks(project_id, version_id, owner_id)
    sensitive = [f"{item.get('toolkit')} · {item.get('tool')}" for item in warnings if item['code'] == 'sensitive_action']
    if sensitive:
        text = 'Risk check: needs approval for ' + ', '.join(sensitive)
        return 'needs_approval', run_event(
            'risk_check', text + ('; Run without asking is on' if auto_approve else ''), 'warning')
    if any(item['code'] == 'scan_incomplete' for item in warnings):
        return 'unknown', run_event('risk_check', 'Risk check: not complete', 'warning')
    return 'ok', run_event('risk_check', 'Risk check: OK')


def run_owner_schedule(project_id: int, schedule_id: int, run_id: int) -> None:
    """Start one claimed agent or pipeline run as its owner and return without waiting for the answer.

    A closed budget blocks the run. Every other failure after the pre-run check closes the run as failed:
    a running run blocks the schedule. Each step adds its line to the run log.
    """
    loaded = _load(project_id, schedule_id, run_id)
    if loaded is None:
        fail_schedule_run(project_id, run_id, NOT_FOUND)
        return
    trigger = getattr(loaded, 'trigger', 'cron')
    record_run_events(project_id, run_id, run_event(
        'started', f"{'Test run' if trigger == 'test' else 'Run'} started · trigger: {trigger} · "
                   f"version {loaded.version.name}", trigger=trigger, version_id=loaded.version.id,
    ), version_id=loaded.version.id)
    check = run_prerun_check(project_id, loaded.schedule, run_id)
    if not check.ok:
        return
    label = _owner_label(check.owner_id)
    record_run_events(project_id, run_id, run_event('account', f'Account: {label}'), principal_label=label)
    scope = closed_budget_scope(project_id, check.owner_id)  # fails open (R2.2)
    if scope:
        block_schedule_run(project_id, run_id, BUDGET_CLOSED.format(scope))
        return
    source = loaded.schedule.entity_type
    if source == 'pipeline' and loaded.schedule.run_mode == 'entry_point':
        user_input, message = '', ENTRY_POINT_MESSAGE  # the pipeline starts at its YAML entry_point (R2.2)
    else:
        user_input = message = fill_placeholders(loaded.prompt, loaded.variables)
    auto_approve = loaded.schedule.approval_rule == 'run_without_asking' and _setting_allows_auto_approve(project_id)
    outcome, risk_line = _risk_check(project_id, loaded.version.id, check.owner_id, auto_approve)
    record_run_events(project_id, run_id, risk_line, risk_outcome=outcome)
    manual = trigger in ('manual', 'test')  # Run now and Test run (US-15 R1.3)
    trigger_type = TriggerType.SCHEDULE_MANUAL if manual else TriggerType.SCHEDULED
    try:
        # lean: a conversation stays when the link fails after it; the run still closes as failed
        conversation_uuid, _, response_id = create_trigger_run_conversation(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            trigger_type=trigger_type, trigger_message=message,
            conversation_name=f"{'Manual' if manual else 'Scheduled'} run: {loaded.entity_name}",
            extra_meta={'schedule_id': schedule_id, 'schedule_run_id': run_id, 'execution_type': trigger_type},
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
        execute_pipeline_via_predict_sio(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            conversation_uuid=conversation_uuid, response_message_id=response_id, user_input=user_input,
            trigger_source=trigger_type, is_system_user=False,
            auto_approve_sensitive_actions=auto_approve,
        )
    except BudgetDoorClosedError as error:  # the door closed after the check (R2.3)
        finish_trigger_run_with_error(project_id, response_id, error)
        block_schedule_run(project_id, run_id, BUDGET_CLOSED.format(error.scope))
    except Exception as error:  # noqa: BLE001 -- the run message ends with the reason (R2.5)
        finish_trigger_run_with_error(project_id, response_id, error)
        fail_schedule_run(project_id, run_id, PREDICT_FAILED)
