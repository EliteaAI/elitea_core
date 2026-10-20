"""Owner run starter of an agent schedule (EL-7011 US-11, ADR-0002, ADR-0008)."""
import re
from types import SimpleNamespace

from pylon.core.tools import log
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from .pipeline_execution import (
    TriggerType,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
    finish_trigger_run_with_error,
)
from .schedule_prerun import run_prerun_check
from .schedule_runs import fail_schedule_run

NOT_FOUND = 'schedule or version not found'
RUN_NOT_FOUND = 'run not found'
START_FAILED = 'the run could not start'
PREDICT_FAILED = 'the agent call failed'
PLACEHOLDER = re.compile(r'\{\{\s*(\w+)\s*\}\}')


def fill_placeholders(text: str, variables: list[dict]) -> str:
    """Fill each {{ name }} once, in one pass; unknown names stay as written and no template logic runs (R2.3)."""
    values = {item['name']: str(item['value']) for item in variables}
    return PLACEHOLDER.sub(lambda match: values.get(match.group(1), match.group(0)), text)


def _load(project_id: int, schedule_id: int) -> SimpleNamespace | None:
    """Plain values of the schedule, its version and its agent, read inside one session."""
    with db.get_session(project_id) as session:
        schedule = session.get(EntitySchedule, schedule_id)
        version = schedule and schedule.version_id and session.get(ApplicationVersion, schedule.version_id)
        agent = version and session.get(Application, version.application_id)
        if not agent:
            return None
        return SimpleNamespace(
            schedule=SimpleNamespace(
                id=schedule.id, name=schedule.name, principal=schedule.principal, created_by=schedule.created_by,
                application_id=schedule.application_id, version_id=schedule.version_id),
            version=SimpleNamespace(id=version.id, application_id=version.application_id),
            agent_name=agent.name, prompt=schedule.prompt or '', variables=list(schedule.variables or []))


def _link_conversation(project_id: int, run_id: int, conversation_uuid: str) -> bool:
    """Write the conversation id on the run row; False when the run row is gone."""
    with db.get_session(project_id) as session:
        run = session.get(EntityScheduleRun, run_id)
        if run is None:
            return False
        run.conversation_id = session.query(Conversation).filter_by(uuid=conversation_uuid).one().id
        session.commit()
        return True


def run_owner_schedule(project_id: int, schedule_id: int, run_id: int) -> None:
    """Start one claimed agent run as its owner and return without waiting for the answer.

    Every failure after the pre-run check closes the run as failed: a running run blocks the schedule.
    """
    loaded = _load(project_id, schedule_id)
    if loaded is None:
        fail_schedule_run(project_id, run_id, NOT_FOUND)
        return
    check = run_prerun_check(project_id, loaded.schedule, run_id)
    if not check.ok:
        return
    prompt = fill_placeholders(loaded.prompt, loaded.variables)
    try:
        # lean: a conversation stays when the link fails after it; the run still closes as failed
        conversation_uuid, _, response_id = create_trigger_run_conversation(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            trigger_type=TriggerType.SCHEDULED, trigger_message=prompt,
            conversation_name=f'Scheduled run: {loaded.agent_name}',
            extra_meta={'schedule_id': schedule_id, 'schedule_run_id': run_id, 'execution_type': 'scheduled'},
            source='agent',
        )
        linked = _link_conversation(project_id, run_id, conversation_uuid)
    except Exception:  # noqa: BLE001 -- a run left running would block the schedule
        log.exception('Schedule %s run %s: cannot create the run conversation', schedule_id, run_id)
        fail_schedule_run(project_id, run_id, START_FAILED)
        return
    if not linked:
        fail_schedule_run(project_id, run_id, RUN_NOT_FOUND)
        return
    try:
        execute_pipeline_via_predict_sio(
            project_id=project_id, version=loaded.version, user_id=check.owner_id,
            conversation_uuid=conversation_uuid, response_message_id=response_id, user_input=prompt,
            trigger_source='scheduled', is_system_user=False,
        )
    except Exception as error:  # noqa: BLE001 -- the run message ends with the reason (R2.5)
        finish_trigger_run_with_error(project_id, response_id, error)
        fail_schedule_run(project_id, run_id, PREDICT_FAILED)
