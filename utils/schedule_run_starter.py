"""Owner run starter of an agent schedule (EL-7011 US-11, ADR-0002, ADR-0008)."""
from types import SimpleNamespace

from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from ..models.pd.chat import _resolve_variables
from .pipeline_execution import (
    TriggerType,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
    finish_trigger_run_with_error,
)
from .schedule_prerun import run_prerun_check
from .schedule_runs import fail_schedule_run

NOT_FOUND = 'schedule or version not found'


def run_owner_schedule(project_id: int, schedule_id: int, run_id: int, trigger: str = 'cron') -> None:
    """Start one claimed agent run as its owner and return without waiting for the answer."""
    with db.get_session(project_id) as session:
        schedule = session.get(EntitySchedule, schedule_id)
        version = schedule and schedule.version_id and session.get(ApplicationVersion, schedule.version_id)
        agent = version and session.get(Application, schedule.application_id)
    if not agent:
        fail_schedule_run(project_id, run_id, NOT_FOUND)
        return
    check = run_prerun_check(project_id, schedule, run_id)
    if not check.ok:
        return
    cron = trigger == 'cron'
    prompt = _resolve_variables(schedule.prompt or '', [SimpleNamespace(**item) for item in schedule.variables])
    conversation_uuid, _, response_id = create_trigger_run_conversation(
        project_id=project_id, version=version, user_id=check.owner_id,
        trigger_type=TriggerType.SCHEDULED if cron else TriggerType.MANUAL,
        trigger_message=prompt, conversation_name=f'Scheduled run: {agent.name}',
        extra_meta={
            'schedule_id': schedule_id, 'schedule_run_id': run_id,
            'execution_type': 'scheduled' if cron else 'manual',
        },
        source='agent',
    )
    with db.get_session(project_id) as session:
        conversation = session.query(Conversation).filter_by(uuid=conversation_uuid).one()
        session.get(EntityScheduleRun, run_id).conversation_id = conversation.id
        session.commit()
    try:
        execute_pipeline_via_predict_sio(
            project_id=project_id, version=version, user_id=check.owner_id,
            conversation_uuid=conversation_uuid, response_message_id=response_id, user_input=prompt,
            trigger_source='scheduled' if cron else None, is_system_user=False,
        )
    except Exception as error:  # noqa: BLE001 -- the run message ends with the reason (R2.5)
        finish_trigger_run_with_error(project_id, response_id, error)
