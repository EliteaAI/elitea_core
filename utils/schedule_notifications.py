from pylon.core.tools import log
from tools import db, rpc_tools, this

from ..models.entity_schedule import EntitySchedulePreference
from ..models.enums.all import NotificationEventTypes as Types
from .sio_utils import SioEvents, get_event_room

# One fixed text per event type; {reason} is the run's reason and is empty without a run
MESSAGES = {
    Types.schedule_run_failed: 'Schedule "{name}" failed: {reason}',
    Types.schedule_run_succeeded: 'Schedule "{name}" ran successfully',
    Types.schedule_approval_needed: 'Schedule "{name}" waits for your approval',
    Types.schedule_approval_expired: 'Schedule "{name}" was not approved in 24 hours and the run stopped',
    Types.schedule_expired: 'Schedule "{name}" reached its stop date and is switched off',
    Types.schedule_disabled: 'Schedule "{name}" was switched off: {reason}',
    Types.schedule_edited_by_admin: 'An admin edited your schedule "{name}"',
    Types.schedule_owner_changed: 'The owner of schedule "{name}" changed',
}


def get_notified_for(session, user_id: int) -> bool:
    """The user's "Get notified" value; true when the user has no row."""
    preference = session.get(EntitySchedulePreference, user_id)
    return preference is None or preference.get_notified


def notify_schedule_event(project_id, owner_id, event_type, schedule, run=None, extra=None) -> bool:
    """Send one schedule notice to the owner. Return True when it was fired, False when dropped or failed."""
    try:
        if event_type == Types.schedule_run_succeeded:
            with db.get_session(project_id) as session:
                if not get_notified_for(session, owner_id):
                    return False
        meta = {
            'message': MESSAGES[event_type].format(name=schedule.name, reason=getattr(run, 'reason', None) or ''),
            'schedule_id': schedule.id,
            'schedule_name': schedule.name,
            'run_id': run.id if run else None,
            'conversation_id': run.conversation_id if run else None,
        }
        if event_type == Types.schedule_owner_changed:
            extra = extra or {}
            meta.update(old_owner_id=extra.get('old_owner_id'), new_owner_id=extra.get('new_owner_id'))
        rpc_tools.EventManagerMixin().event_manager.fire_event('notifications_stream', {
            'project_id': project_id, 'user_id': owner_id, 'meta': meta, 'event_type': event_type,
        })
        return True
    except Exception as e:  # noqa: BLE001 -- a notice must never break the caller (R1.4)
        log.error('Schedule %s: cannot send %s notice: %s', schedule.id, event_type, e)
        return False


def emit_schedule_run_finished(owner_id, schedule_id, run_id, status, entity_id, conversation_id) -> None:
    """Tell the owner's open chats that a run ended; a missed event loses only the chat notice."""
    try:
        this.module.context.sio.emit(
            SioEvents.schedule_run_finished.value,
            {'schedule_id': schedule_id, 'run_id': run_id, 'status': status,
             'entity_id': entity_id, 'conversation_id': conversation_id},
            to=get_event_room('notifications', owner_id),
        )
    except Exception as e:  # noqa: BLE001 -- a lost chat notice must not break the run (R4.3)
        log.error('Schedule %s: cannot emit schedule_run_finished: %s', schedule_id, e)
