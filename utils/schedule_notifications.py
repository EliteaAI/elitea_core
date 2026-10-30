from pylon.core.tools import log
from tools import db, rpc_tools, this

from ..models.entity_schedule import EntitySchedulePreference
from ..models.enums.all import NotificationEventTypes as Types
from .sio_utils import SioEvents, get_event_room

DEFAULT_APPROVAL_HOURS = 24

# One fixed text per event type; {link} is the schedule name as an empty-target markdown link `[name]()`, the
# only form the bell renders as a link; {reason} is ": <reason>" when the run or `extra` gives one, else empty
MESSAGES = {
    Types.schedule_run_failed: 'Schedule "{link}" failed{reason}',
    Types.schedule_run_succeeded: 'Schedule "{link}" ran successfully',
    Types.schedule_approval_needed: 'Schedule "{link}" waits for your approval',
    Types.schedule_approval_expired: 'Schedule "{link}" was not approved in {hours} hours and the run stopped',
    Types.schedule_expired: 'Schedule "{link}" reached its stop date and is switched off',
    Types.schedule_disabled: 'Schedule "{link}" was switched off{reason}',
    Types.schedule_edited_by_admin: 'An admin {action} your schedule "{link}"',
    Types.schedule_owner_changed: 'The owner of schedule "{link}" changed',
}
# The bell's parser takes the first "]" as the end of the link text and knows no backslash escape,
# so brackets in a name become their full-width look-alikes; meta.schedule_name keeps the real name
_NAME_BRACKETS = str.maketrans('[]', '［］')


def _approval_hours() -> int:
    """The approval window in hours, as the run was parked with it; 24 when the setting cannot be read."""
    try:
        from .schedule_runs import approval_wait_hours  # late: schedule_runs sends notices through this module
        return approval_wait_hours()
    except Exception:  # noqa: BLE001 -- a notice must not fail on a settings read
        return DEFAULT_APPROVAL_HOURS


def _schedule_link(schedule) -> str:
    """The name as an empty-target link, `[name]()`; the bell builds the real target from event_type and meta."""
    return f'[{str(schedule.name or "").translate(_NAME_BRACKETS)}]()'


def get_notified_for(session, user_id: int) -> bool:
    """The user's "Get notified" value; true when the user has no row."""
    preference = session.get(EntitySchedulePreference, user_id)
    return preference is None or preference.get_notified


def notify_schedule_event(project_id, owner_id, event_type, schedule, run=None, extra=None) -> bool:
    """Send one schedule notice to the owner. Return True when it was fired, False when dropped or failed.

    `extra` may carry `reason` (when there is no run), `action` ("deleted" for schedule_edited_by_admin, default
    "edited") and, for schedule_owner_changed, `old_owner_id` and `new_owner_id`.
    """
    extra = extra or {}
    if event_type == Types.schedule_run_succeeded and not _owner_wants_success(project_id, owner_id, schedule):
        return False
    try:
        reason = getattr(run, 'reason', None) or extra.get('reason')
        # Only the expired notice names the window; the settings read is skipped for every other notice
        hours = _approval_hours() if event_type == Types.schedule_approval_expired else DEFAULT_APPROVAL_HOURS
        meta = {
            'message': MESSAGES[event_type].format(
                link=_schedule_link(schedule),
                reason=f': {reason}' if reason else '',
                action=extra.get('action', 'edited'),
                hours=hours,
            ),
            'schedule_id': schedule.id,
            'schedule_name': schedule.name,
            'run_id': run.id if run else None,
            'conversation_id': run.conversation_id if run else None,
        }
        if event_type == Types.schedule_owner_changed:
            meta.update(old_owner_id=extra.get('old_owner_id'), new_owner_id=extra.get('new_owner_id'))
        rpc_tools.EventManagerMixin().event_manager.fire_event('notifications_stream', {
            'project_id': project_id, 'user_id': owner_id, 'meta': meta, 'event_type': event_type,
        })
        return True
    except Exception as e:  # noqa: BLE001 -- a notice must never break the caller (R1.4)
        log.error('Schedule %s: cannot send %s notice: %s', schedule.id, event_type, e)
        return False


def _owner_wants_success(project_id, owner_id, schedule) -> bool:
    """The owner's "Get notified" value; a failed read sends the notice, only a saved false drops it (R3.1)."""
    try:
        with db.get_session(project_id) as session:
            return get_notified_for(session, owner_id)
    except Exception as e:  # noqa: BLE001 -- the gate must not lose a notice the owner did not turn off
        log.error('Schedule %s: cannot read Get notified, sending the notice: %s', schedule.id, e)
        return True


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
