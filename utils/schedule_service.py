"""Create, update and delete of an entity schedule: owner, scope, audit and run input rules (EL-7011)."""
import re
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

from opentelemetry import trace as otel_trace
from pylon.core.tools import log
from sqlalchemy import select

from ..models.all import Application, ApplicationVersion
from ..models.entity_schedule import EntitySchedule
from ..models.enums.all import AgentTypes, NotificationEventTypes
from ..models.pd.entity_schedule import ScheduleCreate, ScheduleTiming, ScheduleUpdate
from .folder_access import entity_access_error
from .schedule_access import ScheduleScope
from .schedule_notifications import notify_schedule_event
from .schedule_timing import check_enabled_cap, compute_next_run, end_of_day

# The placeholder rule of the UI form (contextResolver); anything else in braces is template syntax
PLACEHOLDER = re.compile(r'{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}')
TEMPLATE_MARK = re.compile(r'{{|}}|{%|%}|{#|#}')

# Update fields where null clears the value; null elsewhere means "not sent"
CLEARABLE = {'description', 'run_mode', 'prompt', 'expires_at'}
NEXT_RUN_FIELDS = {'cron', 'timezone', 'enabled'}


class ScheduleError(Exception):
    """A rule the request broke; `response` is the (body, status) the API answers."""

    def __init__(self, body: dict, status: int = 400):
        super().__init__(body.get('error'))
        self.response = body, status


class ScheduleNotFound(ScheduleError):
    def __init__(self):
        super().__init__({'error': 'Schedule not found'}, 404)


def _field_error(field: str, message: str) -> ScheduleError:
    return ScheduleError({'error': message, 'field': field})


def prompt_placeholders(text: str | None) -> set[str]:
    """Return the names of the simple `{{name}}` placeholders of a prompt."""
    return set(PLACEHOLDER.findall(text or ''))


def check_prompt_syntax(text: str | None) -> None:
    """Raise a 400 on `prompt` when it holds template syntax other than simple placeholders.

    Raises:
        ScheduleError: When a `{{`, `}}`, `{%`, `%}`, `{#` or `#}` is left after the placeholders.
    """
    if TEMPLATE_MARK.search(PLACEHOLDER.sub('', text or '')):
        raise _field_error('prompt', 'Use only {{name}} placeholders in the prompt')


def check_run_input(entity_type: str, run_mode: str | None, prompt: str | None, variables: list[dict]) -> None:
    """Check the run mode, prompt and variables of one schedule against the field rules.

    Args:
        entity_type: `agent` or `pipeline`.
        run_mode: Null for an agent; `entry_point` or `prompt` for a pipeline.
        prompt: The prompt text, or null.
        variables: `{name, value}` dicts.

    Raises:
        ScheduleError: A 400 that names the broken field.
    """
    has_prompt = bool(prompt and prompt.strip())
    if entity_type == 'agent':
        if run_mode is not None:
            raise _field_error('run_mode', 'Agent schedules take no run mode')
        if not has_prompt:
            raise _field_error('prompt', 'Enter a prompt for the agent')
    elif run_mode == 'entry_point':
        if prompt or variables:
            raise _field_error('prompt' if prompt else 'variables', 'Entry point runs take no prompt or variables')
        return
    elif run_mode != 'prompt':
        raise _field_error('run_mode', 'Choose entry_point or prompt as the run mode of a pipeline')
    elif not has_prompt:
        raise _field_error('prompt', 'Enter a prompt for the pipeline')
    check_prompt_syntax(prompt)
    _check_variables(prompt, variables)


def _check_variables(prompt: str, variables: list[dict]) -> None:
    """Raise a 400 on `variables` unless their names equal the placeholder names of the prompt."""
    # lean: a repeated variable name passes; reject it when the run picks a value by position
    names = {variable['name'] for variable in variables}
    placeholders = prompt_placeholders(prompt)
    if names != placeholders:
        missing = ', '.join(sorted(placeholders - names)) or 'none'
        extra = ', '.join(sorted(names - placeholders)) or 'none'
        raise _field_error(
            'variables', f'Give one variable per prompt placeholder; missing: {missing}; extra: {extra}',
        )


def _check_entity(session, project_id: int, entity_type: str, application_id: int, version_id: int) -> None:
    """Raise unless the version belongs to the project entity, fits `entity_type` and the folder allows a write."""
    if session.get(Application, application_id) is None:
        raise _field_error('application_id', 'The agent or pipeline is not in this project')
    version = session.get(ApplicationVersion, version_id)
    if version is None or version.application_id != application_id:
        raise _field_error('version_id', 'The version does not belong to this agent or pipeline')
    if entity_type != ('pipeline' if version.agent_type == AgentTypes.pipeline else 'agent'):
        raise _field_error('entity_type', f'The version is not a {entity_type}')
    # The type is verified above, so the one folder type is exact
    access_error = entity_access_error(project_id, entity_type, application_id, write=True)
    if access_error:
        raise ScheduleError(*access_error)


def load_schedule_in_scope(session, schedule_id: int, scope: ScheduleScope) -> EntitySchedule:
    """Return the schedule when the caller owns it or holds `schedules.admin`.

    Raises:
        ScheduleNotFound: When no row is in the caller's scope; the 404 hides that the row exists.
    """
    query = select(EntitySchedule).where(EntitySchedule.id == schedule_id)
    if not scope.is_admin:
        query = query.where(EntitySchedule.created_by == scope.user_id)
    row = session.scalars(query).one_or_none()
    if row is None:
        raise ScheduleNotFound()
    return row


def mark_audit_span(schedule: EntitySchedule) -> None:
    """Name the schedule on the current trace span, so the tracing trail records the admin change."""
    span = otel_trace.get_current_span()
    span.set_attribute('entity.type', 'schedule')
    span.set_attribute('entity.id', str(schedule.id))
    span.set_attribute('entity.name', schedule.name)


def _is_admin_change(row: EntitySchedule, scope: ScheduleScope) -> bool:
    return scope.is_admin and row.created_by != scope.user_id


def _notify_owner(project_id: int, owner_id: int, schedule, action: str = 'edited') -> None:
    """Send the admin-change notice; a failure is logged and the committed change stays."""
    try:
        notify_schedule_event(
            project_id, owner_id, NotificationEventTypes.schedule_edited_by_admin, schedule, extra={'action': action},
        )
    except Exception as e:  # noqa: BLE001 -- the change is committed; a lost notice must not fail the save
        log.error('Schedule %s: cannot send the admin change notice: %s', schedule.id, e)


def create_schedule(
        session, project_id: int, body: ScheduleCreate, scope: ScheduleScope, key: UUID | None = None,
) -> EntitySchedule:
    """Store a new schedule owned by the caller and running as the caller; `key` sets the uuid (import only).

    Raises:
        ScheduleError: When an entity or run input rule fails.
        ScheduleLimitError: When an enabled schedule passes the project cap.
    """
    _check_entity(session, project_id, body.entity_type, body.application_id, body.version_id)
    values = body.model_dump()
    check_run_input(body.entity_type, body.run_mode, body.prompt, values['variables'])
    if body.enabled:
        check_enabled_cap(session)
    values['expires_at'] = body.expires_at and end_of_day(body.expires_at, body.timezone)
    row = EntitySchedule(
        **values,
        created_by=scope.user_id,
        updated_by=scope.user_id,
        principal={'kind': 'delegation', 'ref': scope.user_id},
        next_run=compute_next_run(body.cron, body.timezone, datetime.now(UTC)),
    )
    if key is not None:
        row.uuid = key
    session.add(row)
    session.commit()
    return row


def update_schedule(
        session, project_id: int, row: EntitySchedule, body: ScheduleUpdate, scope: ScheduleScope,
) -> EntitySchedule:
    """Apply an edit, pause or resume; the owner and the principal never change.

    Raises:
        ScheduleError: When a resume has no version, or an entity or run input rule fails.
        ScheduleLimitError: When a resume passes the project cap.
        pydantic.ValidationError: When the stop date does not fit the resulting cron and time zone.
    """
    changes = {
        key: value for key, value in body.model_dump(exclude_unset=True).items()
        if value is not None or key in CLEARABLE
    }
    resuming = changes.get('enabled') is True and not row.enabled
    if resuming and changes.get('version_id', row.version_id) is None:
        raise _field_error('version_id', 'Choose a version before you resume this schedule')
    if 'version_id' in changes:
        _check_entity(session, project_id, row.entity_type, row.application_id, changes['version_id'])
    check_run_input(
        row.entity_type,
        changes.get('run_mode', row.run_mode),
        changes.get('prompt', row.prompt),
        changes.get('variables', row.variables),
    )
    cron, timezone = changes.get('cron', row.cron), changes.get('timezone', row.timezone)
    if changes.get('expires_at'):
        # lean: a cron or zone change alone keeps the stored stop date unchecked; recheck it when that matters
        ScheduleTiming(cron=cron, timezone=timezone, expires_at=changes['expires_at'])
        changes['expires_at'] = end_of_day(changes['expires_at'], timezone)
    if resuming:
        check_enabled_cap(session)
        row.disabled_reason = None
    for key, value in changes.items():
        setattr(row, key, value)
    row.updated_by = scope.user_id
    if NEXT_RUN_FIELDS & changes.keys():
        row.next_run = compute_next_run(cron, timezone, datetime.now(UTC))
    admin_change = _is_admin_change(row, scope)
    if admin_change:
        mark_audit_span(row)
    session.commit()
    if admin_change:
        _notify_owner(project_id, row.created_by, row)
    return row


def delete_schedule(session, project_id: int, row: EntitySchedule, scope: ScheduleScope) -> None:
    """Delete the schedule; an admin deleting another owner's schedule is audited and the owner told."""
    admin_change = _is_admin_change(row, scope)
    if admin_change:
        mark_audit_span(row)
    owner_id, deleted = row.created_by, SimpleNamespace(id=row.id, name=row.name)  # read before the row is gone
    session.delete(row)
    session.commit()
    if admin_change:
        _notify_owner(project_id, owner_id, deleted, action='deleted')
