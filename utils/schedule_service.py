"""Create, update and delete of an entity schedule: owner, scope, audit and run input rules (EL-7011)."""
import json
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

from opentelemetry import trace as otel_trace
from pylon.core.tools import log
from sqlalchemy import column, inspect, select, table, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError

from ..models.all import Application, ApplicationVersion, EliteATool
from ..models.entity_schedule import REFUSAL_STREAK, EntitySchedule
from ..models.enums.all import AgentTypes, NotificationEventTypes
from ..models.pd.entity_schedule import ScheduleCreate, ScheduleTiming, ScheduleUpdate
from ..models.pd.index import _cron_gaps
from .folder_access import entity_access_error
from .schedule_access import INDEX_EDIT_PERMISSION, ScheduleScope, schedule_scope
from .schedule_notifications import notify_schedule_event
from .schedule_timing import check_enabled_cap, compute_next_run, end_of_day, min_interval_minutes_for

# The placeholder rule of the UI form (contextResolver); anything else in braces is template syntax
PLACEHOLDER = re.compile(r'{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}')
TEMPLATE_MARK = re.compile(r'{{|}}|{%|%}|{#|#}')

# Update fields where null clears the value; null elsewhere means "not sent"
CLEARABLE = {'description', 'run_mode', 'prompt', 'expires_at'}
NEXT_RUN_FIELDS = {'cron', 'timezone', 'enabled'}
CRON_ONLY_FIELDS = {'cron', 'timezone', 'expires_at'}
CONFIRMED_EVENT = 'schedule_run_without_asking_confirmed'
OWNER_CHANGED = 'owner_changed'
INVALID_INPUT = 'invalid_input'
TOOLKIT_INDEX = 'toolkit_index'
TOOLKIT_INDEX_UNIQUE = 'uq_entity_schedules_toolkit_index'
INDEX_SCHEDULED = 'This index already has a schedule'
INDEX_UNAVAILABLE = 'The index could not be checked; try again'
INDEX_READ_ONLY = 'You have read-only access to this index'
# A Toolkit Index run starts from the saved index settings: no version, input or approval of its own
INDEX_REFUSED_FIELDS = ('application_id', 'version_id', 'run_mode', 'prompt', 'risk_ack')


def refusal(code: str, text: str, status: int = 400, **extra) -> tuple[dict, int]:
    """The (body, status) of every Schedules refusal: a machine `code`, a human `error` and the extra keys (R8.1)."""
    return {'code': code, 'error': text, **extra}, status


class ScheduleError(Exception):
    """A rule the request broke; `response` is the refusal the API answers."""

    def __init__(self, code: str, text: str, status: int = 400, **extra):
        super().__init__(text)
        self.response = refusal(code, text, status, **extra)


class ScheduleNotFound(ScheduleError):
    def __init__(self):
        super().__init__('not_found', 'Schedule not found', 404)


def _field_error(field: str, message: str) -> ScheduleError:
    return ScheduleError(INVALID_INPUT, message, field=field)


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
        entity_type: `agent`, `pipeline` or `toolkit_index` (which takes no run input at all).
        run_mode: Null for an agent; `entry_point` or `prompt` for a pipeline.
        prompt: The prompt text, or null.
        variables: `{name, value}` dicts.

    Raises:
        ScheduleError: A 400 that names the broken field.
    """
    if entity_type == TOOLKIT_INDEX:
        for field, value in (('run_mode', run_mode), ('prompt', prompt)):
            if value is not None:
                raise _field_error(field, f'A Toolkit Index schedule takes no {field}')
        if variables:
            raise _field_error('variables', 'A Toolkit Index schedule takes no variables')
        return
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


def check_entity(session, project_id: int, entity_type: str, application_id: int | None, version_id: int | None,
                 toolkit_id: int | None = None, index_name: str | None = None) -> None:
    """Raise unless the target exists, fits `entity_type` and the folder allows a write.

    An agent or pipeline is the version of an application; a `toolkit_index` is an index of a toolkit that
    has no schedule yet, in `entity_schedules` or in the legacy `indexes_meta` of the toolkit.
    """
    if entity_type == TOOLKIT_INDEX:
        check_toolkit_index(session, project_id, toolkit_id, index_name)
        return
    if application_id is None or version_id is None:
        raise _field_error('application_id' if application_id is None else 'version_id', 'Field required')
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
        body, status = access_error
        raise ScheduleError('not_found' if status == 404 else 'read_only', body['error'], status)


def read_index_configuration(project_id: int, toolkit, index_name: str) -> dict | None:
    """The saved settings of one index of the toolkit, read from the toolkit's own vector store; None when the
    store has no such index. Read-only: no schema or table is created for a store that has none yet.

    Raises:
        Exception: When the connection string cannot be resolved or the store cannot be read.
    """
    # Late: the vector store helpers and the RPC tools load only for a Toolkit Index save
    from tools import rpc_tools

    from .application_tools import _get_pgvector_engine
    from .orphaned_index_schedules import pgvector_settings

    schema = str(toolkit.id)
    expanded = rpc_tools.RpcMixin().rpc.timeout(5).configurations_expand(
        project_id=project_id, settings=pgvector_settings(toolkit.settings),
        user_id=toolkit.author_id, unsecret=True,
    )
    connection_string = (expanded.get('pgvector_configuration') or {}).get('connection_string')
    if not connection_string:
        raise ValueError('the toolkit has no pgvector connection string')
    engine = _get_pgvector_engine(connection_string)
    if not inspect(engine).has_table('langchain_pg_embedding', schema=schema):
        return None
    # Core constructs, not SQL text: the schema name is quoted as an identifier and the index name is a bound value
    meta = table('langchain_pg_embedding', column('cmetadata', JSONB), schema=schema).c.cmetadata
    query = select(meta['index_configuration'].astext.label('configuration')).where(
        meta['type'].astext == 'index_meta', meta['collection'].astext == index_name).limit(1)
    with engine.connect() as conn:
        found = conn.execute(query).first()
    if found is None:
        return None
    configuration = found.configuration
    if isinstance(configuration, str):
        try:
            configuration = json.loads(configuration)
        except ValueError:
            configuration = None
    return configuration if isinstance(configuration, dict) else {}


def check_index_rights(project_id: int, toolkit_id: int | None, scope: ScheduleScope) -> None:
    """Raise unless the caller may write the folder of the toolkit and holds the index edit permission.

    The folder answers first, so a hidden toolkit stays a 404; a missing permission answers like a read-only folder.
    """
    from .folder_access import TOOLKIT_ENTITY_TYPES  # late: only an index schedule needs the toolkit folder types
    if toolkit_id is not None:
        access_error = entity_access_error(project_id, TOOLKIT_ENTITY_TYPES, toolkit_id, write=True)
        if access_error:
            body, status = access_error
            raise ScheduleError('not_found' if status == 404 else 'read_only', body['error'], status)
    if not scope.is_admin and INDEX_EDIT_PERMISSION not in scope.permissions:  # an admin needs no index edit grant
        raise ScheduleError('read_only', INDEX_READ_ONLY, 403)


def check_toolkit_index(session, project_id: int, toolkit_id: int | None, index_name: str | None) -> None:
    """Raise unless the index can get a schedule.

    Order: the toolkit exists, the caller may write its folder and holds the index edit permission (a hidden
    toolkit answers 404 before the index is looked at), the index exists and has saved settings, and nothing
    schedules the index yet.
    """
    if toolkit_id is None or not index_name:
        raise _field_error('toolkit_id' if toolkit_id is None else 'index_name', 'Field required')
    toolkit = session.get(EliteATool, toolkit_id)
    if toolkit is None:
        raise _field_error('toolkit_id', 'The toolkit is not in this project')
    check_index_rights(project_id, toolkit_id, schedule_scope(project_id))
    try:
        configuration = read_index_configuration(project_id, toolkit, index_name)
    except Exception as e:  # noqa: BLE001 -- an unreachable store is a retry for the caller, not a verdict
        log.error('Toolkit %s index check failed: %s', toolkit_id, type(e).__name__)
        raise ScheduleError('index_unavailable', INDEX_UNAVAILABLE, 503)
    if configuration is None:
        raise _field_error('index_name', 'The index is not in this toolkit')
    if not configuration:
        raise _field_error('index_name', 'This index has no saved settings. Run it once first.')
    legacy = ((toolkit.meta or {}).get('indexes_meta') or {}).get(index_name)
    has_row = session.scalar(select(EntitySchedule.id).where(
        EntitySchedule.toolkit_id == toolkit_id, EntitySchedule.index_name == index_name).limit(1)) is not None
    if has_row or (isinstance(legacy, dict) and legacy.get('schedules')):
        raise ScheduleError('index_scheduled', INDEX_SCHEDULED, 409)


def check_index_interval(cron: str | None) -> None:
    """Raise a 400 on `cron` when two runs of a Toolkit Index fall closer than its own floor."""
    floor = min_interval_minutes_for(TOOLKIT_INDEX)
    if cron is not None and min(_cron_gaps(cron)) < timedelta(minutes=floor):
        raise _field_error('cron', f'Runs must be at least {floor} minutes apart')


def check_index_body(body: ScheduleCreate) -> None:
    """Refuse what a Toolkit Index save cannot hold: a version, run input, a risk review or its own approval rule."""
    for field in INDEX_REFUSED_FIELDS:
        if getattr(body, field) is not None:
            raise _field_error(field, f'A Toolkit Index schedule takes no {field}')
    if body.variables:
        raise _field_error('variables', 'A Toolkit Index schedule takes no variables')
    if body.approval_rule != 'pause_and_ask':
        raise _field_error('approval_rule', 'A Toolkit Index schedule has no tool to approve')
    if body.trigger_kind != 'cron':
        raise _field_error('trigger_kind', 'A Toolkit Index schedule runs on a cron')
    check_index_interval(body.cron)


def check_schedule_body(session, project_id: int, body: ScheduleCreate) -> None:
    """The required-field rules of a Save and of a Test Run, in one place.

    The body model already needs name, entity, cron and time zone. This adds what a model cannot
    state: the entity and version exist and fit, and the starting input (prompt, run mode, variables) fits
    the entity type.

    Raises:
        ScheduleError: A 400, 403, 404 or 409 refusal that names the broken field.
    """
    if body.entity_type == TOOLKIT_INDEX:
        check_index_body(body)
    elif body.toolkit_id is not None or body.index_name is not None:
        raise _field_error('toolkit_id' if body.toolkit_id is not None else 'index_name',
                           'Only a Toolkit Index schedule takes a toolkit and an index')
    check_entity(session, project_id, body.entity_type, body.application_id, body.version_id,
                 body.toolkit_id, body.index_name)
    check_run_input(body.entity_type, body.run_mode, body.prompt, [v.model_dump() for v in body.variables])


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


def notify_owner(project_id: int, owner_id: int, schedule, action: str = 'edited') -> None:
    """Send the admin-change notice; a failure is logged and the committed change stays."""
    try:
        notify_schedule_event(
            project_id, owner_id, NotificationEventTypes.schedule_edited_by_admin, schedule, extra={'action': action},
        )
    except Exception as e:  # noqa: BLE001 -- the change is committed; a lost notice must not fail the save
        log.error('Schedule %s: cannot send the admin change notice: %s', schedule.id, e)


def _apply_confirmation(row: EntitySchedule, confirmed: dict | None, user_id: int) -> None:
    """Store who confirmed "Run without asking" and the fingerprint they saw; the rule "Pause and ask me" clears it."""
    if confirmed is not None:
        row.approval_confirmed_by, row.approval_confirmed_at = user_id, datetime.now(UTC)
        row.approval_fingerprint = confirmed['fingerprint']
    elif row.approval_rule == 'pause_and_ask':
        row.approval_confirmed_by = row.approval_confirmed_at = row.approval_fingerprint = None


def audit_schedule_event(project_id: int, row, user_id: int, event: str, detail: str = '') -> None:
    """One audit trail event: the tracing plugin writes a row for this span; never pass a secret in `detail`."""
    otel_trace.get_tracer(__name__).start_span(f'{event}: {detail}' if detail else event, attributes={
        'telemetry.data_type': 'schedule_execution', 'user.id': user_id, 'project.id': project_id,
        'entity.type': 'schedule', 'entity.id': str(row.id), 'entity.name': row.name,
    }).end()


def _audit_confirmation(project_id: int, row: EntitySchedule, user_id: int, confirmed: dict) -> None:
    tools = ', '.join(sorted(f"{item['toolkit']}.{item['tool'] or '*'}" for item in confirmed['sensitive']))
    audit_schedule_event(project_id, row, user_id, CONFIRMED_EVENT, tools)


def _is_webhook(row) -> bool:
    return row.trigger_kind == 'webhook'


def check_webhook_owner_edit(row: EntitySchedule, scope: ScheduleScope, changes) -> None:
    """Raise 403 unless the owner edits a webhook; an admin may only pause, or resume when no owner change paused it."""
    if not _is_webhook(row) or row.created_by == scope.user_id:
        return
    if set(changes) - {'enabled'}:
        raise ScheduleError('not_owner', 'Only the owner can edit this webhook', 403)
    if changes.get('enabled') is True and row.disabled_reason == OWNER_CHANGED:
        raise ScheduleError('not_owner', 'Only the new owner can switch this webhook on again', 403)


def create_schedule(
        session, project_id: int, body: ScheduleCreate, scope: ScheduleScope, key: UUID | None = None,
        confirmed: dict | None = None,
) -> EntitySchedule:
    """Store a new schedule owned by the caller and running as the caller; `key` sets the uuid (import only).

    `confirmed` is the risk review the caller confirmed for "Run without asking"; it is recorded and audited.
    A webhook row gets its secret in the same transaction; a generated one is set on `row.new_secret` once.

    Raises:
        ScheduleError: When an entity or run input rule fails.
        ScheduleLimitError: When an enabled schedule passes the project cap.
    """
    check_schedule_body(session, project_id, body)
    values = body.model_dump(exclude={'risk_ack', 'signing_secret_value'})
    cron_row = body.trigger_kind == 'cron'
    if cron_row and body.enabled:
        check_enabled_cap(session)
    values['expires_at'] = body.expires_at and end_of_day(body.expires_at, body.timezone)
    row = EntitySchedule(
        **values,
        created_by=scope.user_id,
        updated_by=scope.user_id,
        principal={'kind': 'delegation', 'ref': scope.user_id},
        next_run=compute_next_run(body.cron, body.timezone, datetime.now(UTC)) if cron_row and body.enabled else None,
    )
    if key is not None:
        row.uuid = key
    _apply_confirmation(row, confirmed, scope.user_id)
    if body.entity_type == TOOLKIT_INDEX:
        _insert_index_row(session, row)
    else:
        session.add(row)
    if not cron_row:
        _store_first_secret(session, project_id, row, body.signing_secret_value)
    try:
        session.commit()
    except Exception:
        if not cron_row:  # the row is gone: its secret must not stay behind
            session.rollback()
            _drop_row_secrets(session, project_id, None, row.uuid)
        raise
    if confirmed is not None:
        _audit_confirmation(project_id, row, scope.user_id, confirmed)
    if not cron_row:
        audit_schedule_event(project_id, row, scope.user_id, 'webhook_created', row.webhook_type)
    return row


def _insert_index_row(session, row: EntitySchedule) -> None:
    """Add a Toolkit Index row in a savepoint: a second save of the same index loses the race and gets a 409.

    The check before the insert reads the rows, but two saves can both pass it; the partial unique index decides.
    """
    from .schedule_test_runs import UNIQUE_VIOLATION, error_code  # late: that module imports the body models
    try:
        with session.begin_nested():
            session.add(row)
    except IntegrityError as e:
        if error_code(e) == UNIQUE_VIOLATION and _is_index_conflict(e):
            raise ScheduleError('index_scheduled', INDEX_SCHEDULED, 409) from e
        raise


def _is_index_conflict(error: IntegrityError) -> bool:
    """True when the unique violation is the one of `(toolkit_id, index_name)`, not of another unique key."""
    orig = getattr(error, 'orig', None)
    constraint = getattr(getattr(orig, 'diag', None), 'constraint_name', None)
    if constraint:
        return constraint == TOOLKIT_INDEX_UNIQUE
    return 'toolkit_id' in str(orig)  # SQLite (the unit tests) names the columns, not the index


def _store_first_secret(session, project_id: int, row: EntitySchedule, signing_value: str | None) -> None:
    """Write the secret under the row uuid before the commit; a vault failure rolls the row back."""
    from .webhook_secrets import signing_value_error, store_row_secret  # late: cron-only loaders skip vault
    if signing_value is not None:
        error = signing_value_error(signing_value)
        if error:
            raise _field_error('signing_secret_value', error)
    if row.uuid is None:
        session.flush()  # the uuid default is applied on insert
    try:
        value = store_row_secret(session, project_id, row, signing_value)
    except Exception as e:  # noqa: BLE001 -- no row may stay without its secret
        session.rollback()
        log.error('Webhook create: cannot store the secret: %s', type(e).__name__)
        raise ScheduleError('secret_store_unavailable', 'The webhook secret could not be stored; try again', 503)
    row.new_secret = None if signing_value else value  # shown once, never stored on the row


def update_changes(body: ScheduleUpdate) -> dict:
    """The fields an update sets; null means "not sent" except on the clearable fields."""
    return {
        key: value for key, value in body.model_dump(exclude_unset=True, exclude={'risk_ack'}).items()
        if value is not None or key in CLEARABLE
    }


def _check_index_changes(row: EntitySchedule, body: ScheduleUpdate, changes: dict) -> None:
    """Refuse an edit a Toolkit Index row cannot hold, before any check that reads its missing application.

    A version or an approval rule the form sends back unchanged is dropped, so a form that sends the whole body
    still saves. The toolkit and the index are not edit fields of the body at all.
    """
    if changes.get('version_id') is not None:
        raise _field_error('version_id', 'A Toolkit Index schedule takes no version_id')
    if body.risk_ack is not None:
        raise _field_error('risk_ack', 'A Toolkit Index schedule takes no risk_ack')
    if changes.get('approval_rule', row.approval_rule) != row.approval_rule:
        raise _field_error('approval_rule', 'A Toolkit Index schedule has no tool to approve')
    changes.pop('version_id', None)
    changes.pop('approval_rule', None)
    if changes.get('cron') is not None:
        check_index_interval(changes['cron'])


def update_schedule(
        session, project_id: int, row: EntitySchedule, body: ScheduleUpdate, scope: ScheduleScope,
        confirmed: dict | None = None,
) -> EntitySchedule:
    """Apply an edit, pause or resume; the owner and the principal never change.

    `confirmed` is the risk review the caller confirmed for "Run without asking"; it is recorded and audited.
    A change back to "Pause and ask me" clears the recorded confirmation.

    Raises:
        ScheduleError: When a resume has no version, or an entity or run input rule fails.
        ScheduleLimitError: When a resume passes the project cap.
        pydantic.ValidationError: When the stop date does not fit the resulting cron and time zone.
    """
    changes = update_changes(body)
    check_webhook_owner_edit(row, scope, changes)
    cron_row = row.trigger_kind == 'cron'
    set_cron_fields = sorted(k for k in CRON_ONLY_FIELDS & changes.keys() if changes[k] is not None)
    if not cron_row and set_cron_fields:
        raise _field_error(set_cron_fields[0], 'A webhook has no cron, time zone or stop date')
    is_index = row.entity_type == TOOLKIT_INDEX
    if is_index:
        if not scope.is_admin:  # an admin changes any row; the owner needs the rights a save needs
            check_index_rights(project_id, row.toolkit_id, scope)
        _check_index_changes(row, body, changes)
    resuming = changes.get('enabled') is True and not row.enabled
    # A Toolkit Index runs the saved index settings and has no version to choose
    if resuming and not is_index and changes.get('version_id', row.version_id) is None:
        raise _field_error('version_id', 'Choose a version before you resume this schedule')
    if 'version_id' in changes:
        check_entity(session, project_id, row.entity_type, row.application_id, changes['version_id'])
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
        if cron_row:
            check_enabled_cap(session)
        row.disabled_reason = None
        # a resume starts the governance refusal count again (#7254); the column is not mapped
        session.execute(update(EntitySchedule).where(EntitySchedule.id == row.id).values({REFUSAL_STREAK: 0}))
    for key, value in changes.items():
        setattr(row, key, value)
    row.updated_by = scope.user_id
    _apply_confirmation(row, confirmed, scope.user_id)
    if cron_row and NEXT_RUN_FIELDS & changes.keys():
        # A paused schedule has no next run; a resume sets it again from the cron and time zone
        row.next_run = compute_next_run(cron, timezone, datetime.now(UTC)) if row.enabled else None
    admin_change = _is_admin_change(row, scope)
    if admin_change:
        mark_audit_span(row)
    session.commit()
    if confirmed is not None:
        _audit_confirmation(project_id, row, scope.user_id, confirmed)
    if not cron_row and 'approval_rule' in changes:
        audit_schedule_event(project_id, row, scope.user_id, 'webhook_approval_rule_changed', row.approval_rule)
    if admin_change:
        notify_owner(project_id, row.created_by, row)
    return row


def delete_schedule(session, project_id: int, row: EntitySchedule, scope: ScheduleScope) -> None:
    """Delete the schedule; an admin deleting another owner's schedule is audited and the owner told."""
    if row.entity_type == TOOLKIT_INDEX and not scope.is_admin:  # the owner needs the rights a save needs
        check_index_rights(project_id, row.toolkit_id, scope)
    admin_change = _is_admin_change(row, scope)
    if admin_change:
        mark_audit_span(row)
    owner_id, deleted = row.created_by, SimpleNamespace(id=row.id, name=row.name)  # read before the row is gone
    webhook, uuid = _is_webhook(row), row.uuid
    session.delete(row)
    session.commit()
    if webhook:  # after the commit: a failed delete must keep the row's secret
        _drop_row_secrets(session, project_id, deleted.id, uuid)
        audit_schedule_event(project_id, deleted, scope.user_id, 'webhook_deleted')
    if admin_change:
        notify_owner(project_id, owner_id, deleted, action='deleted')


def _drop_row_secrets(session, project_id: int, schedule_id: int, uuid) -> None:
    """Remove a deleted row's vault keys; a failure leaves an orphan key that no row points to."""
    from .webhook_secrets import drop_secrets_of  # late: cron-only loaders skip vault
    try:
        drop_secrets_of(session, project_id, [uuid])
    except Exception as e:  # noqa: BLE001 -- the row is already deleted
        session.rollback()
        log.error('Schedule %s: cannot remove the webhook secrets: %s', schedule_id, type(e).__name__)
