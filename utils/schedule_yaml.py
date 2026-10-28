"""Schedules as a YAML file (EL-7011): the export writes a fixed allow-list of fields, never the stored row.

The import updates the schedule an item matches, by key or else by natural key, through the update path of the
schedule service; an unmatched item is created disabled, through the create path."""
from datetime import UTC
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from tools import db
from yaml import AliasEvent, YAMLError, parse

from ..models.all import Application, ApplicationVersion
from ..models.entity_schedule import EntitySchedule
from .pipeline_utils import from_str_to_yaml, validate_yaml_from_str
from .schedule_service import ScheduleError
from .schedule_timing import end_of_day

FILE_KIND = 'elitea_schedules'
SCHEMA_VERSION = 2
MAX_FILE_BYTES = 1024 * 1024
MAX_ITEMS = 500
NOT_YAML = 'The schedule file is not valid YAML'
TOO_LARGE = 'The schedule file is larger than 1 MB'
TOO_MANY = f'The schedule file has more than {MAX_ITEMS} schedules'
SAVE_FAILED = 'The schedule could not be saved'
REPEATED_KEY = 'The key is used by an earlier schedule of this file'
OTHER_ENTITY = 'The key belongs to a schedule of another agent or pipeline'
UNCONFIRMED_NOTE = ('Sensitive tools need a confirmation in the schedule form, or some tools could not be '
                    'checked; saved as Pause and ask me')
CHAT_NOT_IMPORTED = 'The target chat is not imported; the schedule '
UPDATE_FIELDS = (
    'description', 'run_mode', 'prompt', 'variables', 'cron', 'timezone', 'expires_at', 'approval_rule',
)  # never enabled or name: an update keeps them (R5.1)


def _delivery(row: EntitySchedule) -> dict:
    """The delivery object in the shape of the API body; the chat target is private, so its fields stay empty."""
    return {
        'mode': row.delivery_mode,
        'conversation_uuid': None,
        'participant_id': None,
        'include_history': False,  # no stored column yet; the key keeps the file shape fixed
    }


def _item(row: EntitySchedule, entity_name: str, version_name: str | None) -> dict:
    """One allow-listed file item; the keys are the file format, in file order."""
    is_entry_point = row.run_mode == 'entry_point'
    return {
        'key': str(row.uuid),
        'name': row.name,
        'description': row.description,
        'entity_type': row.entity_type,
        'entity_name': entity_name,
        'version_name': version_name,
        'run_mode': row.run_mode if row.entity_type == 'pipeline' else None,
        'cron': row.cron,
        'timezone': row.timezone,
        'prompt': None if is_entry_point else row.prompt,
        'variables': [] if is_entry_point else [{'name': v['name'], 'value': v['value']} for v in row.variables],
        # Stored as the end of the local stop day in UTC; the local date makes the import keep the day
        'expires_at': row.expires_at and row.expires_at.astimezone(ZoneInfo(row.timezone)).date(),
        'approval_rule': row.approval_rule,
        'delivery': _delivery(row),
    }


def export_schedules_yaml(project_id: int, ids: list[int] | None) -> str:
    """The YAML file text of every schedule of the project, or of `ids`; unknown ids raise a 400."""
    query = (
        select(EntitySchedule, Application.name, ApplicationVersion.name)
        .join(Application, Application.id == EntitySchedule.application_id)
        .outerjoin(ApplicationVersion, ApplicationVersion.id == EntitySchedule.version_id)
        .order_by(EntitySchedule.id)
    )
    if ids is not None:
        query = query.where(EntitySchedule.id.in_(ids))
    with db.get_session(project_id) as session:
        found = session.execute(query.limit(MAX_ITEMS + 1)).all()
    if len(found) > MAX_ITEMS:  # the import takes at most MAX_ITEMS, so a bigger file could not come back
        raise ScheduleError('too_many_schedules', f'{TOO_MANY}; export them in parts by ids')
    unknown = sorted(set(ids or []) - {row.id for row, *_ in found})
    if unknown:
        raise ScheduleError('invalid_input', f'Unknown schedule ids: {", ".join(map(str, unknown))}', field='ids')
    return from_str_to_yaml({
        'kind': FILE_KIND,
        'schema_version': SCHEMA_VERSION,
        'schedules': [_item(*columns) for columns in found],
    })


def _reject_aliases(text: str) -> None:
    """Raise the not-YAML error for an anchor reference: a small file of nested aliases expands to a huge one."""
    try:
        if any(isinstance(event, AliasEvent) for event in parse(text)):
            raise ValueError(NOT_YAML)
    except YAMLError:
        raise ValueError(NOT_YAML) from None


def parse_schedules_file(text: str):
    """Return the checked file; the items stay raw dicts, so one bad item fails alone.

    Raises:
        ValueError: With a schedule message when the text is too large, not a YAML mapping, has a wrong header
            or has too many items.
    """
    # late: the export loads without the import-only modules
    from ..models.pd.schedule_yaml import WRONG_HEADER, ScheduleFile
    if len(text.encode()) > MAX_FILE_BYTES:
        raise ValueError(TOO_LARGE)
    _reject_aliases(text)
    try:
        data = validate_yaml_from_str(text)
    except ValueError:  # the helper words its errors for pipelines
        raise ValueError(NOT_YAML) from None
    try:
        file = ScheduleFile.model_validate(data)
    except ValidationError as e:
        first = e.errors()[0]
        if first['loc'] in (('kind',), ('schema_version',)):  # a missing key reads 'Field required'
            raise ValueError(WRONG_HEADER) from None
        raise ValueError(_reason(e)) from None
    if len(file.schedules) > MAX_ITEMS:
        raise ValueError(TOO_MANY)
    return file


def _reason(error) -> str:
    """The first broken field of a pydantic error and its plain message."""
    first = error.errors()[0]
    message = first['msg'].removeprefix('Value error, ')
    return f'{first["loc"][0]}: {message}' if first['loc'] else message


class _Skipped(Exception):
    """The item matches no single entity version of the project."""


def _entity_index(session, names: set) -> dict:
    """Map (entity_type, entity_name) to its (application_id, version_id, version_name) rows; one query."""
    index = {}
    rows = session.execute(
        select(Application.id, Application.name, ApplicationVersion.id, ApplicationVersion.name, ApplicationVersion.agent_type)
        .join(ApplicationVersion, ApplicationVersion.application_id == Application.id)
        .where(Application.name.in_(names))
    ).all()
    for application_id, name, version_id, version_name, agent_type in rows:
        kind = 'pipeline' if agent_type == 'pipeline' else 'agent'
        index.setdefault((kind, name), []).append((application_id, version_id, version_name))
    return index


def _resolve(index: dict, item) -> tuple[int, int]:
    """Return (application_id, version_id) of the one entity and version the item names, or raise _Skipped."""
    kind = item.entity_type.capitalize()
    versions = index.get((item.entity_type, item.entity_name), [])
    if not versions:
        raise _Skipped(f"{kind} '{item.entity_name}' not found in this project")
    if len({application_id for application_id, *_ in versions}) > 1:
        raise _Skipped(f"{kind} '{item.entity_name}' found more than once")
    if item.version_name is None:
        raise _Skipped(f"Version of {item.entity_type} '{item.entity_name}' is not named")
    matches = [(application_id, version_id) for application_id, version_id, name in versions if name == item.version_name]
    if not matches:
        raise _Skipped(f"Version '{item.version_name}' of {item.entity_type} '{item.entity_name}' not found")
    if len(matches) > 1:
        raise _Skipped(f"Version '{item.version_name}' of {item.entity_type} '{item.entity_name}' found more than once")
    return matches[0]


def _schedule_index(session, file) -> tuple[dict, dict]:
    """Map the uuid, and the (application_id, version_id, name), to the stored schedules the file can match; one query."""
    keys, names = set(), set()
    for raw in file.schedules:
        try:
            keys.add(UUID(str(raw['key'])))
        except (KeyError, ValueError):
            pass  # no key or a bad one; the item model reports a bad key
        if isinstance(raw.get('name'), str):
            names.add(raw['name'])
    by_key, by_name = {}, {}
    for row in session.scalars(
            select(EntitySchedule).where(or_(EntitySchedule.uuid.in_(keys), EntitySchedule.name.in_(names)))
    ):
        by_key[row.uuid] = row
        by_name.setdefault((row.application_id, row.version_id, row.name), []).append(row)
    return by_key, by_name


def _match(by_key: dict, by_name: dict, seen_keys: set, item, application_id: int, version_id: int):
    """Return the stored schedule the item updates, or None; raise _Skipped or ScheduleError when it cannot."""
    if item.key is not None:
        if item.key in seen_keys:
            raise ScheduleError('repeated_key', REPEATED_KEY)
        seen_keys.add(item.key)
        row = by_key.get(item.key)
        if row is not None and (row.application_id, row.entity_type) != (application_id, item.entity_type):
            raise ScheduleError('other_entity', OTHER_ENTITY)
        return row
    rows = by_name.get((application_id, version_id, item.name), [])
    if len(rows) > 1:
        raise _Skipped(f"Schedule '{item.name}' found more than once")
    return rows[0] if rows else None


def _unchanged(row: EntitySchedule, values: dict, version_id: int) -> bool:
    """True when the update would store what the row holds: the stop date as the service stores it."""
    expires_at = values['expires_at'] and end_of_day(values['expires_at'], values['timezone'])
    stored = row.expires_at
    if stored is not None and stored.tzinfo is None:  # a driver without time zones returns naive UTC
        stored = stored.replace(tzinfo=UTC)
    return row.version_id == version_id and stored == expires_at and all(
        getattr(row, field) == values[field] for field in UPDATE_FIELDS if field != 'expires_at'
    )


def _review_item(project_id: int, owner_id: int, version_id: int, values: dict,
                 reviews: dict | None = None) -> str | None:
    """Apply the risk review to one item as the owner who will run it; return the note of a changed rule.

    An import never carries an acknowledgement (owner CR 2026-10-21): "Run without asking" with sensitive
    tools, or with tools that could not be checked, becomes "Pause and ask me".

    Raises:
        ScheduleError: When a blocking finding refuses the item; the error names the findings.
    """
    # late: the export loads without the scan
    from .schedule_risk import scan_complete, scan_schedule
    # `reviews` holds the scans of one import by version and owner, so items of one version scan once
    if reviews is None:
        reviews = {}
    if (version_id, owner_id) not in reviews:
        reviews[(version_id, owner_id)] = scan_schedule(project_id, version_id, owner_id)
    review = reviews[(version_id, owner_id)]
    if review['blocking']:
        raise ScheduleError('schedule_blocked', '; '.join(finding['message'] for finding in review['blocking']))
    if values['approval_rule'] == 'run_without_asking' and (review['sensitive'] or not scan_complete(review)):
        values['approval_rule'] = 'pause_and_ask'
        return UNCONFIRMED_NOTE
    return None


def _save(session, project_id: int, scope, indexes: tuple, item, values: dict, ids: tuple[int, int],
          reviews: dict | None = None):
    """Update the schedule the item matches, else create a disabled one; return (group, row, note).

    The group is 'updated' or 'created'; the note is set when the risk review changed the rule.
    """
    # late: the export loads without these modules
    from ..models.pd.entity_schedule import ScheduleCreate, ScheduleUpdate
    from .schedule_service import create_schedule, update_schedule
    by_key, by_name, _ = indexes
    application_id, version_id = ids
    row = _match(*indexes, item, *ids)
    note = _review_item(project_id, scope.user_id if row is None else row.created_by, version_id, values, reviews)
    if row is not None and _unchanged(row, values, version_id):
        return 'updated', row, note  # no write, so no admin-edit notice and no new updated_by or next_run (R5.5)
    if row is not None:
        old_key = (row.application_id, row.version_id, row.name)
        body = ScheduleUpdate(**{field: values[field] for field in UPDATE_FIELDS}, version_id=version_id)
        update_schedule(session, project_id, row, body, scope)
        if row.version_id != old_key[1]:  # a later keyless item of the file finds the row under its new version
            by_name[old_key].remove(row)
            by_name.setdefault((row.application_id, row.version_id, row.name), []).append(row)
        return 'updated', row, note
    body = ScheduleCreate(**values, application_id=application_id, version_id=version_id, enabled=False)
    row = create_schedule(session, project_id, body, scope, key=item.key)
    by_key[row.uuid] = row  # a later item of the file then updates this row
    by_name.setdefault((application_id, version_id, row.name), []).append(row)
    return 'created', row, note


def import_schedules(project_id: int, admin_id: int, file) -> dict:
    """Update the schedule each item matches, else create a disabled one owned by the admin; return the report.

    The risk review checks each item: a blocking finding fails the item with the reason, and "Run without
    asking" with sensitive tools is saved as "Pause and ask me" with a note (an import never confirms).

    Args:
        project_id: The target project.
        admin_id: The importing admin; owner and principal of every created row.
        file: The `ScheduleFile` from `parse_schedules_file`.

    Returns:
        `{created, updated, skipped, failed}`; every entry has the item name, a created or updated one the
        `schedule_id` and a `note` when the approval rule changed or a chat delivery was not imported, a
        skipped or failed one the `reason`.
    """
    # late: the export loads without these modules
    from ..models.pd.schedule_yaml import ScheduleFileItem
    from .schedule_access import ScheduleScope
    from .schedule_settings import is_run_without_asking_allowed
    scope = ScheduleScope(user_id=admin_id, is_admin=True, permissions=frozenset())
    may_run_without_asking = is_run_without_asking_allowed(project_id)
    report = {'created': [], 'updated': [], 'skipped': [], 'failed': []}
    reviews = {}
    names = {raw['entity_name'] for raw in file.schedules if isinstance(raw.get('entity_name'), str)}
    with db.get_session(project_id) as session:
        index = _entity_index(session, names)
        indexes = (*_schedule_index(session, file), set())  # by uuid, by natural key, keys seen in the file
        for raw in file.schedules:
            entry = {'name': raw.get('name')}
            try:
                item = ScheduleFileItem.model_validate(raw)
                ids = _resolve(index, item)
                values = item.model_dump(exclude={'key', 'entity_name', 'version_name', 'delivery'})
                if item.approval_rule == 'run_without_asking' and not may_run_without_asking:
                    values['approval_rule'] = 'pause_and_ask'
                    entry['note'] = 'Run without asking is not allowed in this project; saved as Pause and ask me'
                group, row, note = _save(session, project_id, scope, indexes, item, values, ids, reviews)
                if note:
                    entry['note'] = note
                if item.delivery and item.delivery.mode == 'conversation':
                    where = 'delivers to the run history' if row.delivery_mode == 'run_history' else (
                        'keeps its stored delivery')
                    entry['note'] = '; '.join(filter(None, (entry.get('note'), CHAT_NOT_IMPORTED + where)))
                entry.update(name=row.name, schedule_id=row.id)  # a key match keeps the stored name
                report[group].append(entry)
            except _Skipped as e:
                report['skipped'].append({**entry, 'reason': str(e)})
            except ValidationError as e:
                report['failed'].append({**entry, 'reason': _reason(e)})
            except ScheduleError as e:
                session.rollback()
                report['failed'].append({**entry, 'reason': e.response[0]['error']})
            except SQLAlchemyError:
                session.rollback()
                report['failed'].append({**entry, 'reason': SAVE_FAILED})
    return report
