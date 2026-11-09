"""Schedules as a YAML file (EL-7011): the export writes a fixed allow-list of fields, never the stored row.

The import updates the schedule an item matches, by key or else by natural key, through the update path of the
schedule service; an unmatched item is created disabled, through the create path."""
from collections.abc import Callable
from datetime import UTC
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from tools import db
from yaml import AliasEvent, YAMLError, parse

from ..models.all import Application, ApplicationVersion, EliteATool
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
WEBHOOK_KEY = 'The key belongs to a webhook; webhooks are not imported'
UNCONFIRMED_NOTE = ('Sensitive tools need a confirmation in the schedule form, or some tools could not be '
                    'checked; saved as Pause and ask me')
OTHER_INDEX_ENTITY = 'The key belongs to a schedule of another index, agent or pipeline'
UPDATE_FIELDS = (
    'description', 'run_mode', 'prompt', 'variables', 'cron', 'timezone', 'expires_at', 'approval_rule',
)  # never enabled or name: an update keeps them (R5.1)
# A Toolkit Index has no version, run input or tool approval; the file names it by toolkit name and index name
TOOLKIT_INDEX = 'toolkit_index'  # the value of schedule_service.TOOLKIT_INDEX; the export test loads this file without it
INDEX_UPDATE_FIELDS = ('description', 'cron', 'timezone', 'expires_at')


def _stop_day(row: EntitySchedule):
    """Stored as the end of the local stop day in UTC; the local date makes the import keep the day."""
    return row.expires_at and row.expires_at.astimezone(ZoneInfo(row.timezone)).date()


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
        'expires_at': _stop_day(row),
        'approval_rule': row.approval_rule,
    }


def _index_item(row: EntitySchedule, toolkit_name: str) -> dict:
    """One Toolkit Index item: the toolkit and the index by name, no ids, and none of the run fields."""
    return {
        'key': str(row.uuid),
        'name': row.name,
        'description': row.description,
        'entity_type': row.entity_type,
        'entity_name': toolkit_name,
        'index_name': row.index_name,
        'cron': row.cron,
        'timezone': row.timezone,
        'expires_at': _stop_day(row),
    }


def export_schedules_yaml(project_id: int, ids: list[int] | None) -> str:
    """The YAML file text of every schedule of the project, or of `ids`; unknown ids raise a 400."""
    query = (
        select(EntitySchedule, func.coalesce(Application.name, EliteATool.name), ApplicationVersion.name)
        .outerjoin(Application, Application.id == EntitySchedule.application_id)
        .outerjoin(EliteATool, EliteATool.id == EntitySchedule.toolkit_id)
        .outerjoin(ApplicationVersion, ApplicationVersion.id == EntitySchedule.version_id)
        .where(EntitySchedule.trigger_kind == 'cron', or_(Application.id.isnot(None), EliteATool.id.isnot(None)))
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
        'schedules': [
            _index_item(row, entity_name) if row.entity_type == TOOLKIT_INDEX else _item(row, entity_name, version_name)
            for row, entity_name, version_name in found
        ],
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


def _toolkit_index(session, names: set) -> dict:
    """Map a toolkit name to the toolkits of the project that bear it; one query."""
    toolkits = {}
    if names:
        for toolkit in session.scalars(select(EliteATool).where(EliteATool.name.in_(names))):
            toolkits.setdefault(toolkit.name, []).append(toolkit)
    return toolkits


def _resolve_toolkit(toolkits: dict, item):
    """Return the one toolkit the Toolkit Index item names, or raise _Skipped."""
    found = toolkits.get(item.entity_name, [])
    if not found:
        raise _Skipped(f"Toolkit '{item.entity_name}' not found in this project")
    if len(found) > 1:
        raise _Skipped(f"Toolkit '{item.entity_name}' found more than once")
    return found[0]


def _schedule_index(session, file, toolkit_ids: set) -> tuple[dict, dict, dict]:
    """Map the uuid, the (application_id, version_id, name) and the (toolkit_id, index_name) to the stored
    schedules the file can match; one query."""
    keys, names = set(), set()
    for raw in file.schedules:
        try:
            keys.add(UUID(str(raw['key'])))
        except (KeyError, ValueError):
            pass  # no key or a bad one; the item model reports a bad key
        if isinstance(raw.get('name'), str):
            names.add(raw['name'])
    by_key, by_name, by_index = {}, {}, {}
    for row in session.scalars(select(EntitySchedule).where(or_(
            EntitySchedule.uuid.in_(keys), EntitySchedule.name.in_(names), EntitySchedule.toolkit_id.in_(toolkit_ids),
    ))):
        by_key[row.uuid] = row
        if row.trigger_kind != 'cron':
            continue
        if row.entity_type == TOOLKIT_INDEX:
            by_index[(row.toolkit_id, row.index_name)] = row
            continue
        by_name.setdefault((row.application_id, row.version_id, row.name), []).append(row)
    return by_key, by_name, by_index


def _row_by_key(by_key: dict, seen_keys: set, key: UUID, belongs: Callable, other_text: str):
    """Return the stored schedule of the item key, or None; refuse a repeated key, a webhook and a foreign target."""
    if key in seen_keys:
        raise ScheduleError('repeated_key', REPEATED_KEY)
    seen_keys.add(key)
    row = by_key.get(key)
    if row is not None and row.trigger_kind != 'cron':
        raise ScheduleError('webhook_key', WEBHOOK_KEY)
    if row is not None and not belongs(row):
        raise ScheduleError('other_entity', other_text)
    return row


def _match(by_key: dict, by_name: dict, seen_keys: set, item, application_id: int, version_id: int):
    """Return the stored schedule the item updates, or None; raise _Skipped or ScheduleError when it cannot."""
    if item.key is not None:
        return _row_by_key(
            by_key, seen_keys, item.key,
            lambda row: (row.application_id, row.entity_type) == (application_id, item.entity_type), OTHER_ENTITY,
        )
    rows = by_name.get((application_id, version_id, item.name), [])
    if len(rows) > 1:
        raise _Skipped(f"Schedule '{item.name}' found more than once")
    return rows[0] if rows else None


def _match_index(by_key: dict, by_index: dict, seen_keys: set, item, toolkit_id: int):
    """Return the stored schedule of the item key, else of its index (one row per index), or None."""
    if item.key is not None:
        return _row_by_key(
            by_key, seen_keys, item.key,
            lambda row: (row.entity_type, row.toolkit_id, row.index_name) == (TOOLKIT_INDEX, toolkit_id, item.index_name),
            OTHER_INDEX_ENTITY,
        )
    return by_index.get((toolkit_id, item.index_name))


def _unchanged(row: EntitySchedule, values: dict, version_id: int | None, fields: tuple = UPDATE_FIELDS) -> bool:
    """True when the update would store what the row holds: the stop date as the service stores it."""
    expires_at = values['expires_at'] and end_of_day(values['expires_at'], values['timezone'])
    stored = row.expires_at
    if stored is not None and stored.tzinfo is None:  # a driver without time zones returns naive UTC
        stored = stored.replace(tzinfo=UTC)
    return row.version_id == version_id and stored == expires_at and all(
        getattr(row, field) == values[field] for field in fields if field != 'expires_at'
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
    rule = values['approval_rule']
    if (version_id, owner_id, rule) not in reviews:
        reviews[(version_id, owner_id, rule)] = scan_schedule(project_id, version_id, owner_id, rule)
    review = reviews[(version_id, owner_id, rule)]
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
    by_key, by_name, seen_keys, _ = indexes
    application_id, version_id = ids
    row = _match(by_key, by_name, seen_keys, item, *ids)
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


def _save_index(session, project_id: int, scope, indexes: tuple, item, values: dict, toolkit):
    """Update the Toolkit Index schedule the item matches, else create a disabled one; return (group, row).

    No risk review: an index runs saved settings, not tools. A new row needs the index in the toolkit's own store
    (a skip with the reason); the service then checks the toolkit folder rights and the one-schedule-per-index rule.
    """
    # late: the export loads without these modules
    from ..models.pd.entity_schedule import ScheduleCreate, ScheduleUpdate
    from .schedule_service import INDEX_UNAVAILABLE, create_schedule, read_index_configuration, update_schedule
    by_key, _, seen_keys, by_index = indexes
    row = _match_index(by_key, by_index, seen_keys, item, toolkit.id)
    if row is not None and _unchanged(row, values, None, INDEX_UPDATE_FIELDS):
        return 'updated', row  # no write, so no admin-edit notice and no new updated_by or next_run
    if row is not None:
        update_schedule(session, project_id, row, ScheduleUpdate(**{f: values[f] for f in INDEX_UPDATE_FIELDS}), scope)
        return 'updated', row
    try:
        configuration = read_index_configuration(project_id, toolkit, item.index_name)
    except Exception as e:  # noqa: BLE001 -- an unreachable store is a retry for the caller, not a verdict
        raise ScheduleError('index_unavailable', INDEX_UNAVAILABLE, 503) from e
    if configuration is None:
        raise _Skipped(f"Index '{item.index_name}' not found in toolkit '{toolkit.name}'")
    body = ScheduleCreate(**values, toolkit_id=toolkit.id, enabled=False)
    row = create_schedule(session, project_id, body, scope, key=item.key)
    by_key[row.uuid] = row  # a later item of the file then updates this row
    by_index[(toolkit.id, row.index_name)] = row
    return 'created', row


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
        `schedule_id` and a `note` when the approval rule changed, a skipped or failed one the `reason`.
    """
    # late: the export loads without these modules
    from ..models.pd.schedule_yaml import ScheduleFileItem
    from .schedule_access import ScheduleScope
    from .schedule_settings import is_run_without_asking_allowed
    scope = ScheduleScope(user_id=admin_id, is_admin=True, permissions=frozenset())
    may_run_without_asking = is_run_without_asking_allowed(project_id)
    report = {'created': [], 'updated': [], 'skipped': [], 'failed': []}
    reviews = {}
    names = {raw['entity_name'] for raw in file.schedules
             if isinstance(raw.get('entity_name'), str) and raw.get('entity_type') != TOOLKIT_INDEX}
    toolkit_names = {raw['entity_name'] for raw in file.schedules
                     if isinstance(raw.get('entity_name'), str) and raw.get('entity_type') == TOOLKIT_INDEX}
    with db.get_session(project_id) as session:
        index = _entity_index(session, names)
        toolkits = _toolkit_index(session, toolkit_names)
        by_key, by_name, by_index = _schedule_index(
            session, file, {toolkit.id for found in toolkits.values() for toolkit in found})
        indexes = (by_key, by_name, set(), by_index)  # by uuid, by natural key, keys seen in the file, by index
        for raw in file.schedules:
            entry = {'name': raw.get('name')}
            try:
                item = ScheduleFileItem.model_validate(raw)
                if item.entity_type == TOOLKIT_INDEX:
                    values = item.model_dump(include={'name', 'entity_type', 'index_name', *INDEX_UPDATE_FIELDS})
                    group, row = _save_index(session, project_id, scope, indexes, item, values,
                                             _resolve_toolkit(toolkits, item))
                    note = None
                else:
                    ids = _resolve(index, item)
                    values = item.model_dump(exclude={'key', 'entity_name', 'version_name', 'index_name'})
                    if item.approval_rule == 'run_without_asking' and not may_run_without_asking:
                        values['approval_rule'] = 'pause_and_ask'
                        entry['note'] = 'Run without asking is not allowed in this project; saved as Pause and ask me'
                    group, row, note = _save(session, project_id, scope, indexes, item, values, ids, reviews)
                if note:
                    entry['note'] = note
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
