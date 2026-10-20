"""Schedules as a YAML file (EL-7011): the export writes a fixed allow-list of fields, never the stored row.

The import creates every item disabled, through the create path of the schedule service."""
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import select
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.entity_schedule import EntitySchedule
from .pipeline_utils import from_str_to_yaml, validate_yaml_from_str
from .schedule_service import ScheduleError

FILE_KIND = 'elitea_schedules'
SCHEMA_VERSION = 1
MAX_FILE_BYTES = 1024 * 1024
NOT_YAML = 'The schedule file is not valid YAML'
TOO_LARGE = 'The schedule file is larger than 1 MB'
WRONG_HEADER = 'This is not a schedule file of schema version 1'


def _item(row: EntitySchedule, entity_name: str, version_name: str | None) -> dict:
    """One allow-listed file item; the keys are the file format, in file order."""
    is_entry_point = row.run_mode == 'entry_point'
    return {
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
        found = session.execute(query).all()
    unknown = sorted(set(ids or []) - {row.id for row, *_ in found})
    if unknown:
        raise ScheduleError({'error': f'Unknown schedule ids: {", ".join(map(str, unknown))}'})
    return from_str_to_yaml({
        'kind': FILE_KIND,
        'schema_version': SCHEMA_VERSION,
        'schedules': [_item(*columns) for columns in found],
    })


def parse_schedules_file(text: str):
    """Return the checked file; the items stay raw dicts, so one bad item fails alone.

    Raises:
        ValueError: With a schedule message when the text is too large, not a YAML mapping, or has a wrong header.
    """
    # late: the export loads without the import-only modules
    from ..models.pd.schedule_yaml import ScheduleFile
    if len(text.encode()) > MAX_FILE_BYTES:
        raise ValueError(TOO_LARGE)
    try:
        data = validate_yaml_from_str(text)
    except ValueError:  # the helper words its errors for pipelines
        raise ValueError(NOT_YAML) from None
    try:
        return ScheduleFile.model_validate(data)
    except ValidationError as e:
        first = e.errors()[0]
        if first['loc'] in (('kind',), ('schema_version',)):  # a missing key reads 'Field required'
            raise ValueError(WRONG_HEADER) from None
        raise ValueError(_reason(e)) from None


def _reason(error) -> str:
    """The first broken field of a pydantic error and its plain message."""
    first = error.errors()[0]
    message = first['msg'].removeprefix('Value error, ')
    return f'{first["loc"][0]}: {message}' if first['loc'] else message


class _Skipped(Exception):
    """The item matches no single entity version of the project."""


def _resolve(session, item) -> tuple[int, int]:
    """Return (application_id, version_id) of the one entity and version the item names, or raise _Skipped."""
    is_pipeline = item.entity_type == 'pipeline'
    kind = item.entity_type.capitalize()
    versions = session.execute(
        select(Application.id, ApplicationVersion.id, ApplicationVersion.name)
        .join(ApplicationVersion, ApplicationVersion.application_id == Application.id)
        .where(Application.name == item.entity_name)
        .where((ApplicationVersion.agent_type == 'pipeline') == is_pipeline)
    ).all()
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


def import_schedules(project_id: int, admin_id: int, file) -> dict:
    """Create one disabled schedule per matched item, owned by the admin; return the report.

    Args:
        project_id: The target project.
        admin_id: The importing admin; owner and principal of every row.
        file: The `ScheduleFile` from `parse_schedules_file`.

    Returns:
        `{created, skipped, failed}`; every entry has the item name, a created one the `schedule_id`
        and a `note` when the approval rule changed, a skipped or failed one the `reason`.
    """
    # late: the export loads without these modules
    from ..models.pd.entity_schedule import ScheduleCreate
    from ..models.pd.schedule_yaml import ScheduleFileItem
    from .schedule_access import ScheduleScope
    from .schedule_risk import is_run_without_asking_allowed
    from .schedule_service import create_schedule
    scope = ScheduleScope(user_id=admin_id, is_admin=True, permissions=frozenset())
    may_run_without_asking = is_run_without_asking_allowed(project_id)
    report = {'created': [], 'skipped': [], 'failed': []}
    with db.get_session(project_id) as session:
        for raw in file.schedules:
            entry = {'name': raw.get('name')}
            try:
                item = ScheduleFileItem.model_validate(raw)
                application_id, version_id = _resolve(session, item)
                values = item.model_dump(exclude={'entity_name', 'version_name'})
                if item.approval_rule == 'run_without_asking' and not may_run_without_asking:
                    values['approval_rule'] = 'pause_and_ask'
                    entry['note'] = 'Run without asking is not allowed in this project; saved as Pause and ask me'
                body = ScheduleCreate(**values, application_id=application_id, version_id=version_id, enabled=False)
                entry['schedule_id'] = create_schedule(session, project_id, body, scope).id
                report['created'].append(entry)
            except _Skipped as e:
                report['skipped'].append({**entry, 'reason': str(e)})
            except ValidationError as e:
                report['failed'].append({**entry, 'reason': _reason(e)})
            except ScheduleError as e:
                session.rollback()
                report['failed'].append({**entry, 'reason': e.response[0]['error']})
    return report
