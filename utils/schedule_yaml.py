"""Schedules as a YAML file (EL-7011): the export writes a fixed allow-list of fields, never the stored row."""
from zoneinfo import ZoneInfo

from sqlalchemy import select
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.entity_schedule import EntitySchedule
from .pipeline_utils import from_str_to_yaml
from .schedule_service import ScheduleError

FILE_KIND = 'elitea_schedules'
SCHEMA_VERSION = 1


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
