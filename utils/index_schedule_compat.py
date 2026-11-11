"""The old PATCH of an index schedule, kept for one release and written as a Toolkit Index schedule row.

The old route sets one schedule per user key in the toolkit settings. The row of the Schedules module holds one
schedule per index, run by the caller, so the old `user_id` and `credentials` fields are read and not stored.
"""
from pydantic import ValidationError
from sqlalchemy import select
from tools import db

from ..models.all import EliteATool
from ..models.entity_schedule import EntitySchedule
from ..models.pd.entity_schedule import NAME_MAX, ScheduleCreate, ScheduleUpdate
from ..models.pd.index import UpdateIndexingSchedule
from .schedule_access import schedule_scope
from .schedule_service import (
    TOOLKIT_INDEX,
    ScheduleError,
    create_schedule,
    load_schedule_in_scope,
    refusal,
    update_schedule,
)
from .schedule_timing import ScheduleLimitError

LIMIT_REACHED = 'schedule_limit_reached'  # the code of the Schedules routes


def _invalid(error: ValidationError) -> tuple[dict, int]:
    first = error.errors()[0]
    return refusal('invalid_input', first['msg'].removeprefix('Value error, '),
                   field=first['loc'][0] if first['loc'] else 'body')


def _saved_body(row) -> dict:
    return {
        'ok': True, 'schedule_id': row.id, 'cron': row.cron, 'timezone': row.timezone, 'enabled': row.enabled,
        'next_run': row.next_run.isoformat() if row.next_run else None,
    }


def save_index_schedule(project_id: int, toolkit_id: int, index_name: str, payload: dict) -> tuple[dict, int]:
    """Create or change the schedule row of the index from an old PATCH body; answers `(body, status)`.

    An index that the toolkit settings still schedule answers 409: delete that entry first, or let the
    migration move it.
    """
    try:
        update = UpdateIndexingSchedule.model_validate(payload)
    except ValidationError as error:
        return _invalid(error)
    scope = schedule_scope(project_id)
    with db.get_session(project_id) as session:
        toolkit = session.get(EliteATool, toolkit_id)
        if toolkit is None:
            return refusal('not_found', 'Toolkit not found', 404)
        row_id = session.scalar(select(EntitySchedule.id).where(
            EntitySchedule.toolkit_id == toolkit_id, EntitySchedule.index_name == index_name).limit(1))
        try:
            if row_id is None:
                row = create_schedule(session, project_id, ScheduleCreate(
                    entity_type=TOOLKIT_INDEX, toolkit_id=toolkit_id, index_name=index_name,
                    name=f'{toolkit.name} / {index_name}'[:NAME_MAX], cron=update.cron, timezone=update.timezone,
                    enabled=update.enabled), scope)
            else:
                row = update_schedule(
                    session, project_id, load_schedule_in_scope(session, row_id, scope),
                    ScheduleUpdate(cron=update.cron, timezone=update.timezone, enabled=update.enabled), scope)
        except ScheduleError as error:
            return error.response
        except ScheduleLimitError as error:
            return refusal(LIMIT_REACHED, str(error), 409)
        except ValidationError as error:
            return _invalid(error)
        return _saved_body(row), 200
