"""Next run, stop date and limits of an entity schedule (EL-7011)."""
from datetime import UTC, date, datetime, time
from typing import NamedTuple
from zoneinfo import ZoneInfo

from croniter import croniter
from sqlalchemy import func, select
from tools import this

DEFAULT_MIN_INTERVAL_MINUTES = 5
DEFAULT_MAX_ENABLED_PER_PROJECT = 50


class ScheduleLimits(NamedTuple):
    min_interval_minutes: int
    max_enabled_per_project: int


class ScheduleLimitError(Exception):
    def __init__(self, limit: int):
        super().__init__(f'A project can have at most {limit} enabled schedules')
        self.limit = limit


def schedule_limits() -> ScheduleLimits:
    """Read at each call, so an admin change applies to the next check."""
    section = this.module.descriptor.config.get('scheduler', {}).get('schedules', {})
    return ScheduleLimits(
        section.get('min_interval_minutes', DEFAULT_MIN_INTERVAL_MINUTES),
        section.get('max_enabled_per_project', DEFAULT_MAX_ENABLED_PER_PROJECT),
    )


def compute_next_run(cron: str, timezone_name: str, after: datetime) -> datetime:
    """The next firing after `after` in the schedule timezone, as UTC."""
    # lean: trusts an aware `after` (naive reads as server time); check it when a caller may pass naive
    zone = ZoneInfo(timezone_name)
    # Step in local wall time: croniter 1.4.1 (the runtime pin) keeps the start's UTC offset
    # across a clock change, so an aware start gives a firing one hour off on that day
    firings = croniter(cron, after.astimezone(zone).replace(tzinfo=None))
    while True:
        firing = firings.get_next(datetime).replace(tzinfo=zone).astimezone(UTC)
        if firing > after:  # a repeated hour can map a wall time to an instant not after `after`
            return firing


def end_of_day(day: date, timezone_name: str) -> datetime:
    """23:59:59 of `day` in the schedule timezone, as UTC."""
    local = datetime.combine(day, time(23, 59, 59), tzinfo=ZoneInfo(timezone_name))
    return local.astimezone(UTC)


def check_enabled_cap(session, adding: int = 1) -> None:
    """Raise when `adding` more enabled schedules would pass the project cap.

    Callers pass adding=0 for a row that is already enabled.
    """
    # Imported here: the pydantic model imports this module and must not load the ORM tables.
    from ..models.entity_schedule import EntitySchedule

    cap = schedule_limits().max_enabled_per_project
    # lean: soft cap, no lock (US-07 checks and writes in one transaction); lock when parallel enables pass it
    enabled = session.execute(
        select(func.count()).select_from(EntitySchedule).where(EntitySchedule.enabled.is_(True))
    ).scalar()
    if enabled + adding > cap:
        raise ScheduleLimitError(cap)
