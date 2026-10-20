"""Timing of a schedule body (EL-7011): cron, time zone and stop date."""
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ValidationInfo, field_validator

from ...utils.schedule_timing import compute_next_run, end_of_day, schedule_limits
from .index import _cron_gaps, _validate_cron_expression


class ScheduleTiming(BaseModel):
    """Cron, IANA time zone and optional stop date; field order is validation order."""
    cron: str
    timezone: str
    expires_at: date | None = None

    @field_validator('cron')
    @classmethod
    def validate_cron(cls, v: str) -> str:
        v = v.strip()
        if len(v.split()) != 5:
            raise ValueError('Enter a cron with 5 parts: minute hour day month weekday')
        v = _validate_cron_expression(v)
        floor = schedule_limits().min_interval_minutes
        if min(_cron_gaps(v)) < timedelta(minutes=floor):
            raise ValueError(f'Runs must be at least {floor} minutes apart')
        return v

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (KeyError, ValueError):
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @field_validator('expires_at')
    @classmethod
    def validate_expires_at(cls, v: date | None, info: ValidationInfo) -> date | None:
        # A bad cron or time zone already failed its own field; nothing to compare against
        if v is None or 'cron' not in info.data or 'timezone' not in info.data:
            return v
        cron, zone = info.data['cron'], info.data['timezone']
        now = datetime.now(UTC)
        if v < now.astimezone(ZoneInfo(zone)).date():
            raise ValueError('The stop date is in the past')
        if compute_next_run(cron, zone, now) > end_of_day(v, zone):
            raise ValueError('The schedule never runs before its stop date')
        return v
