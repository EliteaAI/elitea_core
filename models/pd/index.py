from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, root_validator, validator, ValidationError
from datetime import datetime, timedelta, timezone

from croniter import croniter


_DAILY_FLOOR = timedelta(hours=24)
# Number of consecutive firings to inspect when verifying the minimum gap.
# 32 covers monthly patterns (28-31 day gaps) and weekly multi-day patterns
# without making validation expensive.
_GAP_PROBE_FIRINGS = 32

_DEFAULT_TIMEZONE = 'UTC'
_EPOCH_ISO = datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()


def _validate_cron_expression(v: str) -> str:
    # basic type and emptiness check for clearer errors than croniter alone
    if not isinstance(v, str) or not v.strip():
        raise ValueError('cron must be a non-empty string')
    v = v.strip()

    # strict validation via croniter
    try:
        # constructing croniter is enough to validate the expression
        croniter(v)
    except Exception as e:
        raise ValueError(f'invalid cron expression: {e}')

    return v


def _validate_daily_floor(cron_expr: str) -> str:
    """Reject cron expressions that fire more than once per 24 hours.

    Mirrors the daily-frequency floor enforced by the index scheduling UI
    (validateMinimumDailyFrequency in indexSchedule.helpers.js). Direct API
    callers bypass the UI gate, so the same constraint is enforced here.

    Probes the next several firings and asserts every consecutive gap is
    >= 24h. This catches all sub-daily patterns (every-N-minutes, multiple
    hours per day, hour ranges) without re-implementing cron field parsing.
    """
    base = datetime(2000, 1, 1, tzinfo=timezone.utc)
    itr = croniter(cron_expr, base)
    prev = itr.get_next(datetime)
    for _ in range(_GAP_PROBE_FIRINGS):
        nxt = itr.get_next(datetime)
        if nxt - prev < _DAILY_FLOOR:
            raise ValueError('Frequency cannot be more than once per day')
        prev = nxt
    return cron_expr


class Credentials(BaseModel):
    private: Optional[bool] = False
    elitea_title: str

    @root_validator(pre=True)
    def accept_legacy_title(cls, values):
        # Schedules stored before the alita->elitea rename still carry `alita_title`.
        # configurations.expand_configuration reads either key, so rejecting them here
        # would strand rows that the rest of the pipeline can still resolve.
        if isinstance(values, dict) and not values.get('elitea_title') and values.get('alita_title'):
            values = {**values, 'elitea_title': values['alita_title']}
        return values


class UpdateIndexingSchedule(BaseModel):
    cron: str
    enabled: bool = False
    # -1 indicates no user and
    # scheduling is in project configurations
    user_id: Optional[int] = -1
    credentials: Optional[Credentials] = None
    timezone: str

    @validator('timezone')
    def validate_timezone(cls, v):
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        v = _validate_cron_expression(v)
        return _validate_daily_floor(v)


class ToolkitIndexingSchedule(BaseModel):
    cron: str
    enabled: bool
    credentials: Optional[Credentials] = None
    # Schedules written before these fields existed have no author, timezone or last_run.
    # They stay parseable so the scheduler can still run them; a missing author only
    # blocks resolving a *private* credential, which resolve_credentials rejects on its own.
    created_by: Optional[int] = Field(default=None, gt=0)
    timezone: str = _DEFAULT_TIMEZONE
    # store last_run as ISO 8601 string (always UTC)
    last_run: str = _EPOCH_ISO

    @validator('timezone', pre=True)
    def validate_timezone(cls, v):
        if v is None:
            return _DEFAULT_TIMEZONE
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('last_run', pre=True)
    def normalize_last_run(cls, v):
        """Accept datetime or string, ensure tz is present, normalize to UTC, and store as ISO string."""
        if v is None:
            # An absent last_run makes the schedule immediately due, which is the
            # intended recovery for a legacy row that has never run.
            return _EPOCH_ISO
        # Convert input to datetime first
        if isinstance(v, datetime):
            dt = v
        else:
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                raise ValueError('last_run must be a valid ISO 8601 datetime string')

        # If no timezone or naive, assume UTC
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Normalize to UTC if not already
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        return _validate_cron_expression(v)


class SaveIndexConfiguration(BaseModel):
    # The tool schema is owned by the SDK, so the payload is only shape-checked here.
    configuration: dict


class IndexDataRemovedEvent(BaseModel):
    index_name: str
    toolkit_id: int = Field(gt=0)
    project_id: int = Field(gt=0)

    class Config:
        extra = 'allow'
