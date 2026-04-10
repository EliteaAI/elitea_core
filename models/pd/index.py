from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, validator, ValidationError
from datetime import datetime, timezone

from croniter import croniter


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


class Credentials(BaseModel):
    private: Optional[bool] = False
    elitea_title: str


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
        return _validate_cron_expression(v)


class ToolkitIndexingSchedule(BaseModel):
    cron: str
    enabled: bool
    credentials: Optional[Credentials] = None
    created_by: int = Field(gt=0)
    timezone: str
    # store last_run as ISO 8601 string (always UTC)
    last_run: str

    @validator('timezone')
    def validate_timezone(cls, v):
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('last_run', pre=True)
    def normalize_last_run(cls, v):
        """Accept datetime or string, ensure tz is present, normalize to UTC, and store as ISO string."""
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


class IndexDataRemovedEvent(BaseModel):
    index_name: str
    toolkit_id: int = Field(gt=0)
    project_id: int = Field(gt=0)

    class Config:
        extra = 'allow'
