"""Bodies of a schedule (EL-7011): timing, create, update and details."""
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

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


NAME_MAX = 256
DESCRIPTION_MAX = 2304  # the entity_schedules.description column width
RunMode = Literal['entry_point', 'prompt']
ApprovalRule = Literal['pause_and_ask', 'run_without_asking']


class ScheduleVariable(BaseModel):
    name: str = Field(min_length=1)
    value: str


class ScheduleCreate(ScheduleTiming):
    """The run input pairing (R5.1, R5.4 to R5.7) is checked by the service, which needs entity_type."""
    name: str = Field(min_length=1, max_length=NAME_MAX)
    description: str | None = Field(None, max_length=DESCRIPTION_MAX)
    entity_type: Literal['agent', 'pipeline']
    application_id: int
    version_id: int
    run_mode: RunMode | None = None
    prompt: str | None = None
    variables: list[ScheduleVariable] = []
    enabled: bool = True
    approval_rule: ApprovalRule = 'pause_and_ask'


class ScheduleUpdate(BaseModel):
    """No owner or entity fields: the extra `created_by` and `principal` of a body are dropped (R2.2)."""
    name: str | None = Field(None, min_length=1, max_length=NAME_MAX)
    description: str | None = Field(None, max_length=DESCRIPTION_MAX)
    version_id: int | None = None
    run_mode: RunMode | None = None
    prompt: str | None = None
    variables: list[ScheduleVariable] | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: date | None = None
    enabled: bool | None = None
    approval_rule: ApprovalRule | None = None

    @field_validator('cron')
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        return v if v is None else ScheduleTiming.validate_cron(v)

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: str | None) -> str | None:
        return v if v is None else ScheduleTiming.validate_timezone(v)


class ScheduleOwner(BaseModel):
    id: int
    name: str | None = None


class ScheduleDetails(BaseModel):
    """A stored row as the API answers it; no timing rules, so an old stop date still reads."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    entity_type: str
    application_id: int
    version_id: int | None
    run_mode: str | None
    prompt: str | None
    variables: list[ScheduleVariable]
    cron: str
    timezone: str
    expires_at: datetime | None
    enabled: bool
    created_by: int
    owner: ScheduleOwner
    updated_by: int | None
    next_run: datetime | None
    last_run: datetime | None
    last_run_status: str | None
    disabled_reason: str | None
    approval_rule: str
    allowed_actions: list[str]
