"""Bodies of a schedule (EL-7011): timing, create, update and details."""
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from ...utils.schedule_timing import compute_next_run, end_of_day, schedule_limits
from .index import _cron_gaps, _validate_cron_expression

CRON_TEXT = 'Cron with 5 parts: minute hour day month weekday, read in `timezone`'
TIMEZONE_TEXT = 'IANA time zone name, such as Europe/Warsaw'
EXPIRES_TEXT = 'Last day the schedule runs, in its time zone; null for no stop date'


class ScheduleTiming(BaseModel):
    """Cron, IANA time zone and optional stop date; field order is validation order."""
    cron: str = Field(description=CRON_TEXT)
    timezone: str = Field(description=TIMEZONE_TEXT)
    expires_at: date | None = Field(None, description=EXPIRES_TEXT)

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
NAME_TEXT = 'Schedule name'
DESCRIPTION_TEXT = 'Free text about the schedule'
VERSION_TEXT = 'Id of the entity version that runs'
RUN_MODE_TEXT = 'Pipeline input: `entry_point` runs without a message, `prompt` sends `prompt`'
PROMPT_TEXT = 'Message sent at each run; `{{ name }}` takes a variable value'
VARIABLES_TEXT = 'Values of the entity variables'
APPROVAL_TEXT = ('A human step `pause_and_ask` waits for the owner; `run_without_asking` '
                 'goes on, when the project allows it')


class ScheduleVariable(BaseModel):
    name: str = Field(min_length=1, description='Variable name of the agent prompt or the pipeline')
    value: str = Field(description='Value used for every run')


class ScheduleCreate(ScheduleTiming):
    """The run input pairing (R5.1, R5.4 to R5.7) is checked by the service, which needs entity_type."""
    model_config = ConfigDict(json_schema_extra={'examples': [{
        'name': 'Daily report', 'entity_type': 'agent', 'application_id': 12, 'version_id': 34,
        'prompt': 'Summarize the open tickets', 'cron': '0 9 * * 1-5', 'timezone': 'Europe/Warsaw',
    }]})

    name: str = Field(min_length=1, max_length=NAME_MAX, description=NAME_TEXT)
    description: str | None = Field(None, max_length=DESCRIPTION_MAX, description=DESCRIPTION_TEXT)
    entity_type: Literal['agent', 'pipeline'] = Field(description='Kind of the scheduled entity')
    application_id: int = Field(description='Id of the agent or pipeline')
    version_id: int = Field(description=VERSION_TEXT)
    run_mode: RunMode | None = Field(None, description=RUN_MODE_TEXT)
    prompt: str | None = Field(None, description=PROMPT_TEXT)
    variables: list[ScheduleVariable] = Field([], description=VARIABLES_TEXT)
    enabled: bool = Field(True, description='False saves the schedule paused')
    approval_rule: ApprovalRule = Field('pause_and_ask', description=APPROVAL_TEXT)


class ScheduleUpdate(BaseModel):
    """No owner or entity fields: the extra `created_by` and `principal` of a body are dropped (R2.2)."""
    model_config = ConfigDict(json_schema_extra={'examples': [{'enabled': False}, {'cron': '30 8 * * *'}]})

    name: str | None = Field(None, min_length=1, max_length=NAME_MAX, description=NAME_TEXT)
    description: str | None = Field(None, max_length=DESCRIPTION_MAX, description=DESCRIPTION_TEXT)
    version_id: int | None = Field(None, description=VERSION_TEXT)
    run_mode: RunMode | None = Field(None, description=RUN_MODE_TEXT)
    prompt: str | None = Field(None, description=PROMPT_TEXT)
    variables: list[ScheduleVariable] | None = Field(None, description=VARIABLES_TEXT)
    cron: str | None = Field(None, description=CRON_TEXT)
    timezone: str | None = Field(None, description=TIMEZONE_TEXT)
    expires_at: date | None = Field(None, description=EXPIRES_TEXT)
    enabled: bool | None = Field(None, description='False pauses the schedule, true resumes it')
    approval_rule: ApprovalRule | None = Field(None, description=APPROVAL_TEXT)

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
