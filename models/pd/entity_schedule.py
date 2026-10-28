"""Bodies of a schedule (EL-7011): timing, create, update and details."""
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ...utils.schedule_timing import compute_next_run, end_of_day, schedule_limits
from .index import _cron_gaps, _validate_cron_expression

CRON_TEXT = 'Cron with 5 parts: minute hour day month weekday, read in `timezone`'
TIMEZONE_TEXT = 'IANA time zone name, such as Europe/Warsaw'
EXPIRES_TEXT = 'Last day the schedule runs, in its time zone; null for no stop date'


class ScheduleTiming(BaseModel):
    """Cron, IANA time zone and optional stop date; field order is validation order."""
    cron: str = Field(description=CRON_TEXT, examples=['0 9 * * 1-5'])
    timezone: str = Field(description=TIMEZONE_TEXT, examples=['Europe/Warsaw'])
    expires_at: date | None = Field(None, description=EXPIRES_TEXT, examples=['2026-12-31'])

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
DeliveryMode = Literal['run_history', 'conversation']
DbId = Annotated[int, Field(ge=1, le=2**31 - 1)]  # the database integer range
ApprovalRule = Literal['pause_and_ask', 'run_without_asking']
NAME_TEXT = 'Schedule name'
DESCRIPTION_TEXT = 'Free text about the schedule'
VERSION_TEXT = 'Id of the entity version that runs'
RUN_MODE_TEXT = 'Pipeline input: `entry_point` runs without a message, `prompt` sends `prompt`'
PROMPT_TEXT = 'Message sent at each run; `{{ name }}` takes a variable value'
VARIABLES_TEXT = 'Values of the entity variables'
APPROVAL_TEXT = ('A human step `pause_and_ask` waits for the owner; `run_without_asking` '
                 'goes on, when the project allows it')
NAME_EXAMPLE = 'Daily report'
DESCRIPTION_EXAMPLE = 'Weekday summary of the open tickets'
PROMPT_EXAMPLE = 'Summarize the open tickets'
VARIABLES_EXAMPLE = [{'name': 'ticket_limit', 'value': '20'}]


RISK_ACK_TEXT = ('The acknowledgement of the sensitive tools: send it again after a 409 '
                 '`risk_confirmation_required` or `risk_changed`, with the `fingerprint` of that answer')


class RiskAck(BaseModel):
    """What the person confirmed in the dialog "Review risks before scheduling" (owner CR 2026-10-21)."""
    fingerprint: str = Field(min_length=1, description='The `fingerprint` of the check or of the 409 answer',
                             examples=['3f1c9a0e5b7d4c2a8e6f0b1d3c5a7e9f2b4d6f8a0c2e4a6b8d0f2a4c6e8b0d2f'])
    confirmed_name: str = Field('', description='The schedule name the person typed; must equal the schedule '
                                                'name when the rule is `run_without_asking`',
                                examples=['Daily report'])


# The scheduled entity and version, the same in the risk check and the create body
EntityType = Annotated[Literal['agent', 'pipeline'], Field(description='Kind of the scheduled entity', examples=['agent'])]
ApplicationId = Annotated[int, Field(description='Id of the agent or pipeline', examples=[12])]
VersionId = Annotated[int, Field(description=VERSION_TEXT, examples=[34])]


class ScheduleRiskCheck(BaseModel):
    """The body of the dry-run risk check: the version that would run."""
    model_config = ConfigDict(json_schema_extra={'examples': [
        {'entity_type': 'agent', 'application_id': 12, 'version_id': 34}]})

    entity_type: EntityType
    application_id: ApplicationId
    version_id: VersionId
    schedule_id: int | None = Field(
        default=None, description='Id of the schedule an edit checks: the scan runs as its owner and the answer '
        'adds `confirmed`', examples=[7])


class ScheduleDelivery(BaseModel):
    """Where a run posts its answer: the run history, or one chat the person is in."""
    mode: DeliveryMode = Field('run_history', description='`run_history` keeps the answer in the run log, '
                               '`conversation` also posts it into the chat', examples=['conversation'])
    conversation_uuid: str | None = Field(None, max_length=36, description='Chat that receives the answer; '
                                          'required for `conversation`', examples=['3f1c9a0e-5b7d-4c2a-8e6f-0b1d3c5a7e9f'])
    participant_id: DbId | None = Field(None, description='Participant of that chat that posts; '
                                        'required for `conversation`', examples=[5])
    include_history: int = Field(0, ge=0, le=20, description='How many recent chat messages each run sends '
                                 'to the model; 0 sends none', examples=[5])

    @model_validator(mode='after')
    def check_targets(self) -> 'ScheduleDelivery':
        if self.mode == 'run_history':
            self.conversation_uuid = self.participant_id = None
        elif not self.conversation_uuid or self.participant_id is None:
            raise ValueError('Mode conversation needs conversation_uuid and participant_id')
        return self


DELIVERY_TEXT = 'Where each run posts its answer; on an edit, absent keeps the stored delivery'


class ScheduleVariable(BaseModel):
    name: str = Field(min_length=1, description='Variable name of the agent prompt or the pipeline',
                      examples=['ticket_limit'])
    value: str = Field(description='Value used for every run', examples=['20'])


class ScheduleCreate(ScheduleTiming):
    """The run input pairing (R5.1, R5.4 to R5.7) is checked by the service, which needs entity_type."""
    model_config = ConfigDict(json_schema_extra={'examples': [{
        'name': 'Daily report', 'entity_type': 'agent', 'application_id': 12, 'version_id': 34,
        'prompt': 'Summarize the open tickets', 'cron': '0 9 * * 1-5', 'timezone': 'Europe/Warsaw',
    }]})

    name: str = Field(min_length=1, max_length=NAME_MAX, description=NAME_TEXT, examples=[NAME_EXAMPLE])
    description: str | None = Field(None, max_length=DESCRIPTION_MAX, description=DESCRIPTION_TEXT,
                                    examples=[DESCRIPTION_EXAMPLE])
    entity_type: EntityType
    application_id: ApplicationId
    version_id: VersionId
    run_mode: RunMode | None = Field(None, description=RUN_MODE_TEXT, examples=['prompt'])
    prompt: str | None = Field(None, description=PROMPT_TEXT, examples=[PROMPT_EXAMPLE])
    variables: list[ScheduleVariable] = Field([], description=VARIABLES_TEXT, examples=[VARIABLES_EXAMPLE])
    enabled: bool = Field(True, description='False saves the schedule paused', examples=[True])
    approval_rule: ApprovalRule = Field('pause_and_ask', description=APPROVAL_TEXT, examples=['pause_and_ask'])
    delivery: ScheduleDelivery | None = Field(None, description=DELIVERY_TEXT)
    risk_ack: RiskAck | None = Field(None, description=RISK_ACK_TEXT,
                                     examples=[{'fingerprint': '3f1c9a0e', 'confirmed_name': NAME_EXAMPLE}])


class ScheduleEdit(BaseModel):
    """The PUT body: no owner, entity or `enabled` field; extra `created_by` and `principal` are dropped (R2.2)."""
    model_config = ConfigDict(json_schema_extra={'examples': [{'cron': '30 8 * * *'}, {'name': NAME_EXAMPLE}]})

    name: str | None = Field(None, min_length=1, max_length=NAME_MAX, description=NAME_TEXT, examples=[NAME_EXAMPLE])
    description: str | None = Field(None, max_length=DESCRIPTION_MAX, description=DESCRIPTION_TEXT,
                                    examples=[DESCRIPTION_EXAMPLE])
    version_id: int | None = Field(None, description=VERSION_TEXT, examples=[35])
    run_mode: RunMode | None = Field(None, description=RUN_MODE_TEXT, examples=['prompt'])
    prompt: str | None = Field(None, description=PROMPT_TEXT, examples=[PROMPT_EXAMPLE])
    variables: list[ScheduleVariable] | None = Field(None, description=VARIABLES_TEXT, examples=[VARIABLES_EXAMPLE])
    cron: str | None = Field(None, description=CRON_TEXT, examples=['30 8 * * *'])
    timezone: str | None = Field(None, description=TIMEZONE_TEXT, examples=['Europe/Warsaw'])
    expires_at: date | None = Field(None, description=EXPIRES_TEXT, examples=['2026-12-31'])
    approval_rule: ApprovalRule | None = Field(None, description=APPROVAL_TEXT, examples=['run_without_asking'])
    delivery: ScheduleDelivery | None = Field(None, description=DELIVERY_TEXT)
    risk_ack: RiskAck | None = Field(None, description=RISK_ACK_TEXT,
                                     examples=[{'fingerprint': '3f1c9a0e', 'confirmed_name': NAME_EXAMPLE}])

    @field_validator('cron')
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        return v if v is None else ScheduleTiming.validate_cron(v)

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: str | None) -> str | None:
        return v if v is None else ScheduleTiming.validate_timezone(v)


class ScheduleUpdate(ScheduleEdit):
    """What the service applies: an edit, or `enabled` from the pause and resume actions and the file import."""
    model_config = ConfigDict(json_schema_extra={'examples': [{'enabled': False}, {'cron': '30 8 * * *'}]})

    enabled: bool | None = Field(None, description='False pauses the schedule, true resumes it', examples=[False])


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
    delivery: ScheduleDelivery = Field(default_factory=ScheduleDelivery)
    approval_confirmed_by: str | None = None  # the name of who confirmed "Run without asking"
    approval_confirmed_at: datetime | None = None
    allowed_actions: list[str]
