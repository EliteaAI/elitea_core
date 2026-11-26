"""Pydantic models for the schedule list query arguments and rows."""
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

MAX_DB_ID = 2**31 - 1  # the database integer range
INDEX_NAME_MAX = 256  # the entity_schedules.index_name column width
DbId = Annotated[int, Field(ge=1, le=MAX_DB_ID)]
TOOL_NAME_MAX = 256  # the longest tool name the filter accepts; a longer one is a 400
ToolName = Annotated[str, Field(min_length=1, max_length=TOOL_NAME_MAX)]
# The values the list and runs options accept; Swagger reads them with typing.get_args (US-55 D3)
EntityType = Literal['agent', 'pipeline', 'toolkit_index']
RunStatus = Literal['running', 'success', 'failed', 'skipped', 'blocked', 'waiting_for_approval']
RunTrigger = Literal['cron', 'manual', 'test', 'webhook']
# The run kinds that have no run record: the conversation's own execution type tells them apart
ExecutionType = Literal['chat', 'api', 'mcp']
# The status shown on a run row (AC-24): a schedule run's status, or for a plain chat one derived from stored data.
# Stopped and Partial are index-run states only; they never reach this list.
RunOutcome = Literal['running', 'waiting_for_approval', 'success', 'failed', 'skipped', 'blocked']
SortBy = Literal['name', 'entity_name', 'entity_type', 'next_run', 'last_run', 'enabled', 'last_delivery_at']
SortOrder = Literal['asc', 'desc']


class RunFilters(BaseModel):
    """The filters of the run lists: the saved runs of a schedule and the Test Runs of an agent or pipeline."""

    status: RunStatus | None = None
    trigger: RunTrigger | None = None
    version_id: DbId | None = None


MAX_FILTER_VALUES = 20  # a comma list of one filter: more than this is a 400 (AC-48)


def _split_comma_list(value):
    """A comma separated query value as a list of its non-empty elements (AC-48: ``a,,b`` reads as a, b).

    A value with no element at all (``''``, ``','``) stays as it is, so it fails the option check: a 400, as before.
    """
    if isinstance(value, str):
        items = [item.strip() for item in value.split(',') if item.strip()]
        return items or value
    return value


class ConversationListFilters(BaseModel):
    """The conversation list filters. Each takes a comma list (OR within one filter; filters combine with AND).

    A value outside the allowed set is a 400, and so is a list longer than MAX_FILTER_VALUES. An empty list means
    the filter is not applied.
    """

    status: RunStatus | list[RunStatus] | None = None
    trigger: RunTrigger | list[RunTrigger] | None = None
    version_id: DbId | list[DbId] | None = None
    execution_type: ExecutionType | list[ExecutionType] | None = None
    run_outcome: RunOutcome | list[RunOutcome] | None = None  # the derived run_status of a row (AC-24, AC-26)
    tool_name: ToolName | list[ToolName] | None = None  # MCP runs: a tool called in the run (AC-45)

    @field_validator('status', 'trigger', 'version_id', 'execution_type', 'run_outcome', 'tool_name', mode='before')
    @classmethod
    def _comma_list(cls, value):
        values = _split_comma_list(value)
        if isinstance(values, list):
            if len(values) > MAX_FILTER_VALUES:
                raise ValueError(f'at most {MAX_FILTER_VALUES} values')
            if not values:
                return None
            # One value stays a scalar, so the single-value contract and its results do not change (AC-48)
            return values[0] if len(values) == 1 else values
        return values


class ScheduleListFilters(BaseModel):
    """Query arguments of the schedule list; a bad value names its field."""

    entity_type: str | None = Field(None, description='Only schedules of this entity kind', examples=['agent'])
    status: str | None = Field(None, description='Only schedules whose last run has this status',
                               examples=['failed'])
    enabled: bool | None = Field(None, description='True: active, false: paused', examples=[True])
    owner_ids: list[DbId] | None = Field(None, description='Only schedules of these owners; schedules admin only',
                                         examples=[[3, 7]])
    entity_id: DbId | None = Field(None, description='Only schedules of this agent or pipeline; with entity_type '
                                   'toolkit_index, of this toolkit', examples=[12])
    index_name: str | None = Field(None, max_length=INDEX_NAME_MAX, description='Only schedules of this index name',
                                   examples=['docs'])
    version_id: DbId | None = Field(None, description='Only schedules that run this version', examples=[31])
    search: str | None = Field(None, description='Match on schedule name, entity name or index name', examples=['daily report'])
    limit: int = Field(default=20, ge=1, le=100, description='Rows per page', examples=[20])
    offset: int = Field(default=0, ge=0, le=MAX_DB_ID, description='Rows to skip', examples=[0])
    trigger_kind: Literal['cron', 'webhook'] = Field(
        'cron', description='Row kind; the default keeps webhooks out of schedule lists', examples=['webhook'])
    webhook_type: Literal['github', 'gitlab', 'custom'] | None = Field(
        None, description='Only webhooks of this sender', examples=['github'])
    sort_by: SortBy = Field('next_run', description='Sort column', examples=['name'])
    sort_order: SortOrder = Field('asc', description='Sort direction', examples=['desc'])

    @field_validator('owner_ids', mode='before')
    @classmethod
    def _split_owner_ids(cls, value):
        """Accept a comma separated query string as a list of ids."""
        if isinstance(value, str):
            return value.split(',')
        return value


class ScheduleOwner(BaseModel):
    """Owner of a schedule; the name is null when the lookup fails."""

    id: int = Field(description='User id of the owner')
    name: str | None = Field(None, description='Name, or email when the user has no name; null when the lookup fails')


class ScheduleListItem(BaseModel):
    """One row of the schedule list; it has no tags field."""

    id: int = Field(description='Schedule id')
    name: str = Field(description='Schedule name')
    description: str | None = Field(None, description='Schedule description')
    entity_type: str = Field(description='Kind of the scheduled entity: agent, pipeline or toolkit_index')
    run_mode: str | None = Field(None, description='How a run starts: entry_point or prompt')
    application_id: DbId | None = Field(None, description='Id of the agent or pipeline; null on a Toolkit Index')
    toolkit_id: DbId | None = Field(None, description='Toolkit of a Toolkit Index schedule; null on other kinds')
    index_name: str | None = Field(None, description='Index of a Toolkit Index schedule; null on other kinds')
    entity_name: str | None = Field(None, description='Name of the agent, pipeline or toolkit')
    version_id: int | None = Field(None, description='Version that runs; null when the version is deleted')  # US-01
    version_name: str | None = Field(None, description='Name of the version that runs')
    cron: str | None = Field(None, description='Cron with 5 parts, read in `timezone`; null on webhooks')
    timezone: str | None = Field(None, description='IANA time zone name; null on webhooks')
    trigger_kind: str = Field('cron', description='cron or webhook')
    webhook_type: str | None = Field(None, description='Webhook sender: github, gitlab or custom')
    gitlab_auth_method: str | None = Field(
        None, description='GitLab webhooks: secret_token or signing_token; a rotate of a signing row takes a new token')
    webhook_url: str | None = Field(None, description='Delivery URL of a webhook; never carries the secret')
    previous_secret_until: datetime | None = Field(
        None, description='Webhooks: the previous secret is accepted until this time')
    last_delivery_at: datetime | None = Field(None, description='Webhooks: time of the last authentic delivery')
    last_delivery_outcome: str | None = Field(
        None, description='Webhooks: accepted, duplicate, ping, disabled, refused, capped or start_failed')
    running_runs: int | None = Field(None, description='Webhooks: runs running now (deliveries and samples)')
    run_cap: int | None = Field(None, description='Webhooks: the running-run cap of this webhook')
    enabled: bool = Field(description='True: active, false: paused')
    disabled_reason: str | None = Field(None, description='Why the system paused the schedule')
    next_run: datetime | None = Field(None, description='Time of the next run; null when paused or expired')
    last_run: datetime | None = Field(None, description='Start time of the last run')
    last_run_status: str | None = Field(None, description='Status of the last run')
    last_run_reason: str | None = Field(None, description='Why the last run failed, was skipped or blocked')
    last_conversation_id: int | None = Field(None, description='Conversation of the last run')
    expires_at: datetime | None = Field(None, description='Last day the schedule runs; null for no stop date')
    owner: ScheduleOwner = Field(description='Owner of the schedule')
    allowed_actions: list[str] = Field(description='Actions the caller may take on this row')
