"""Pydantic models for the schedule list query arguments and rows."""
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

DbId = Annotated[int, Field(ge=1, le=2**31 - 1)]  # the database integer range
# The values the list and runs options accept; Swagger reads them with typing.get_args (US-55 D3)
EntityType = Literal['agent', 'pipeline']
RunStatus = Literal['running', 'success', 'failed', 'skipped', 'blocked', 'waiting_for_approval']
SortBy = Literal['name', 'entity_name', 'entity_type', 'next_run', 'last_run', 'enabled']
SortOrder = Literal['asc', 'desc']


class ScheduleListFilters(BaseModel):
    """Query arguments of the schedule list; a bad value names its field."""

    entity_type: str | None = Field(None, description='Only schedules of this entity kind', examples=['agent'])
    status: str | None = Field(None, description='Only schedules whose last run has this status',
                               examples=['failed'])
    enabled: bool | None = Field(None, description='True: active, false: paused', examples=[True])
    owner_ids: list[DbId] | None = Field(None, description='Only schedules of these owners; schedules admin only',
                                         examples=[[3, 7]])
    entity_id: DbId | None = Field(None, description='Only schedules of this agent or pipeline', examples=[12])
    version_id: DbId | None = Field(None, description='Only schedules that run this version', examples=[31])
    search: str | None = Field(None, description='Match on schedule or entity name', examples=['daily report'])
    limit: int = Field(default=20, ge=1, le=100, description='Rows per page', examples=[20])
    offset: int = Field(default=0, ge=0, le=2**31 - 1, description='Rows to skip',  # the database integer range
                        examples=[0])
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
    entity_type: str = Field(description='Kind of the scheduled entity: agent or pipeline')
    run_mode: str | None = Field(None, description='How a run starts: entry_point or prompt')
    application_id: int = Field(description='Id of the agent or pipeline')
    entity_name: str = Field(description='Name of the agent or pipeline')
    version_id: int | None = Field(None, description='Version that runs; null when the version is deleted')  # US-01
    version_name: str | None = Field(None, description='Name of the version that runs')
    cron: str = Field(description='Cron with 5 parts, read in `timezone`')
    timezone: str = Field(description='IANA time zone name')
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
