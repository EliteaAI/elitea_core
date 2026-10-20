"""Pydantic models for the schedule list query arguments and rows."""
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

DbId = Annotated[int, Field(ge=1, le=2**31 - 1)]  # the database integer range


class ScheduleListFilters(BaseModel):
    """Query arguments of the schedule list; a bad value names its field."""

    entity_type: str | None = None
    status: str | None = None
    enabled: bool | None = None
    owner_ids: list[DbId] | None = None
    entity_id: DbId | None = None
    search: str | None = None
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=2**31 - 1)  # the database integer range

    @field_validator('owner_ids', mode='before')
    @classmethod
    def _split_owner_ids(cls, value):
        """Accept a comma separated query string as a list of ids."""
        if isinstance(value, str):
            return value.split(',')
        return value


class ScheduleOwner(BaseModel):
    """Owner of a schedule; the name is null when the lookup fails."""

    id: int
    name: str | None = None


class ScheduleListItem(BaseModel):
    """One row of the schedule list; it has no tags field."""

    id: int
    name: str
    description: str | None = None
    entity_type: str
    run_mode: str | None = None
    application_id: int
    entity_name: str
    version_id: int | None = None  # a deleted version sets it to null (US-01)
    version_name: str | None = None
    cron: str
    timezone: str
    enabled: bool
    disabled_reason: str | None = None
    next_run: datetime | None = None
    last_run: datetime | None = None
    last_run_status: str | None = None
    last_run_reason: str | None = None
    last_conversation_id: int | None = None
    expires_at: datetime | None = None
    owner: ScheduleOwner
    allowed_actions: list[str]
