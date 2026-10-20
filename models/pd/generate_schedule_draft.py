"""Request and response of the Build with AI schedule draft endpoint (EL-7011, US-21)."""
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, constr


class LockedEntity(BaseModel):
    """Type and entity fixed by the editor the form was opened from."""
    entity_type: Literal['agent', 'pipeline']
    application_id: int


class GenerateScheduleDraftRequest(BaseModel):
    prompt: constr(strip_whitespace=True, min_length=1) = Field(description="The user's description of the schedule")
    current: dict = Field(default_factory=dict, description="Current form values; empty for a new schedule")
    locked_entity: LockedEntity | None = None
    timezone: str | None = Field(default=None, description="The browser time zone")


class ScheduleDraft(BaseModel):
    """The twelve form fields; an unusable field is null (variables: empty list)."""
    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: int | None = None
    version_id: int | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list[dict] = Field(default_factory=list)
    cron: str | None = None
    timezone: str | None = None
    expires_at: date | None = None
    approval_rule: str | None = None


class ScheduleDraftWarning(BaseModel):
    field: str
    reason: str


class GenerateScheduleDraftResponse(BaseModel):
    draft: ScheduleDraft
    warnings: list[ScheduleDraftWarning] = Field(default_factory=list)
