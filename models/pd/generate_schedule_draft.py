"""Request and response of the Build with AI schedule draft endpoint (EL-7011, US-21)."""
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, constr


class LockedEntity(BaseModel):
    """Type and entity fixed by the editor the form was opened from."""
    entity_type: Literal['agent', 'pipeline'] = Field(description="Kind of the entity", examples=["agent"])
    application_id: int = Field(description="Id of the agent or pipeline", examples=[12])


class CurrentForm(BaseModel):
    """Current form values; a known key with the wrong type fails, other keys pass through."""
    model_config = ConfigDict(extra='allow')

    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: StrictInt | None = None
    version_id: StrictInt | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list | dict | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: str | None = None
    approval_rule: str | None = None


class CurrentForm(BaseModel):
    """Current form values; a known key with the wrong type fails, other keys pass through."""
    model_config = ConfigDict(extra='allow')

    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: StrictInt | None = None
    version_id: StrictInt | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list | dict | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: str | None = None
    approval_rule: str | None = None


class CurrentForm(BaseModel):
    """Current form values; a known key with the wrong type fails, other keys pass through."""
    model_config = ConfigDict(extra='allow')

    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: StrictInt | None = None
    version_id: StrictInt | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list | dict | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: str | None = None
    approval_rule: str | None = None


class CurrentForm(BaseModel):
    """Current form values; a known key with the wrong type fails, other keys pass through."""
    model_config = ConfigDict(extra='allow')

    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: StrictInt | None = None
    version_id: StrictInt | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list | dict | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: str | None = None
    approval_rule: str | None = None


class CurrentForm(BaseModel):
    """Current form values; a known key with the wrong type fails, other keys pass through."""
    model_config = ConfigDict(extra='allow')

    name: str | None = None
    description: str | None = None
    entity_type: str | None = None
    application_id: StrictInt | None = None
    version_id: StrictInt | None = None
    run_mode: str | None = None
    prompt: str | None = None
    variables: list | dict | None = None
    cron: str | None = None
    timezone: str | None = None
    expires_at: str | None = None
    approval_rule: str | None = None


class GenerateScheduleDraftRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": [{
        "prompt": "Run the report agent every weekday at 9", "timezone": "Europe/Warsaw",
    }]})

    prompt: constr(strip_whitespace=True, min_length=1) = Field(
        description="The user's description of the schedule", examples=["Run the report agent every weekday at 9"])
    current: CurrentForm = Field(default_factory=CurrentForm, description="Current form values; empty for a new schedule",
                          examples=[{"name": "Daily report", "cron": "0 9 * * 1-5"}])
    locked_entity: LockedEntity | None = Field(
        default=None, description="Entity the form is fixed to; the draft keeps it",
        examples=[{"entity_type": "agent", "application_id": 12}])
    timezone: str | None = Field(default=None, description="The browser time zone", examples=["Europe/Warsaw"])


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
