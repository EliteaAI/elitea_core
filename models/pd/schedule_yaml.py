"""File models of the schedule YAML export and import (EL-7011)."""
from datetime import date
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .entity_schedule import (
    DESCRIPTION_MAX,
    NAME_MAX,
    ApprovalRule,
    RunMode,
    ScheduleVariable,
)

WRONG_HEADER = 'This is not a schedule file of schema version 1 or 2'


class ScheduleFile(BaseModel):
    """The header is strict. Items stay raw dicts, so one bad item fails alone (R2.4)."""
    kind: Literal['elitea_schedules']
    schema_version: Literal[1, 2]
    schedules: list[dict[str, Any]]

    @field_validator('kind', 'schema_version', mode='wrap')
    @classmethod
    def validate_header(cls, v, handler):
        # Literal[1, 2] accepts True; the file must say a number
        if isinstance(v, bool):
            raise ValueError(WRONG_HEADER)  # noqa: TRY004 - pydantic needs ValueError
        try:
            return handler(v)
        except ValueError:
            raise ValueError(WRONG_HEADER) from None


class ScheduleFileItem(BaseModel):
    """One schedule of the file: the allow-listed keys only. Timing rules come from ScheduleCreate."""
    model_config = ConfigDict(extra='forbid')

    key: UUID | None = None  # the schedule uuid; version 1 files have none
    name: str = Field(min_length=1, max_length=NAME_MAX)
    description: str | None = Field(None, max_length=DESCRIPTION_MAX)
    entity_type: Literal['agent', 'pipeline']
    entity_name: str
    version_name: str | None = None
    run_mode: RunMode | None = None
    cron: str
    timezone: str
    prompt: str | None = None
    variables: list[ScheduleVariable] = []
    expires_at: date | None = None
    approval_rule: ApprovalRule = 'pause_and_ask'
