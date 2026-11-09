"""File models of the schedule YAML export and import (EL-7011)."""
from datetime import date
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .entity_schedule import (
    DESCRIPTION_MAX,
    INDEX_NAME_MAX,
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
    entity_type: Literal['agent', 'pipeline', 'toolkit_index']
    entity_name: str  # the agent, pipeline or toolkit name
    index_name: str | None = Field(None, min_length=1, max_length=INDEX_NAME_MAX)  # Toolkit Index items only
    version_name: str | None = None
    run_mode: RunMode | None = None
    cron: str
    timezone: str
    prompt: str | None = None
    variables: list[ScheduleVariable] = []
    expires_at: date | None = None
    approval_rule: ApprovalRule = 'pause_and_ask'

    @model_validator(mode='after')
    def check_target_fields(self):
        """A Toolkit Index item names its index and carries no run field; the other kinds carry no index name."""
        if self.entity_type != 'toolkit_index':
            if self.index_name is not None:
                raise ValueError('Only a Toolkit Index schedule takes an index_name')
            return self
        if self.index_name is None:
            raise ValueError('A Toolkit Index schedule needs an index_name')
        run_fields = (
            ('version_name', self.version_name is not None), ('run_mode', self.run_mode is not None),
            ('prompt', self.prompt is not None), ('variables', bool(self.variables)),
            ('approval_rule', self.approval_rule != 'pause_and_ask'),  # an index has no tool to approve
        )
        for field, is_set in run_fields:
            if is_set:
                raise ValueError(f'A Toolkit Index schedule takes no {field}')
        return self
