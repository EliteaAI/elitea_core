from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, constr, field_validator


class SkillRunStatus(str, Enum):
    running = 'running'
    success = 'success'
    error = 'error'
    stopped = 'stopped'


class SkillRunFilters(BaseModel):
    model_config = ConfigDict(extra='ignore')

    created_from: Optional[datetime] = None
    created_to: Optional[datetime] = None
    author_id: Optional[int] = None
    version_id: Optional[int] = None
    status: Optional[SkillRunStatus] = None
    model: Optional[constr(strip_whitespace=True, min_length=1, max_length=256)] = None

    @field_validator('*', mode='before')
    @classmethod
    def blank_as_unset(cls, value):
        return None if value == '' else value

    @field_validator('created_from', 'created_to')
    @classmethod
    def as_naive_utc(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None or value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
