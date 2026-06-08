from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

MAX_CONTENT_SIZE = 2500


class ProjectContextUpdate(BaseModel):
    content: str = ''
    enabled: bool = True

    @field_validator('content')
    @classmethod
    def validate_content(cls, v: str) -> str:
        if len(v) > MAX_CONTENT_SIZE:
            raise ValueError(f'Content must not exceed {MAX_CONTENT_SIZE} characters')
        return v


class ProjectContextDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    content: str
    enabled: bool
    updated_at: Optional[datetime] = None
