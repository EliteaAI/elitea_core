from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class ChatTemplateParticipant(BaseModel):
    id: int
    name: Optional[str] = None
    entity_name: str
    project_id: Optional[int] = None
    agent_type: Optional[str] = None
    toolkit_type: Optional[str] = None


class ChatTemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    participants: List[ChatTemplateParticipant] = Field(default_factory=list)

    @field_validator('name')
    @classmethod
    def name_not_blank(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError('name must not be blank')
        return stripped


class ChatTemplateUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    participants: List[ChatTemplateParticipant] = Field(default_factory=list)

    @field_validator('name')
    @classmethod
    def name_not_blank(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError('name must not be blank')
        return stripped


class ChatTemplateRead(BaseModel):
    id: int
    name: str
    participants: List[ChatTemplateParticipant]
    is_default: bool
    created_at: datetime
    updated_at: datetime

    model_config = {'from_attributes': True}
