from __future__ import annotations

from typing import Optional
from uuid import UUID

from pydantic import BaseModel, field_validator, model_validator


class EnterRoomPayload(BaseModel):
    project_id: int
    conversation_id: int | UUID | str


class LeaveRoomPayload(BaseModel):
    conversation_uuid: UUID | str


class JoinCanvasPayload(BaseModel):
    project_id: int
    canvas_uuid: UUID | str


class EditCanvasPayload(BaseModel):
    project_id: int
    canvas_uuid: UUID | str
    content: Optional[str] = None


class CanvasLeavePayload(BaseModel):
    project_id: int
    canvas_uuid: UUID | str
    canvas_content: str
    code_language: Optional[str] = None


class TestToolkitEnterRoomPayload(BaseModel):
    stream_id: UUID | str
    event_name: Optional[str] = "test_toolkit_tool"

class EvalRunRoomPayload(BaseModel):
    project_id: int
    run_id: int

class NextInputSuggestionPayload(BaseModel):
    sid: str
    suggestions: list[str]
    stream_id: str | None = None
    message_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def coerce_suggestion(cls, data: dict) -> dict:
        if "suggestions" not in data and "suggestion" in data:
            data = {**data, "suggestions": [data["suggestion"]]}
        return data

    @field_validator("suggestions")
    @classmethod
    def cap_suggestions(cls, v: list[str]) -> list[str]:
        return v[:3]
