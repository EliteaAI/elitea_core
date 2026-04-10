from __future__ import annotations

from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class EnterRoomPayload(BaseModel):
    project_id: int
    conversation_id: int


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
