from datetime import datetime
from typing import Optional, Any

from pydantic import BaseModel, ConfigDict


class TraceStepListItem(BaseModel):
    """Light projection for the pin list: labels + ordering only, no heavy fields."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    message_group_id: int
    kind: str
    tool_name: Optional[str] = None
    parent_agent_name: Optional[str] = None
    parent_agent_call_id: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    is_error: bool = False
    step_type: Optional[str] = None
    model_name: Optional[str] = None
    finish_reason: Optional[str] = None


class TraceStepDetail(TraceStepListItem):
    """Full single-step payload fetched on pin expand: adds the heavy fields."""
    tool_inputs: Optional[Any] = None
    tool_output: Optional[str] = None
    text: Optional[str] = None
    thinking: Optional[str] = None
    attrs: Optional[dict] = None
