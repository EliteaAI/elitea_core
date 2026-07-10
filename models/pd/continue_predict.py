from uuid import UUID
from typing import Optional, List, Dict, Any, Literal

from pydantic import BaseModel, Field, conint, model_validator


class ContinuePredictPayload(BaseModel):
    """REST/MCP analog of SioContinuePredictModel for the HITL resume / continue flow.

    Resumes a conversation paused at a HITL node. Keyed by ``message_id`` (the paused
    response message), mirroring the SIO ``chat_continue_predict`` event.
    """
    conversation_uuid: UUID
    message_id: str = Field(
        ..., description="UUID of the paused response message to resume (from the HITL interrupt).")
    hitl_resume: bool = Field(
        True, description="Whether this request resumes a HITL interrupt.")
    hitl_action: Optional[Literal["approve", "reject", "edit", "block_with_comment"]] = Field(
        None, description="HITL decision. Required when hitl_resume is true.")
    hitl_value: Optional[str] = Field(
        None, description="Edited text for 'edit', or the user's note for 'block_with_comment'.")
    hitl_decisions: Optional[List[Dict[str, Any]]] = Field(
        None, description="Per-child HITL decisions for a parallel sub-agent resume.")
    user_input: Optional[str] = Field(
        None, description="Optional input to use instead of the default 'continue'.")
    thread_id: Optional[str] = Field(
        None, description="Explicit thread id; falls back to the paused message's meta when omitted.")
    await_task_timeout: conint(ge=-1, le=300) = 30  # type: ignore

    @model_validator(mode='after')
    def require_hitl_action_when_resuming(self):
        if self.hitl_resume and self.hitl_action is None:
            raise ValueError("hitl_action is required when hitl_resume is true")
        return self
