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
    hitl_action: Optional[Literal["approve", "reject", "edit", "block_with_comment", "answer"]] = Field(
        None, description="HITL decision. Required when hitl_resume is true.")
    hitl_value: Optional[str | Dict[str, Any]] = Field(
        None, description="Edited text for 'edit', the user's note for 'block_with_comment', or the answers object for a clarifying-question 'answer'.")
    hitl_decisions: Optional[List[Dict[str, Any]]] = Field(
        None, description="Per-child HITL decisions for a parallel sub-agent resume.")
    mcp_auth_resume: bool = Field(
        False, description="Whether this request resumes a durable Toolkit authorization interrupt.")
    mcp_auth_action: Optional[Literal["authorize", "skip"]] = Field(
        None, description="Toolkit authorization decision. Required when mcp_auth_resume is true.")
    mcp_auth_decisions: Optional[List[Dict[str, Any]]] = Field(
        None, description="Per-request Toolkit authorization decisions for a durable child resume.")
    authorization_request_id: Optional[str] = Field(
        None, description="Exact durable Toolkit authorization request being resolved.")
    user_input: Optional[str] = Field(
        None, description="Optional input to use instead of the default 'continue'.")
    thread_id: Optional[str] = Field(
        None, description="Explicit thread id; falls back to the paused message's meta when omitted.")
    await_task_timeout: conint(ge=-1, le=300) = 30  # type: ignore

    @model_validator(mode='before')
    @classmethod
    def select_resume_protocol(cls, values):
        if (
            isinstance(values, dict)
            and values.get('mcp_auth_resume')
            and 'hitl_resume' not in values
        ):
            values = dict(values)
            values['hitl_resume'] = False
        return values

    @model_validator(mode='after')
    def require_hitl_action_when_resuming(self):
        if self.hitl_resume and self.hitl_action is None:
            raise ValueError("hitl_action is required when hitl_resume is true")
        if self.mcp_auth_resume and self.mcp_auth_action is None:
            raise ValueError("mcp_auth_action is required when mcp_auth_resume is true")
        if self.hitl_resume and self.mcp_auth_resume:
            raise ValueError("hitl_resume and mcp_auth_resume are mutually exclusive")
        return self

    @model_validator(mode='after')
    def require_hitl_value_for_text_actions(self):
        # 'edit' needs the replacement text; 'block_with_comment' needs the note.
        # Without this the resume proceeds silently on a downstream fallback.
        if self.hitl_action in ("edit", "block_with_comment") and not self.hitl_value:
            raise ValueError(f"hitl_value is required when hitl_action is '{self.hitl_action}'")
        return self
