from typing import Any, Dict, Literal, Optional, List
from pydantic import BaseModel, Field, ConfigDict, model_validator


class LLMSettingsRequest(BaseModel):
    """LLM Model Configuration"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "model_name": "gpt-5-mini",
                "model_project_id": 1,
                "max_tokens": 2048,
                "temperature": 0.7,
            }
        }
    )

    model_name: Optional[str] = Field(
        default=None,
        description="LLM model name (e.g., 'gpt-4o', 'claude-sonnet-4-6'). "
                    "If not provided, uses project's default model."
    )
    model_project_id: Optional[int] = Field(
        default=None,
        description="Project ID where model is configured. Defaults to current project_id."
    )
    max_tokens: Optional[int] = Field(
        default=2048,
        description="Maximum tokens in response"
    )
    temperature: Optional[float] = Field(
        default=0.7,
        ge=0,
        le=1,
        description="Response creativity (0=deterministic, 1=random)"
    )
    reasoning_effort: Optional[str] = Field(
        default=None,
        description=(
            "For reasoning models. One of the model's configured supported_efforts "
            "(none, minimal, low, medium, high, xhigh, max); models without that configuration take low, medium or high"
        )
    )


class PredictLLMSettingsRequest(LLMSettingsRequest):
    """/predict_llm settings: the model selection is honoured only on this endpoint."""
    selection: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Model selection, the same shape agents and skill run settings store: "
            "{'mode': 'auto', 'profile_ref': ..., 'scope_mode': ..., 'reasoning': ...} lets Auto pick the model "
            "(leave model_name, model_project_id and reasoning_effort unset); {'mode': 'fixed', 'model_ref': ...} "
            "pins one. Validated with the chat request."
        )
    )

    @model_validator(mode='after')
    def _drop_default_temperature_for_auto(self):
        if (self.selection or {}).get('mode') == 'auto' and 'temperature' not in self.model_fields_set:
            self.temperature = None
        return self

    @model_validator(mode='after')
    def _drop_default_max_tokens_for_auto(self):
        if (self.selection or {}).get('mode') == 'auto' and 'max_tokens' not in self.model_fields_set:
            self.max_tokens = None
        return self


class ChatMessage(BaseModel):
    """Chat history message"""
    role: str = Field(description="Message role: 'user' or 'assistant'")
    content: str = Field(description="Message content")


class LLMPredictRequest(BaseModel):
    """Simplified LLM Prediction Request"""
    model_config = ConfigDict(
        extra='allow',
        json_schema_extra={
            "examples": [
                {
                    "user_input": "What is the capital of France?",
                    "llm_settings": {
                        "model_name": "gpt-5-mini",
                        "model_project_id": 1,
                        "max_tokens": 2048,
                        "temperature": 0.7,
                    },
                    "chat_history": [],
                    "instructions": "You are a helpful assistant.",
                    "await_task_timeout": 30,
                },
            ]
        }
    )

    user_input: str = Field(
        description="The message to send to the LLM"
    )
    llm_settings: Optional[PredictLLMSettingsRequest] = Field(
        default=None,
        description="LLM model settings. If not provided, uses project's default model."
    )
    chat_history: Optional[List[ChatMessage]] = Field(
        default=[],
        description="Previous messages for context (optional)"
    )
    instructions: Optional[str] = Field(
        default=None,
        description="System instructions for the LLM (optional)"
    )
    await_task_timeout: Optional[int] = Field(
        default=30,
        ge=0,
        description="Seconds to wait for response (0=async, >0=blocking). Default: 30"
    )
    sid: Optional[str] = Field(
        default=None,
        description="Socket.IO session ID for streaming responses"
    )
    stream_id: Optional[str] = Field(
        default=None,
        description="Stream ID for SIO room scoping. Clients SHOULD provide this so they can subscribe to the matching room before the request. If omitted, the server generates one (which the client cannot listen for)."
    )
    message_id: Optional[str] = Field(
        default=None,
        description="Message ID for the LLM turn. If omitted, the server generates one."
    )
    thread_id: Optional[str] = Field(
        default=None,
        description="Conversation thread ID for multi-turn conversations"
    )
    checkpoint_id: Optional[str] = Field(
        default=None,
        description="Checkpoint ID for resuming from a specific point"
    )
    return_chat_history: bool = Field(
        default=False,
        description="Include chat_history in response (only needed for blocking callers that read the result)"
    )
    routing_surface: Literal['chat', 'agent'] = Field(
        default='chat',
        description=(
            "How Auto model selection classifies the request: 'chat' for a chat turn, 'agent' for an agent task "
            "(a skill run is an agent task). Only affects Auto."
        )
    )
    include_project_context: bool = Field(
        default=False,
        description="Deliver the project's context with the instructions, the same way agents and skill runs receive it"
    )

    def routing_projection(self) -> Optional[dict]:
        if not self.include_project_context:
            return None
        return {'instructions': self.instructions or '', 'task': self.user_input}
