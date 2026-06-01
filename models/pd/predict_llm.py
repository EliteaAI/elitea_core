from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict


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
        description="For reasoning models (o1, o1-mini, claude-opus, etc.). Values: 'low', 'medium', 'high'"
    )


class ChatMessage(BaseModel):
    """Chat history message"""
    role: str = Field(description="Message role: 'user' or 'assistant'")
    content: str = Field(description="Message content")


class LLMPredictRequest(BaseModel):
    """Simplified LLM Prediction Request"""
    model_config = ConfigDict(
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
    llm_settings: Optional[LLMSettingsRequest] = Field(
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
    thread_id: Optional[str] = Field(
        default=None,
        description="Conversation thread ID for multi-turn conversations"
    )
    checkpoint_id: Optional[str] = Field(
        default=None,
        description="Checkpoint ID for resuming from a specific point"
    )
