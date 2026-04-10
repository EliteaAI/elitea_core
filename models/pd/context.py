from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any
from datetime import datetime

from .llm_settings_v2 import LLMSettingsModel


class ContextStrategyUpdate(BaseModel):
    summary_llm_settings: Optional[LLMSettingsModel] = None
    max_context_tokens: Optional[int] = Field(None, ge=1000)
    preserve_recent_messages: Optional[int] = Field(None, ge=1)
    preserve_system_messages: Optional[bool] = None
    enable_summarization: Optional[bool] = None
    summary_instructions: Optional[str] = None
    enabled: Optional[bool] = True

    @model_validator(mode='after')
    def validate_summary_max_tokens(self):
        if self.summary_llm_settings is not None and self.summary_llm_settings.max_tokens is not None:
            if self.max_context_tokens is not None and self.summary_llm_settings.max_tokens < 100:
                raise ValueError(
                    f'Summary max tokens ({self.summary_llm_settings.max_tokens}) must be at least 100'
                )
            if (
                self.max_context_tokens is not None
                and self.summary_llm_settings.max_tokens >= self.max_context_tokens
            ):
                raise ValueError(
                    f'Summary max tokens ({self.summary_llm_settings.max_tokens}) must be less than '
                    f'max context tokens ({self.max_context_tokens})'
                )
        return self


class ContextStrategy(BaseModel):
    """Complete context strategy configuration."""
    name: str = "default"
    enabled: bool = True
    enable_summarization: bool = True
    max_context_tokens: int = Field(64000, ge=1000)
    preserve_recent_messages: int = Field(5, ge=1, le=99)
    preserve_system_messages: bool = True
    summary_instructions: Optional[str] = 'Generate a concise summary of the following conversation messages'
    summary_llm_settings: Optional[dict] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    last_optimized_at: Optional[str] = None


class ContextStatus(BaseModel):
    current_tokens: int
    max_tokens: int
    utilization: float
    message_groups_in_context: int
    summary_count: int = 0
    context_analytics: Dict[str, Any]
