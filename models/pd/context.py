from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any, Literal
from datetime import datetime

from .llm import LLMSettingsWriteModel


class ContextStrategyUpdate(BaseModel):
    summary_llm_settings: Optional[LLMSettingsWriteModel] = None
    context_window_mode: Optional[Literal['balanced', 'full']] = None
    # Deprecated (#6898): kept for public API clients, superseded by context_window_mode
    max_context_tokens: Optional[int] = Field(None, ge=1000)
    # Platform admins only, when the platform toggle is on; null clears it
    manual_max_context_tokens: Optional[int] = Field(None, ge=1000)
    preserve_recent_messages: Optional[int] = Field(None, ge=1)
    preserve_system_messages: Optional[bool] = None
    enable_summarization: Optional[bool] = None
    enable_context_editing: Optional[bool] = None
    summary_instructions: Optional[str] = None
    # Deprecated (#6898): context management is always on; accepted from old clients and ignored
    enabled: Optional[bool] = True

    @model_validator(mode='after')
    def validate_summary_max_tokens(self):
        if self.summary_llm_settings is not None and self.summary_llm_settings.max_tokens is not None:
            if self.summary_llm_settings.max_tokens < 100:
                raise ValueError(
                    f'Summary max tokens ({self.summary_llm_settings.max_tokens}) must be at least 100'
                )
        return self


class ContextStrategy(BaseModel):
    """Complete context strategy configuration."""
    name: str = "default"
    enabled: bool = True
    enable_summarization: bool = True
    enable_context_editing: bool = False
    context_window_mode: Literal['balanced', 'full'] = 'balanced'
    max_context_tokens: Optional[int] = Field(None, ge=1000)
    preserve_recent_messages: int = Field(5, ge=1, le=99)
    preserve_system_messages: bool = True
    summary_instructions: Optional[str] = 'Generate a concise summary of the following conversation messages'
    summary_llm_settings: Optional[dict] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    last_optimized_at: Optional[str] = None

    @field_validator('enabled', mode='before')
    @classmethod
    def always_enabled(cls, _value):
        return True


class ContextStatus(BaseModel):
    current_tokens: int
    max_tokens: int
    utilization: float
    message_groups_in_context: int
    summary_count: int = 0
    token_source: str = 'approximate'
    token_breakdown: Dict[str, Any] = Field(default_factory=dict)
    context_analytics: Dict[str, Any]
