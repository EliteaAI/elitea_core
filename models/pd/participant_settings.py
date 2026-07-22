from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Literal, List, Dict

from ..enums.all import ChatHistoryTemplates
from .llm import llm_settings_family_conflict


class EntitySettingsLlmBase(BaseModel):
    """Shared fields for per-conversation/participant LLM overrides."""
    temperature: Optional[float] = None
    reasoning_effort: Optional[Literal['low', 'medium', 'high']] = None
    max_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None
    chat_history_template: Literal['all', 'context_managed'] | int = ChatHistoryTemplates.all.value


class EntitySettingsLlm(EntitySettingsLlmBase):
    """Canonical LLM settings model used for user-level and per-conversation overrides.

    Read/response variant — self-heals a conflicting temperature/reasoning_effort combo
    instead of failing. Use EntitySettingsLlmWrite for validating caller-authored input."""

    @model_validator(mode="after")
    def _auto_correct_family_conflict(self) -> "EntitySettingsLlm":
        if llm_settings_family_conflict(self.temperature, self.reasoning_effort):
            self.temperature = None
        return self


class EntitySettingsLlmWrite(EntitySettingsLlmBase):
    """Write variant — rejects a conflicting temperature/reasoning_effort combo with a clear
    validation error instead of silently persisting it. Use for validating incoming request
    payloads (entity_settings PUT/PATCH)."""

    @model_validator(mode="after")
    def _reject_family_conflict(self) -> "EntitySettingsLlmWrite":
        if llm_settings_family_conflict(self.temperature, self.reasoning_effort):
            raise ValueError(
                "temperature is not allowed together with a reasoning_effort (other than "
                "'none') — reasoning models reject a custom temperature"
            )
        return self


# ------- applications --------
class EntitySettingsApplication(BaseModel):
    """Per-conversation settings for an application participant."""
    model_config = ConfigDict(populate_by_name=True)

    version_id: int = Field(alias='id')
    variables: Optional[List] = Field(default_factory=list)
    chat_history_template: Literal['all', 'interaction', 'context_managed'] | int = ChatHistoryTemplates.all.value
    icon_meta: Optional[Dict] = Field(default_factory=dict)
    llm_settings: Optional[EntitySettingsLlm] = None


class EntitySettingsUser(BaseModel):
    llm_settings: Optional[EntitySettingsLlm] = {}
    chat_history_template: Literal['all', 'context_managed'] | int = ChatHistoryTemplates.all.value
