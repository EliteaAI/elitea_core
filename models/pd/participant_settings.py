from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal, List, Dict

from ..enums.all import ChatHistoryTemplates


class EntitySettingsLlm(BaseModel):
    """Canonical LLM settings model used for user-level and per-conversation overrides."""
    temperature: Optional[float] = None
    reasoning_effort: Optional[Literal['low', 'medium', 'high']] = None
    max_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None
    chat_history_template: Literal['all', 'context_managed'] | int = ChatHistoryTemplates.all.value


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
