from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal, List, Dict

from ..enums.all import ChatHistoryTemplates


# ------- applications --------
class ApplicationLlmSettings(BaseModel):
    temperature: Optional[float] = None
    reasoning_effort: Optional[str] = None
    context_window: Optional[int] = None
    max_output_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None


class EntitySettingsApplication(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    version_id: int = Field(alias='id')
    variables: Optional[List] = Field(default_factory=list)
    chat_history_template: Literal['all', 'interaction', 'context_managed'] | int = ChatHistoryTemplates.all.value
    icon_meta: Optional[Dict] = Field(default_factory=dict)


class EntitySettingsLlm(BaseModel):
    temperature: Optional[float] = None
    reasoning_effort: Optional[Literal['low', 'medium', 'high']] = None
    max_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None
    chat_history_template: Literal['all', 'context_managed'] | int = ChatHistoryTemplates.all.value


class EntitySettingsUser(BaseModel):
    llm_settings: Optional[EntitySettingsLlm] = {}
    chat_history_template: Literal['all', 'context_managed'] | int = ChatHistoryTemplates.all.value
