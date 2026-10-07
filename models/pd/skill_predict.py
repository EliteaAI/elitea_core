from typing import Annotated, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SkillRunLLMSettings(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        json_schema_extra={'example': {'model_name': 'gpt-5-mini', 'temperature': 0.2}},
    )

    model_name: Optional[str] = Field(default=None, description="Model to run the skill with")
    model_project_id: Optional[int] = Field(
        default=None, description="Project the model is configured in. Resolved from the model name when omitted."
    )
    temperature: Optional[Annotated[float, Field(gt=0, le=1)]] = None
    max_tokens: Optional[int] = Field(default=None, gt=0)
    reasoning_effort: Optional[str] = None


class SkillPredictRequest(BaseModel):
    model_config = ConfigDict(
        extra='forbid',
        json_schema_extra={
            'example': {
                'user_input': 'Review this paragraph for tone.',
                'chat_history': [],
                'llm_settings': {'model_name': 'gpt-5-mini', 'temperature': 0.2},
                'async_mode': False,
            }
        },
    )

    user_input: str | List[dict] = Field(description="User message: text or content blocks")
    chat_history: List[dict] = Field(default_factory=list, description="Prior turns, oldest first")
    llm_settings: Optional[SkillRunLLMSettings] = Field(
        default=None,
        description="Overrides the skill's saved model settings; the caller project's default model is used when neither is set",
    )
    async_mode: bool = False
    callback_url: Optional[str] = None
    callback_headers: Optional[Dict[str, str]] = None
    return_chat_history: bool = False
