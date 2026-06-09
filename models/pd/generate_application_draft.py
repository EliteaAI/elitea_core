from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict

from .predict_llm import LLMSettingsRequest


class GenerateApplicationDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_description": "A code reviewer that checks GitHub merge requests and posts comments",
                "llm_settings": {
                    "model_name": "gpt-5-mini",
                    "max_tokens": 2048,
                    "temperature": 0,
                },
            }
        }
    )

    user_description: str = Field(description="Natural-language description of the desired agent")
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
                    "uses the project's default model with temperature=0 and max_tokens=2048.",
    )


class ToolkitSuggestion(BaseModel):
    id: int = Field(description="Toolkit instance ID from elitea_tools table")
    type: str = Field(description="Toolkit type key, e.g. 'mcp', 'github', 'artifact'")
    name: str = Field(description="Toolkit instance name")
    description: Optional[str] = None


class ApplicationSuggestion(BaseModel):
    application_id: int
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    type: Optional[str] = None

    def model_post_init(self, __context):
        if self.id is None:
            self.id = self.application_id


class GenerateApplicationDraftResponse(BaseModel):
    name: str = Field(max_length=32, description="Agent name (≤ 32 characters)")
    description: Optional[str] = None
    instructions: str = Field(description="Agent system prompt / instructions")
    welcome_message: Optional[str] = None
    conversation_starters: Optional[List[str]] = None

    suggested_toolkits: List[ToolkitSuggestion] = Field(
        default_factory=list,
        description="Toolkit types the agent likely needs — requires user confirmation before linking"
    )
    suggested_applications: List[ApplicationSuggestion] = Field(
        default_factory=list,
        description="Existing agents/pipelines the agent may want to call — requires user confirmation"
    )
