from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

from .predict_llm import LLMSettingsRequest

MAX_SUGGESTED_SKILLS = 5


class GenerateApplicationDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "user_description": "A code reviewer that checks GitHub merge requests and posts comments",
                    "llm_settings": {
                        "model_name": "gpt-5-mini",
                        "max_tokens": 4096,
                        "temperature": 0.7,
                    }
                },
                {
                    "user_description": "Improve my agent, suggest agents, pipelines, toolkits",
                    "llm_settings": {
                        "model_name": "gpt-5-mini",
                        "max_tokens": 4096,
                        "temperature": 0.7,
                    },
                    'application_id': 1,
                    'version_id': 1
                }
            ]
        }
    )

    user_description: str = Field(
        description="Natural-language description of the desired agent or changes (edit mode)"
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
        "uses the project's default model with temperature=0 and max_tokens=2048.",
    )
    application_id: Optional[int] = Field(
        default=None,
        description="Application ID to edit. When provided with version_id, enables edit mode.",
    )
    version_id: Optional[int] = Field(
        default=None,
        description="Version ID to edit. Required when application_id is provided.",
    )

    @model_validator(mode='after')
    def validate_edit_params(self):
        """Ensure both application_id and version_id are provided together."""
        if (self.application_id is None) != (self.version_id is None):
            raise ValueError("Both application_id and version_id must be provided for edit mode")
        return self

    @property
    def is_edit_mode(self) -> bool:
        return self.application_id is not None and self.version_id is not None


class ToolkitSuggestion(BaseModel):
    id: int = Field(description="Toolkit instance ID from elitea_tools table")
    type: str = Field(description="Toolkit type key, e.g. 'mcp', 'github', 'artifact'")
    name: str = Field(description="Toolkit instance name")
    description: Optional[str] = None


class ApplicationSuggestion(BaseModel):
    application_id: int
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None

    def model_post_init(self, __context):
        if self.id is None:
            self.id = self.application_id


class SkillSuggestion(BaseModel):
    id: int = Field(description="Skill ID from skills table")
    name: str = Field(description="Skill name")
    description: Optional[str] = Field(default=None, description="Skill description")


class GenerateApplicationDraftResponse(BaseModel):
    """Response for both create and edit modes.

    In CREATE mode: LLM suggests new resources to add.
    In EDIT mode: LLM returns the complete modified configuration with final resource lists.
    """
    name: str = Field(
        min_length=1, max_length=32, description="Agent name (1–32 characters)"
    )
    description: str = Field(
        min_length=1,
        max_length=2304,
        description="Agent description (1–2304 characters)",
    )
    instructions: str = Field(description="Agent system prompt / instructions")
    welcome_message: Optional[str] = Field(
        default=None, max_length=768, description="Welcome message (≤ 768 characters)"
    )
    conversation_starters: Optional[List[str]] = None

    @field_validator("conversation_starters", mode="before")
    @classmethod
    def limit_conversation_starters(cls, v):
        if v is None:
            return v
        v = [s.strip() for s in v if s and s.strip()]
        if len(v) > 4:
            raise ValueError(
                f'conversation_starters cannot exceed 4 items, got {len(v)}'
            )
        v = [s[:768] for s in v]
        return v

    suggested_toolkits: List[ToolkitSuggestion] = Field(
        default_factory=list,
        description="Toolkit instances (excluding MCP and application/pipeline types)",
    )
    suggested_mcp: List[ToolkitSuggestion] = Field(
        default_factory=list, description="MCP toolkit instances the agent likely needs"
    )
    suggested_pipelines: List[ApplicationSuggestion] = Field(
        default_factory=list,
        description="Pipeline/application instances the agent likely needs",
    )
    suggested_agents: List[ApplicationSuggestion] = Field(
        default_factory=list, description="Existing agents the agent may want to call"
    )
    suggested_skills: List[SkillSuggestion] = Field(
        default_factory=list,
        description=f"Skills the agent may want to use (max {MAX_SUGGESTED_SKILLS})",
    )

    @field_validator("suggested_skills", mode="before")
    @classmethod
    def limit_suggested_skills(cls, v):
        if v is None:
            return []
        if len(v) > MAX_SUGGESTED_SKILLS:
            v = v[:MAX_SUGGESTED_SKILLS]
        return v

    @field_validator("suggested_toolkits", mode="before")
    @classmethod
    def validate_toolkits(cls, v):
        if not v:
            return []
        result = []
        for item in v:
            if not isinstance(item, dict):
                continue
            if item.get("type", "").lower() == "mcp":
                continue
            elif item.get("type", "").lower() == "application":
                continue
            elif item.get("type"):
                result.append(item)
        return result

    @field_validator("suggested_mcp", mode="before")
    @classmethod
    def validate_mcp(cls, v):
        if not v:
            return []
        result = []
        for item in v:
            if not isinstance(item, dict):
                continue
            if item.get("type", "").lower() == "mcp":
                result.append(item)
        return result

    @field_validator("suggested_agents", mode="before")
    @classmethod
    def validate_agents(cls, v):
        if not v:
            return []
        result = []
        for item in v:
            if not isinstance(item, dict):
                continue
            if item.get("type", "").lower() == "agent":
                app_id = item.get("application_id")
                if app_id:
                    result.append({
                        "application_id": app_id,
                        "id": app_id,
                        "name": item.get("name"),
                        "description": item.get("description"),
                        "type": "agent",
                    })
        return result

    @field_validator("suggested_pipelines", mode="before")
    @classmethod
    def validate_pipelines(cls, v):
        if not v:
            return []
        result = []
        for item in v:
            if not isinstance(item, dict):
                continue
            if item.get("type", "").lower() == "pipeline":
                app_id = item.get("application_id")
                if app_id:
                    result.append({
                        "application_id": app_id,
                        "id": app_id,
                        "name": item.get("name"),
                        "description": item.get("description"),
                        "type": "pipeline",
                    })
        return result
