from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

from .predict_llm import LLMSettingsRequest

MAX_SUGGESTED_SKILLS = 5


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
    name: str
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
        v = [s for s in v if s and s.strip()]
        if len(v) > 4:
            v = v[:4]
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

    @model_validator(mode="before")
    @classmethod
    def split_by_type(cls, data):
        if not isinstance(data, dict):
            return data
        # Split suggested_toolkits by toolkit type
        all_toolkit_items = []
        for key in ("suggested_toolkits", "suggested_mcp"):
            all_toolkit_items.extend(data.get(key) or [])
        mcp_items = []
        pipeline_items = []
        remaining_toolkits = []
        for item in all_toolkit_items:
            item_type = (
                item.get("type", "")
                if isinstance(item, dict)
                else getattr(item, "type", "")
            ).lower()
            if item_type == "mcp":
                mcp_items.append(item)
            elif item_type == "application":
                pipeline_items.append(
                    {
                        "application_id": item.get("id"),
                        "id": item.get("id"),
                        "name": item.get("name", ""),
                        "description": item.get("description"),
                        "type": "pipeline",
                    }
                )
            else:
                remaining_toolkits.append(item)
        data["suggested_toolkits"] = remaining_toolkits
        data["suggested_mcp"] = mcp_items
        # Split suggested_applications by application type into agents and pipelines
        all_app_items = []
        for key in (
            "suggested_applications",
            "suggested_agents",
            "suggested_pipelines",
        ):
            all_app_items.extend(data.get(key) or [])
        agent_items = []
        for item in all_app_items:
            item_type = (
                item.get("type", "")
                if isinstance(item, dict)
                else getattr(item, "type", "")
            ).lower()
            if item_type == "pipeline":
                pipeline_items.append(item)
            else:
                agent_items.append(item)
        data["suggested_agents"] = agent_items
        data["suggested_pipelines"] = pipeline_items
        data.pop("suggested_applications", None)
        return data
