from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

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

    @field_validator("conversation_starters", mode="before")
    @classmethod
    def limit_conversation_starters(cls, v):
        if v is not None and len(v) > 4:
            return v[:4]
        return v

    suggested_toolkits: List[ToolkitSuggestion] = Field(
        default_factory=list,
        description="Toolkit instances (excluding MCP and application/pipeline types)"
    )
    suggested_mcp: List[ToolkitSuggestion] = Field(
        default_factory=list,
        description="MCP toolkit instances the agent likely needs"
    )
    suggested_pipelines: List[ApplicationSuggestion] = Field(
        default_factory=list,
        description="Pipeline applications the agent may want to call"
    )
    suggested_agents: List[ApplicationSuggestion] = Field(
        default_factory=list,
        description="Existing agents the agent may want to call"
    )

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
        remaining_toolkits = []
        for item in all_toolkit_items:
            item_type = item.get("type", "") if isinstance(item, dict) else getattr(item, "type", "")
            if item_type == "mcp":
                mcp_items.append(item)
            else:
                remaining_toolkits.append(item)
        data["suggested_toolkits"] = remaining_toolkits
        data["suggested_mcp"] = mcp_items
        # Split suggested_applications by application type
        all_app_items = []
        for key in ("suggested_applications", "suggested_agents", "suggested_pipelines"):
            all_app_items.extend(data.get(key) or [])
        agent_items = []
        pipeline_items = []
        for item in all_app_items:
            item_type = item.get("type", "") if isinstance(item, dict) else getattr(item, "type", "")
            if item_type == "pipeline":
                pipeline_items.append(item)
            else:
                agent_items.append(item)
        data["suggested_agents"] = agent_items
        data["suggested_pipelines"] = pipeline_items
        data.pop("suggested_applications", None)
        return data
