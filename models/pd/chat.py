import uuid
from typing import Optional, List, Dict, Any

from jinja2 import Environment, DebugUndefined
from pydantic import BaseModel, Field, model_validator, ConfigDict
# from .tool import ToolUpdateModel
from .tool import ToolChatModel
from .utils import MergeUpdateBase

from .version import ApplicationVariableModel, LLMSettingsModel


def _resolve_variables(text, vars) -> str:
    environment = Environment(undefined=DebugUndefined)
    template = environment.from_string(text)
    converted_vars = {d.name: d.value for d in vars}
    return template.render(**converted_vars)


# Merged from promptlib_shared.models.pd.chat
class ChatHistory(BaseModel):
    role: str
    content: str | list
    additional_kwargs: Optional[Dict[str, Any]] = {}


ChatHistoryMessage = ChatHistory


class ContextStrategyModel(BaseModel):
    enabled: Optional[bool] = True
    enable_summarization: Optional[bool] = True
    max_context_tokens: Optional[int] = 64000
    preserve_recent_messages: Optional[int] = 5
    summary_instructions: Optional[str] = 'Generate a concise summary of the following conversation messages'
    summary_llm_settings: Optional[LLMSettingsModel] = None


class ApplicationChatRequest(MergeUpdateBase):
    application_id: Optional[int] = None
    version_name: Optional[str] = Field(default=None, alias="name", description="Version name (e.g., 'v1.0', 'base')")
    application_name: Optional[str] = Field(default=None, description="Agent/Application display name for observability traces")
    user_input: Optional[str | list] = None
    hitl_resume: Optional[bool] = False
    hitl_action: Optional[str] = None
    hitl_value: Optional[str] = None
    # Parallel sub-agent fan-out (#4993): per-child HITL decisions keyed by the
    # parent Application tool_call_id. {tool_call_id, action, value}.
    hitl_decisions: Optional[List[Dict[str, Any]]] = None
    chat_history: Optional[List[ChatHistoryMessage]] = []
    instructions: Optional[str] = None
    variables: Optional[List[ApplicationVariableModel]] = None
    # tools: Optional[List[ToolUpdateModel]]
    tools: Optional[List[ToolChatModel]] = []
    llm_settings: Optional[LLMSettingsModel] = None
    project_id: int
    version_id: Optional[int] = Field(default=None, alias='id')
    message_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    stream_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    thread_id: Optional[str] = None
    checkpoint_id: Optional[str] = None
    interaction_uuid: str | uuid.UUID | None = None
    version_details: Optional[dict] = None
    internal_tools: Optional[List[str]] = []
    invoked_skills: Optional[List[dict]] = Field(
        default=None,
        description="Per-turn resolved skill bodies (from ~skill refs) injected at the LLM node",
    )
    mcp_tokens: Optional[Dict[str, str | Dict[str, Any]]] = Field(
        default_factory=dict,
        description="MCP OAuth tokens by server URL (string for legacy, dict with access_token/session_id for new format)"
    )
    ignored_mcp_servers: Optional[List[str]] = Field(
        default_factory=list,
        description="List of MCP server URLs to ignore (user chose to continue without auth)"
    )
    should_continue: Optional[bool] = False
    meta: Optional[dict] = {}
    conversation_id: Optional[str] = Field(
        default=None,
        description="Conversation UUID for planning toolkit scoping"
    )
    context_settings: Optional[ContextStrategyModel] = Field(
        default=None,
        description="Context settings for the LLM"
    )
    is_regenerate: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @model_validator(mode='after')
    def check_version_details_reachable(self):
        app_ids = bool(self.application_id and self.version_id)
        version_details = bool(self.version_details)

        if sum([app_ids, version_details]) == 0:
            raise ValueError("Either 'application_id' and 'version_id' or 'version_details' must be provided.")

        return self


class LLMChatRequest(MergeUpdateBase):
    user_input: Optional[str | list] = None
    hitl_resume: Optional[bool] = False
    hitl_action: Optional[str] = None
    hitl_value: Optional[str] = None
    # Parallel sub-agent fan-out (#4993): per-child HITL decisions keyed by the
    # parent Application tool_call_id. {tool_call_id, action, value}.
    hitl_decisions: Optional[List[Dict[str, Any]]] = None
    chat_history: Optional[List[ChatHistoryMessage]] = []
    instructions: Optional[str] = None
    tools: Optional[List[dict]] = []
    llm_settings: Optional[LLMSettingsModel] = None
    project_id: int
    message_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    stream_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    thread_id: Optional[str] = None
    checkpoint_id: Optional[str] = None
    interaction_uuid: str | uuid.UUID | None = None
    internal_tools: Optional[List[str]] = []
    invoked_skills: Optional[List[dict]] = Field(
        default=None,
        description="Per-turn resolved skill bodies (from ~skill refs) injected at the LLM node",
    )
    mcp_tokens: Optional[Dict[str, str | Dict[str, Any]]] = Field(
        default_factory=dict,
        description="MCP OAuth tokens by server URL (string for legacy, dict with access_token/session_id for new format)"
    )
    ignored_mcp_servers: Optional[List[str]] = Field(
        default_factory=list,
        description="List of MCP server URLs to ignore (user chose to continue without auth)"
    )
    should_continue: Optional[bool] = False
    conversation_id: Optional[str] = Field(
        default=None,
        description="Conversation UUID for planning toolkit scoping"
    )
    persona: Optional[str] = Field(
        default="generic",
        description=(
            "Default persona for chat. Accepted values: 'generic', 'qa', 'nerdy', "
            "'quirky', 'cynical', 'none', 'bare'. Use 'bare' to bypass all "
            "Elitea-injected identity/persona content and send only the user's own "
            "instructions (plus tool-required guidance) to the model."
        )
    )
    steps_limit: Optional[int] = Field(
        default=None,
        description="Maximum tool execution iterations per turn (default: 25)"
    )
    meta: Optional[dict] = {}
    context_settings: Optional[ContextStrategyModel] = Field(
        default=None,
        description="Context settings for the LLM"
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# class ApplicationPredict(BaseModel):
#     collection: str
#     chat_history: List[ChatHistoryMessage]
#
#     top_p: Optional[float]
#     top_k: Optional[int]
#
#     tools: Optional[List[ApplicationToolBase]]
