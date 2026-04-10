import mimetypes
import uuid
from pathlib import Path
from typing import Optional, Dict, Any, List
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo, ConfigDict
from tools import db, VaultClient

from ...models.participants import Participant
from ...utils.toolkits_utils import format_tool_call_as_user_input
from .attachment import AttachmentMessageItemPredict
from .participant_settings import EntitySettingsLlm


class PredictPayload(BaseModel):
    stream_id: str


class PromptPredictPayload(PredictPayload):
    model_config = ConfigDict(populate_by_name=True)

    project_id: int
    prompt_id: Optional[int] = None
    prompt_version_id: Optional[int] = None
    user_input: Optional[str] = Field(default=None, alias='input')


class ToolkitToolCallPayload(BaseModel):
    """Payload for calling a toolkit tool."""
    tool_name: str = Field(..., description="Name of the tool to call in the toolkit")
    tool_params: Dict[str, Any] = Field(default_factory=dict, description="Parameters to pass to the tool")


class SioPredictModel(BaseModel):
    project_id: int
    participant_id: Optional[int] = None
    user_ids: Optional[list] = []
    conversation_uuid: UUID | str
    user_input: Optional[str] = None
    tool_call_input: Optional[ToolkitToolCallPayload] = None
    question_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    interaction_uuid: str | uuid.UUID | None = None
    attachments_info: Optional[List[AttachmentMessageItemPredict]] = None
    llm_settings: Optional[EntitySettingsLlm] = {}
    mcp_tokens: Optional[Dict[str, str | Dict[str, Any]]] = Field(default_factory=dict, description="MCP OAuth tokens by server URL (string for legacy, dict with access_token/session_id for new format)")
    ignored_mcp_servers: Optional[List[str]] = Field(default_factory=list, description="List of MCP server URLs to ignore (user chose to continue without auth)")
    # Keep for backwards compatibility - not used in normal flow, always False
    should_continue: Optional[bool] = Field(default=False, description="Deprecated: Use chat_continue_predict event instead")
    thread_id: Optional[str] = Field(default=None, description="Thread ID for execution")
    persona: Optional[str] = Field(default="generic", description="Default persona for chat: 'generic' or 'qa'")

    @model_validator(mode='after')
    def user_input_from_tool_call_input(self):
        """Generate user_input string from tool_call_input if present."""
        if self.tool_call_input:
            # Always override user_input when tool_call_input is provided
            self.user_input = format_tool_call_as_user_input(
                self.tool_call_input.tool_name,
                self.tool_call_input.tool_params
            )
        
        return self

    @model_validator(mode='after')
    def validate_user_input_or_tool_call(self):
        """Ensure at least one of user_input or tool_call_input is provided."""
        if not self.user_input and not self.tool_call_input:
            raise ValueError('At least one of user_input or tool_call_input must be provided')
        
        return self

    @model_validator(mode='after')
    def validate_attachments(self):
        """Enforce total and image attachment count limits from vault config."""
        if not self.attachments_info:
            return self

        vault_client = VaultClient(self.project_id)
        secrets = vault_client.get_all_secrets()
        chat_max_upload_count = int(secrets.get('chat_max_upload_count', 10))
        chat_max_image_upload_count = int(secrets.get('chat_max_image_upload_count', 10))

        total_count = len(self.attachments_info)
        if total_count > chat_max_upload_count:
            raise ValueError(
                f"Number of attachments ({total_count}) exceeds the limit of {chat_max_upload_count}"
            )

        image_count = sum(
            1 for a in self.attachments_info
            if (mimetypes.guess_type(Path(a.filepath).name)[0] or '').startswith('image')
            and not a.filepath.lower().endswith('.svg')
        )

        if image_count > chat_max_image_upload_count:
            raise ValueError(
                f"Number of image attachments ({image_count}) exceeds the limit of {chat_max_image_upload_count}"
            )

        return self

    @field_validator('participant_id')
    def check_participant(cls, value, info: ValidationInfo):
        if value:
            project_id: Optional[int] = info.data.get('project_id')
            if project_id is not None:
                with db.get_session(project_id) as session:
                    participant = session.query(Participant).where(
                        Participant.id == value
                    ).first()
                    assert participant, f'The participant with id {value} does not exist'
                    session.expunge(participant)
                    info.data['participant'] = participant
        return value

    # todo: remove this when we move to correct payload from ui
    @model_validator(mode='before')
    def set_from_payload(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get('user_input') is None:
            old_payload = values.get('payload', {})
            values['user_input'] = old_payload.get('user_input', old_payload.get('input'))
        return values


class SioContinuePredictModel(BaseModel):
    """
    Simplified payload model for continuing execution after MCP auth or other interruptions.
    Only contains fields actually needed for the "Continue" flow.
    """
    project_id: int
    conversation_uuid: UUID | str
    message_id: str = Field(..., description="Message ID of the response message to continue")
    thread_id: Optional[str] = Field(default=None, description="Thread ID for continuing execution (optional, falls back to message meta)")
    mcp_tokens: Optional[Dict[str, str | Dict[str, Any]]] = Field(default_factory=dict, description="MCP OAuth tokens by server URL")
    ignored_mcp_servers: Optional[List[str]] = Field(default_factory=list, description="List of MCP server URLs to ignore")
    # Always True for Continue flow - used by generate_payload to signal resume from checkpoint
    should_continue: bool = Field(default=True, description="Always True for Continue flow")
    # User input for continue flow - if provided, uses this instead of 'continue'
    user_input: Optional[str] = Field(default=None, description="User input to use instead of 'continue'")
    hitl_resume: bool = Field(default=False, description="Whether this continue request resumes a HITL interrupt")
    hitl_action: Optional[str] = Field(default=None, description="HITL action: approve, reject, or edit")
    hitl_value: Optional[str] = Field(default=None, description="Edited text for HITL edit resumes")
    # Fields needed for compatibility with generate_payload
    interaction_uuid: str | uuid.UUID | None = None
    llm_settings: Optional[EntitySettingsLlm] = None
    attachments_info: Optional[List[AttachmentMessageItemPredict]] = None
    question_id: Optional[str] = None


class SioRegenerateModel(BaseModel):
    payload: dict
    sid: str
    question_id: str
    conversation_uuid: Optional[str]


class SioPredictContinueModel(BaseModel):
    payload: dict
    sid: str
