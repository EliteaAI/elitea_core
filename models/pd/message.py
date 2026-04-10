from uuid import UUID
from typing import Optional, List, Union, Literal
from datetime import datetime

from pydantic import BaseModel, ConfigDict, model_validator, Field, conint

from .canvas import CanvasItemDetail
from .participant import ParticipantDetails
from .text import TextMessageItemDetail
from .attachment import AttachmentMessageItemDetail, AttachmentMessageItemPredict
from .predict import ToolkitToolCallPayload
from ...utils.toolkits_utils import format_tool_call_as_user_input


class MessageGroupBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    sent_to_id: Optional[int] = None
    reply_to_id: Optional[int] = None
    updated_at: Optional[datetime] = None


class MessageItemBase(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='allow')

    id: int
    uuid: UUID | str
    meta: dict
    order_index: int
    item_type: Literal['text_message', 'canvas_message', 'attachment_message']
    item_details: Union[
        TextMessageItemDetail,
        CanvasItemDetail,
        AttachmentMessageItemDetail
    ] = Field(discriminator='item_type', default=None)

    @model_validator(mode='before')
    @classmethod
    def set_item_details(cls, v):
        if isinstance(v, dict):
            v['item_details'] = v
        else:
            v.item_details = v
        return v


class MessageGroupDetail(MessageGroupBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: UUID | str
    author_participant_id: int
    meta: dict
    message_items: List[MessageItemBase]
    created_at: datetime
    sent_to: Optional[ParticipantDetails]
    is_streaming: bool
    task_id: Optional[str]


class MessagePostPayload(BaseModel):
    participant_id: Optional[int] = None
    conversation_uuid: UUID
    user_input: Optional[str] = None
    tool_call_input: Optional[ToolkitToolCallPayload] = None
    await_task_timeout: conint(ge=-1, le=300) = 30  # type: ignore
    attachments_info: Optional[List[AttachmentMessageItemPredict]] = None
    llm_settings: Optional[dict] = None
    return_task_id: bool = False

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

    @model_validator(mode='before')
    @classmethod
    def check_llm_settings_for_llm_predict(cls, v):
        if isinstance(v, dict):
            if v.get('participant_id') is None:  # LLM predict
                llm_settings = v.get('llm_settings')
                if not llm_settings:
                    raise ValueError('llm_settings must be provided for LLM prediction')
        return v
