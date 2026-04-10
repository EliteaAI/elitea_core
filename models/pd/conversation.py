from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, constr, ConfigDict, computed_field

from .message import MessageGroupDetail
from .participant import ParticipantBase, ParticipantCreate
from ...utils.chat_constants import CONVERSATION_NAME_MAX_LENGTH


class ConversationBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: Optional[int] = None
    name: str
    is_private: bool
    author_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    meta: dict
    source: Optional[str] = 'elitea'
    attachment_participant_id: Optional[int] = None
    instructions: Optional[str] = None


class ConversationList(ConversationBase):
    id: int


class ConversationListExtended(ConversationList):
    participants_count: int
    message_groups_count: int
    users_count: int
    duration: float = 0.0


class ConversationDetailsOrm(ConversationBase):
    model_config = ConfigDict(from_attributes=True)

    participants: List[ParticipantBase]
    uuid: Optional[str | UUID] = None


class ConversationDetails(ConversationDetailsOrm):
    message_groups_count: int
    message_groups: List[MessageGroupDetail]

    class Config:
        orm_mode = True


class ConversationCreate(BaseModel):
    name: constr(min_length=3, max_length=CONVERSATION_NAME_MAX_LENGTH)
    is_private: bool = True
    participants: Optional[List[ParticipantCreate]] = Field(default_factory=list)
    author_id: int
    source: Optional[constr(to_lower=True, strip_whitespace=True)] = 'elitea'
    meta: Optional[dict] = Field(default_factory=dict)
    instructions: Optional[str] = ''


class ConversationUpdate(BaseModel):
    name: Optional[constr(min_length=3, max_length=CONVERSATION_NAME_MAX_LENGTH)] = None
    is_private: Optional[bool] = None
    folder_id: Optional[int] = None
    attachment_participant_id: Optional[int] = None
    instructions: Optional[str] = None
    is_hidden: Optional[bool] = None
    meta: Optional[dict] = None
