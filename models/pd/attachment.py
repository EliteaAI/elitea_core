from typing import Optional, Union, List, Literal
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AttachmentBase(BaseModel):
    id: Optional[int] = None
    sent_to_id: Optional[int]
    reply_to_id: Optional[int]
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True


class AttachmentMessageItemPredict(BaseModel):
    """Attachment reference for predict payload."""
    model_config = ConfigDict(from_attributes=True)

    filepath: str = Field(..., description="File path in format /{bucket}/{filename}")


class AttachmentMessageItemCreated(BaseModel):
    """Attachment upload response."""
    model_config = ConfigDict(from_attributes=True)

    filepath: str = Field(..., description="File path in format /{bucket}/{name}")
    file_size: int = Field(ge=0)


class AttachmentMessageItemBase(BaseModel):
    """Base attachment model with content"""
    model_config = ConfigDict(from_attributes=True)

    filepath: str = Field(..., description="File path in format /{bucket}/{name}")
    attachment_type: str
    content: Optional[Union[dict, List[dict]]] = None


class AttachmentMessageItemDetail(AttachmentMessageItemBase):
    """Full attachment detail for API responses."""
    id: int
    item_type: Literal['attachment_message']


class AttachmentManagerCreatePayload(BaseModel):
    toolkit_id: Optional[int] = Field(...)


class ChunkUploadPayload(BaseModel):
    """Payload for chunked file upload"""
    file_id: str = Field(..., min_length=1, description="Unique identifier for the file upload session")
    chunk_index: int = Field(..., ge=0, description="Index of the current chunk (0-based)")
    total_chunks: int = Field(..., gt=0, description="Total number of chunks for this file")
    file_name: str = Field(..., min_length=1, description="Original filename")
