from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class _ImportBase(BaseModel):
    model_config = ConfigDict(extra='ignore')


class ImportParticipantRef(_ImportBase):
    participant_id: Optional[int] = None
    entity_name: Optional[str] = None
    name: Optional[str] = None


class ImportTextItem(_ImportBase):
    type: Literal['text_message']
    content: Optional[str] = ''


class ImportCanvasItem(_ImportBase):
    type: Literal['canvas_message']
    name: Optional[str] = None
    canvas_type: Optional[str] = None
    code_language: Optional[str] = None
    content: Optional[str] = ''


class ImportAttachmentItem(_ImportBase):
    type: Literal['attachment_message', 'generated_file']
    name: str = Field(..., min_length=1, max_length=1024)
    attachment_type: Optional[str] = None
    file_size: Optional[int] = None
    export_path: Optional[str] = None
    export_status: Optional[str] = None


ImportItem = Annotated[
    Union[ImportTextItem, ImportCanvasItem, ImportAttachmentItem],
    Field(discriminator='type'),
]


class ImportMessage(_ImportBase):
    uuid: Optional[str] = None
    created_at: Optional[str] = None
    author: Optional[ImportParticipantRef] = None
    sent_to: Optional[ImportParticipantRef] = None
    reply_to_uuid: Optional[str] = None
    items: List[ImportItem] = []


class ImportParticipant(_ImportBase):
    id: Optional[int] = None
    entity_name: str
    name: Optional[str] = None
    version: Optional[str] = None


class ImportConversationInfo(_ImportBase):
    uuid: Optional[str] = None
    name: Optional[str] = None
    instructions: Optional[str] = None
    participants: List[ImportParticipant] = []


class ImportExportInfo(_ImportBase):
    format_version: str
    exported_at: Optional[str] = None
    include_attachments: bool = False
    project_id: Optional[int] = None


class ChatExportEnvelope(_ImportBase):
    """Top level of an export file; messages are validated one by one to report the broken index."""
    export_info: ImportExportInfo
    conversation: ImportConversationInfo
    messages: List[dict]


class ImportCommitPayload(_ImportBase):
    selected_attachments: List[str] = []
