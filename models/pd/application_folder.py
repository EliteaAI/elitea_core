from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, List
from datetime import datetime
from uuid import UUID


class ApplicationFolderBase(BaseModel):
    name: str = Field(..., max_length=128)
    agent_type: str = Field(..., description="'openai' for agents, 'pipeline' for pipelines")
    meta: Optional[Dict] = Field(default_factory=dict)


class ApplicationFolderCreate(ApplicationFolderBase):
    """Request model for creating a folder. owner_id is set automatically by the API from authenticated user."""
    owner_id: Optional[int] = Field(None, json_schema_extra={"hidden": True})


class ApplicationFolderUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=128)
    meta: Optional[Dict] = Field(default_factory=dict)


class ApplicationFolderDetails(ApplicationFolderBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: UUID
    owner_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None


class ApplicationFolderList(ApplicationFolderDetails):
    """Folder with application count for list views."""
    applications_count: Optional[int] = 0


class ApplicationFolderWithApplications(ApplicationFolderDetails):
    """Folder with nested applications for grouped views."""
    applications: List[dict] = Field(default_factory=list)
    total: int = 0


class MoveApplicationToFolderRequest(BaseModel):
    folder_id: Optional[int] = Field(None, description="Target folder ID. Set to null to remove from folder.")
