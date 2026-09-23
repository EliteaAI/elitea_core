from typing import List, Optional

from pydantic import BaseModel, Field


class ProjectChatParticipant(BaseModel):
    entity_name: str
    entity_id: int
    entity_version_id: Optional[int] = None
    name: Optional[str] = None
    project_id: Optional[int] = None
    agent_type: Optional[str] = None


class ProjectChatConfig(BaseModel):
    participants: List[ProjectChatParticipant] = Field(default_factory=list)
