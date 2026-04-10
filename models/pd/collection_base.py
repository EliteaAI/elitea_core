from typing import Optional, Annotated

from pydantic import (
    BaseModel, AnyUrl, ConfigDict, PlainSerializer
)

# Serializable URL string
UrlStr = Annotated[AnyUrl, PlainSerializer(lambda x: str(x), return_type=str)]


class TagBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    name: str
    data: Optional[dict] = None


class PromptTagUpdateModel(TagBaseModel):
    id: Optional[int] = None


class AuthorBaseModel(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    avatar: Optional[UrlStr] = None


class AuthorDetailModel(AuthorBaseModel):
    id: Optional[int] = None
    title: Optional[str] = None
    description: Optional[str] = None
    total_conversations: int = 0
    public_conversations: int = 0
    public_applications: int = 0
    total_applications: int = 0
    public_pipelines: int = 0
    total_pipelines: int = 0
    total_toolkits: int = 0
    public_collections: int = 0
    total_collections: int = 0
    rewards: int = 0
