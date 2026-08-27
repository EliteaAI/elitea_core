from typing import Optional, Annotated

from pydantic import (
    BaseModel, AnyUrl, ConfigDict, PlainSerializer, model_validator, ValidationInfo
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


class VersionAuthorMixin(BaseModel):
    """Mixin that populates author_name / author_email on version list models.

    Requires the subclass to declare:
        author_id: int = Field(..., exclude=True)
        author_name: Optional[str] = None
        author_email: Optional[str] = None

    Resolution order:
      1. Use pre-fetched authors_map from Pydantic validation context (batch path).
      2. Fall back to a single get_authors_data() RPC call (legacy / missing context).
    """

    @model_validator(mode='after')
    def resolve_author(self, info: ValidationInfo) -> 'VersionAuthorMixin':
        from ...utils.authors import get_authors_data  # local import to avoid circular

        authors_map = (info.context or {}).get('authors_map', {})
        if self.author_id and self.author_id in authors_map:
            entry = authors_map[self.author_id]
            self.author_name = entry.get('name')
            self.author_email = entry.get('email')
        elif self.author_id and not authors_map:
            authors_data = get_authors_data(author_ids=[self.author_id])
            if authors_data:
                self.author_name = authors_data[0].get('name')
                self.author_email = authors_data[0].get('email')
        return self


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
    public_skills: int = 0
    total_skills: int = 0
    rewards: int = 0
