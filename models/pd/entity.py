from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, model_validator, field_validator, Field, ConfigDict

from .collection_base import TagBaseModel, AuthorBaseModel
from .tag import TagListModel
from ..enums.all import PublishStatus
from ...utils.utils import determine_entity_status


class EntityVersionListModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    status: PublishStatus
    created_at: datetime  # probably delete this
    author_id: int = Field(exclude=True)
    tags: List[TagListModel] = Field(default=[], exclude=True)


class EntityListModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    created_at: datetime
    versions: List[EntityVersionListModel] = Field(default=[], exclude=True)
    author_ids: set[int] = Field(default_factory=set, exclude=True)
    authors: List[AuthorBaseModel] = []
    tags: Optional[TagBaseModel] = None
    status: Optional[PublishStatus] = None

    @model_validator(mode='after')
    def parse_versions_data(self):
        tags = dict()
        version_statuses = set()

        for version in self.versions:
            for tag in version.tags:
                tags[tag.name] = tag
            self.author_ids.add(version.author_id)
            version_statuses.add(version.status)

        self.tags = list(tags.values())
        self.status = determine_entity_status(version_statuses)
        return self

    def set_authors(self, user_map: dict) -> None:
        self.authors = [
            AuthorBaseModel(**user_map[author_id]) for author_id in self.author_ids
        ]


class PublishedEntityListModel(EntityListModel):
    likes: Optional[int] = None
    is_liked: Optional[bool] = None
    trending_likes: Optional[int] = None

    @field_validator('is_liked')
    @classmethod
    def is_liked_field(cls, v):
        if v is None:
            return False
        return v

    @field_validator('likes')
    @classmethod
    def likes_field(cls, v):
        if v is None:
            return 0
        return v

