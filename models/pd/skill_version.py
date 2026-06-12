from datetime import datetime
from typing import List, Optional

from pydantic import (
    BaseModel,
    Field,
    model_validator,
    ConfigDict,
    ValidationInfo,
)

from .collection_base import TagBaseModel, AuthorBaseModel, PromptTagUpdateModel
from .tag import TagDetailModel
from ...utils.authors import get_authors_data


class SkillVersionCreateModel(BaseModel):
    name: str = Field(default='base', min_length=1)
    instructions: str = Field(min_length=1)
    tags: Optional[List[TagBaseModel]] = None
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class SkillVersionListModel(BaseModel):
    id: int
    name: str
    created_at: datetime
    author_id: int = Field(..., exclude=True)
    tags: List[TagBaseModel] = Field(default_factory=list, exclude=True)

    model_config = ConfigDict(from_attributes=True)


class SkillVersionDetailModel(BaseModel):
    id: int
    name: str
    instructions: str
    author_id: int
    author: Optional[AuthorBaseModel] = None
    tags: List[TagDetailModel] = Field(default_factory=list)
    created_at: datetime
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode='after')
    def add_author_data(self, info: ValidationInfo):
        if self.author is None and self.author_id:
            authors_map = info.context.get('authors_map') if info.context else None
            if authors_map and self.author_id in authors_map:
                self.author = AuthorBaseModel(**authors_map[self.author_id])
            else:
                authors_data: list = get_authors_data(author_ids=[self.author_id])
                if authors_data:
                    self.author = AuthorBaseModel(**authors_data[0])
        return self


class SkillVersionUpdateModel(BaseModel):
    name: Optional[str] = Field(None, min_length=1)
    instructions: Optional[str] = Field(None, min_length=1)
    tags: Optional[List[PromptTagUpdateModel]] = None
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class SkillVersionExportModel(BaseModel):
    name: str
    instructions: str
    tags: List[TagBaseModel] = Field(default_factory=list)
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class SkillVersionImportModel(BaseModel):
    name: str = Field(default='base', min_length=1)
    instructions: str = Field(min_length=1)
    tags: Optional[List[TagBaseModel]] = None
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)
