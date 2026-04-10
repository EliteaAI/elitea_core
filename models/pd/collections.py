from datetime import datetime
from queue import Empty
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

from pylon.core.tools import log
from tools import rpc_tools

from ...models.pd.collection_base import AuthorBaseModel
from ...models.pd.entity import EntityListModel
from ...models.pd.tag import TagDetailModel
from ...utils.constants import ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API
from ..enums.all import CollectionPatchOperations


class CollectionItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    owner_id: int


class CollectionPrivateTwinModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int = Field(..., alias='shared_id')
    owner_id: int = Field(..., alias='shared_owner_id')


class CollectionPatchModel(BaseModel):
    project_id: int
    collection_id: int
    operation: CollectionPatchOperations
    prompt: Optional[CollectionItem] = None
    datasource: Optional[CollectionItem] = None
    application: Optional[CollectionItem] = None

    @model_validator(mode='before')
    @classmethod
    def check_only_one_entity(cls, values):
        fields = ("prompt", "datasource", "application",)
        if [bool(values.get(f)) for f in fields].count(True) != 1:
            raise ValueError(f'One non-empty of the fields is expected: {fields}')

        return values


class CollectionModel(BaseModel):
    name: str
    owner_id: int
    author_id: Optional[int] = None
    description: Optional[str] = None
    prompts: Optional[List[CollectionItem]] = []
    datasources: Optional[List[CollectionItem]] = []
    applications: Optional[List[CollectionItem]] = []
    shared_id: Optional[int] = None
    shared_owner_id: Optional[int] = None


# class PromptBaseModel(BaseModel):
#     id: int
#     name: str
#     description: Optional[str]
#     owner_id: int
#
#     class Config:
#         orm_mode = True


class CollectionShortDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    status: str


class CollectionDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    status: str
    author_id: int
    prompts: Optional[List[EntityListModel]] = []
    datasources: Optional[List[EntityListModel]] = []
    applications: Optional[List[EntityListModel]] = []
    author: Optional[AuthorBaseModel] = None
    created_at: datetime


class CollectionUpdateModel(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: str


class CollectionListModel(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
    )
    
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    author_id: int
    status: str
    author: Optional[AuthorBaseModel] = None
    prompts: Optional[List] = Field(default=[], exclude=True)
    datasources: Optional[List] = Field(default=[], exclude=True)
    applications: Optional[List] = Field(default=[], exclude=True)
    tags: List[TagDetailModel] = []
    created_at: datetime
    includes_prompt: Optional[bool] = None
    includes_datasource: Optional[bool] = None
    includes_application: Optional[bool] = None
    prompt_addability: Optional[bool] = None
    datasource_addability: Optional[bool] = None
    application_addability: Optional[bool] = None
    prompt_count: int = 0
    datasource_count: int = 0
    application_count: int = 0
    likes: Optional[int] = None
    trending_likes: Optional[int] = None
    is_liked: Optional[bool] = None

    @model_validator(mode='after')
    def count_entities(self):
        self.prompt_count = len(self.prompts)
        self.datasource_count = len(self.datasources)
        self.application_count = len(self.applications)
        return self

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

    @field_validator('description')
    @classmethod
    def truncate_long_description(cls, value: Optional[str]) -> Optional[str]:
        if value:
            return value[:ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API]
        return value


class PublishedCollectionDetailModel(CollectionDetailModel):
    likes: Optional[int] = 0
    is_liked: Optional[bool] = False

    def get_likes(self, project_id: int) -> None:
        try:
            likes_data = rpc_tools.RpcMixin().rpc.timeout(2).social_get_likes(
                project_id=project_id, entity='collection', entity_id=self.id
            )
            # self.likes = [LikeModel(**like) for like in likes_data['rows']]
            self.likes = likes_data['total']
        except Empty:
            self.likes = 0

    def check_is_liked(self, project_id: int) -> None:
        try:
            self.is_liked = rpc_tools.RpcMixin().rpc.timeout(2).social_is_liked(
                project_id=project_id, entity='collection', entity_id=self.id
            )
        except Empty:
            self.is_liked = False


class CollectionSearchModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str


class MultipleCollectionSearchModel(BaseModel):
    items: List[CollectionSearchModel]
