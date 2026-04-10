from typing import Optional, List, Set
from ..enums.all import AgentTypes
from queue import Empty
from datetime import datetime
import uuid
import yaml
from pylon.core.tools import log
from tools import rpc_tools, SecretString
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict, ValidationError, ValidationInfo,
)

from ...utils.authors import get_authors_data
from ...models.enums.all import PublishStatus
from ...models.pd.collection_base import AuthorBaseModel, TagBaseModel
from ...utils.constants import ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API

from .version import (
    ApplicationVersionBaseModel,
    ApplicationVersionBaseCreateModel,
    ApplicationVersionDetailModel,
    ApplicationVersionListModel,
    ApplicationVersionFullUpdateModel,
    ApplicationExportVersionDetailModel,
    ApplicationVersionForkCreateModel,
)


class ApplicationArgsForwardingModel(BaseModel):
    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)

    @model_validator(mode='before')
    @classmethod
    def args_forwarding(cls, values):
        project_id = values.get('project_id')
        user_id = values.get('user_id')

        if version := values.get('version'):
            version['project_id'] = project_id
            version['user_id'] = user_id

        if version_details := values.get('version_details'):
            version_details['project_id'] = project_id
            version_details['user_id'] = user_id

        if versions := values.get('versions'):
            for version in versions:
                version['project_id'] = project_id
                version['user_id'] = user_id

        return values


class ApplicationBaseModel(BaseModel):
    name: str = Field(min_length=1)
    description: Optional[str] = None
    owner_id: int
    shared_id: Optional[int] = None
    shared_owner_id: Optional[int] = None
    versions: Optional[List[ApplicationVersionBaseModel]] = None
    webhook_secret: Optional[SecretString] = None
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class ApplicationDetailModel(ApplicationBaseModel):
    id: int
    versions: List[ApplicationVersionListModel]
    version_details: Optional[ApplicationVersionDetailModel] = None
    created_at: datetime
    collections: Optional[list] = None
    is_pinned: bool = False

    def check_is_pinned(self, project_id: int):
        try:
            self.is_pinned = rpc_tools.RpcMixin().rpc.timeout(2).social_is_pinned(
                project_id=project_id, entity='application', entity_id=self.id
            )
        except Empty:
            self.is_pinned = False
        return self


class ApplicationExportModel(ApplicationBaseModel, ApplicationArgsForwardingModel):
    id: int
    import_uuid: str = None
    versions: List[ApplicationExportVersionDetailModel]
    created_at: datetime
    owner_id: int = Field(..., exclude=True)
    shared_id: Optional[int] = Field(None, exclude=True)
    shared_owner_id: Optional[int] = Field(None, exclude=True)
    webhook_secret: Optional[SecretString] = Field(None, exclude=True)

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        hash_ = hash((self.__class__.__name__, self.id, self.owner_id, self.name))
        self.import_uuid = str(uuid.UUID(int=abs(hash_)))
        return self

    model_config = ConfigDict(from_attributes=False)


class ApplicationDetailLikesModel(ApplicationDetailModel):
    likes: int = 0
    is_liked: bool = False

    def get_likes(self, project_id: int) -> None:
        try:
            likes_data = rpc_tools.RpcMixin().rpc.timeout(2).social_get_likes(
                project_id=project_id, entity='application', entity_id=self.id
            )
            self.likes = likes_data['total']
        except Empty:
            self.likes = 0

    def check_is_liked(self, project_id: int) -> None:
        try:
            self.is_liked = rpc_tools.RpcMixin().rpc.timeout(2).social_is_liked(
                project_id=project_id, entity='application', entity_id=self.id
            )
        except Empty:
            self.is_liked = False


def determine_application_status(version_statuses: Set[PublishStatus]) -> PublishStatus:
    status_priority = (
        PublishStatus.rejected,
        PublishStatus.on_moderation,
        PublishStatus.published,
        PublishStatus.draft,
    )

    for status in status_priority:
        if status in version_statuses:
            return status


class ApplicationListModel(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    created_at: datetime
    versions: List[ApplicationVersionListModel] = Field(..., exclude=True)
    author_ids: set[int] = Field(default_factory=set, exclude=True)
    authors: List[AuthorBaseModel] = Field(default_factory=list)
    tags: Optional[TagBaseModel] = None
    status: Optional[PublishStatus] = None
    is_forked: bool = False
    icon_meta: Optional[dict] = {}
    meta: Optional[dict] = {}
    agent_type: Optional[AgentTypes] = None
    has_interrupt: bool = False
    has_swarm: bool = False
    is_pinned: bool = False

    model_config = ConfigDict(from_attributes=True)

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
        self.status = determine_application_status(version_statuses)
        return self

    def set_authors(self, user_map: dict) -> None:
        self.authors = [
            AuthorBaseModel(**user_map[author_id]) for author_id in self.author_ids if author_id in user_map
        ]

    @field_validator('description')
    @classmethod
    def truncate_long_description(cls, value: Optional[str]) -> Optional[str]:
        if value:
            return value[:ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API]
        return value

    @model_validator(mode='after')
    def set_is_forked(self) -> 'ApplicationListModel':
        for v in self.versions:
            meta = v.meta or {}
            if 'parent_entity_id' in meta and 'parent_project_id' in meta:
                self.is_forked = True
                break
        return self

    @model_validator(mode='after')
    def set_meta(self) -> 'ApplicationListModel':
        for v in self.versions:
            meta = v.meta or {}
            if 'parent_entity_id' in meta and 'parent_project_id' in meta:
                self.meta = meta
                return self
        return self

    @model_validator(mode='after')
    def check_has_interrupt(self) -> 'ApplicationListModel':
        """
        Check if the agent has interrupts or subgraphs in its instructions.
        Returns True if interrupts or subgraphs are found, False otherwise.
        """
        versions = self.versions
        if not versions:
            return self

        # Check the latest version first, then fallback to any version
        latest_version = None
        for version in versions:
            if version.name == 'latest':
                latest_version = version
                break

        selected_version = latest_version or versions[-1]

        instructions = selected_version.instructions
        if not instructions:
            return self

        try:
            pipeline_yaml_object = yaml.safe_load(instructions)

            if not isinstance(pipeline_yaml_object, dict):
                return self

            # Check for interrupt keys
            if [yaml_node for key, yaml_node in pipeline_yaml_object.items() if 'interrupt_' in key and yaml_node]:
                self.has_interrupt = True
                return self

            # Check for subgraphs or pipelines in nodes
            nodes = pipeline_yaml_object.get('nodes', [])
            for yaml_node in nodes:
                if yaml_node.get('type') is None:
                    raise ValidationError(f'In pipeline with ID={self.id}, missing type in node: {yaml_node}')
                if yaml_node['type'] == 'pipeline' or yaml_node['type'] == 'subgraph':
                    self.has_interrupt = True
                    return self

        except (yaml.YAMLError, AttributeError, TypeError):
            # log.debug(f"Error parsing instructions for interrupt check: {e}")
            pass

        return self

    @model_validator(mode='after')
    def check_has_swarm(self) -> 'ApplicationListModel':
        """Check if any version has swarm mode enabled in internal_tools."""
        for v in self.versions:
            meta = v.meta or {}
            internal_tools = meta.get('internal_tools', [])
            if 'swarm' in internal_tools:
                self.has_swarm = True
                break
        return self

    @model_validator(mode='after')
    def set_icon_meta(self) -> 'ApplicationListModel':
        versions = self.versions
        if not versions:
            return self

        latest_version, oldest_version = None, None

        for version in versions:
            if version.name == 'latest':
                latest_version = version
                break

        if not latest_version:
            oldest_version = min(versions, key=lambda version: version.created_at)

        selected_version = latest_version or oldest_version

        meta = selected_version.meta or {}

        if 'icon_meta' in meta:
            self.icon_meta = meta['icon_meta']

        return self

    @model_validator(mode='after')
    def set_agent_type(self) -> 'ApplicationListModel':
        # we expect that all versions have the same agent type
        versions = self.versions
        if versions:
            self.agent_type = versions[0].agent_type
        return self


class MultipleApplicationListModel(BaseModel):
    applications: List[ApplicationListModel]

    @model_validator(mode='after')
    def parse_authors_data(self):
        all_authors = set()
        for i in self.applications:
            all_authors.update(i.author_ids)

        users = get_authors_data(list(all_authors))

        user_map = {i['id']: i for i in users}

        for i in self.applications:
            i.set_authors(user_map)

        return self


class ApplicationUpdateModel(ApplicationArgsForwardingModel):
    name: Optional[str] = Field(None, min_length=1, max_length=32)
    description: Optional[str] = None
    version: Optional[ApplicationVersionFullUpdateModel] = None
    webhook_secret: Optional[SecretString] = None


class ApplicationCreateModel(ApplicationBaseModel, ApplicationArgsForwardingModel):
    name: Optional[str] = Field(None, min_length=1, max_length=32)
    versions: List[ApplicationVersionBaseCreateModel]

    @field_validator('versions', mode='before')
    @classmethod
    def check_only_latest_version(cls, value: Optional[List[dict]], info):
        assert len(value) == 1, 'Only 1 version can be created'
        return value


class ApplicationImportModel(ApplicationBaseModel, ApplicationArgsForwardingModel):
    versions: Optional[List[ApplicationVersionForkCreateModel]] = None


class PublishedApplicationVersionListModel(ApplicationVersionListModel):
    author: Optional[AuthorBaseModel] = None

    @model_validator(mode='after')
    def add_author_data(self, info: ValidationInfo) -> 'PublishedApplicationVersionListModel':
        if self.author is None and self.author_id:
            # Check for pre-fetched authors in validation context (batch optimization)
            authors_map = info.context.get('authors_map') if info.context else None
            if authors_map and self.author_id in authors_map:
                self.author = AuthorBaseModel(**authors_map[self.author_id])
            else:
                # Fallback to individual fetch for single-item cases
                authors_data: list = get_authors_data(author_ids=[self.author_id])
                if authors_data:
                    self.author = AuthorBaseModel(**authors_data[0])
        return self


class PublishedApplicationDetailModel(ApplicationDetailLikesModel):
    versions: List[PublishedApplicationVersionListModel]

    @field_validator('versions')
    @classmethod
    def check_versions(cls, value: list) -> list:
        return [version for version in value if version.status == PublishStatus.published]


class PublishedApplicationListModel(ApplicationListModel):
    likes: Optional[int] = None
    is_liked: Optional[bool] = None
    trending_likes: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)

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


class MultiplePublishedApplicationListModel(MultipleApplicationListModel):
    applications: List[PublishedApplicationListModel]


class ApplicationRelationModel(BaseModel):
    application_id: int
    version_id: int
    has_relation: bool
