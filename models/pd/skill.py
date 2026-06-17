import re
import uuid
from datetime import datetime
from typing import Annotated, Dict, List, Optional

from pydantic import (
    BaseModel,
    Field,
    AfterValidator,
    field_validator,
    model_validator,
    ConfigDict,
)

from .collection_base import TagBaseModel, AuthorBaseModel
from ...models.enums.all import SkillEntityTypes
from ...utils.authors import get_authors_data
from ...utils.constants import ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API

SKILL_NAME_RE = re.compile(r'^[a-z0-9]$|^[a-z0-9][a-z0-9-]*[a-z0-9]$')


def validate_skill_name(value: str) -> str:
    if len(value) > 64 or not SKILL_NAME_RE.match(value):
        raise ValueError('name must be <=64 chars, lowercase letters/digits/hyphens only')
    if 'claude' in value or 'anthropic' in value:
        raise ValueError('name cannot contain "claude" or "anthropic"')
    return value


SkillName = Annotated[str, AfterValidator(validate_skill_name)]

from .skill_version import (
    SkillVersionCreateModel,
    SkillVersionListModel,
    SkillVersionDetailModel,
    SkillVersionUpdateModel,
    SkillVersionExportModel,
    SkillVersionImportModel,
)


class SkillImportResultModel(BaseModel):
    """Outcome of importing a single skill via the ``import_skill`` primitive."""
    id: int
    versions: Dict[str, int] = Field(default_factory=dict)
    reused: bool = False


class InvokedSkillModel(BaseModel):
    """
    Per-turn carrier for a skill explicitly invoked via ``~skill-name`` in the
    current user message.
    """
    skill_id: int
    skill_version_id: int
    name: str = Field(..., min_length=1)
    version_name: str
    instructions: str = Field(..., min_length=1)


class SkillArgsForwardingModel(BaseModel):
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

        if versions := values.get('versions'):
            for version in versions:
                version['project_id'] = project_id
                version['user_id'] = user_id

        return values


class SkillCreateModel(SkillArgsForwardingModel):
    """Model for creating a new skill with initial version."""
    name: SkillName = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1, max_length=2304)
    owner_id: int
    versions: List[SkillVersionCreateModel]
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)

    @field_validator('versions', mode='before')
    @classmethod
    def check_single_version(cls, value: Optional[List[dict]], info):
        if not value or len(value) != 1:
            raise ValueError('Exactly 1 version must be provided when creating a skill')
        return value


class SkillListModel(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    owner_id: int
    created_at: datetime
    versions: List[SkillVersionListModel] = Field(default_factory=list, exclude=True)
    author_ids: set[int] = Field(default_factory=set, exclude=True)
    authors: List[AuthorBaseModel] = Field(default_factory=list)
    tags: List[TagBaseModel] = Field(default_factory=list)
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode='after')
    def parse_versions_data(self):
        tags = dict()
        for version in self.versions:
            for tag in version.tags:
                tags[tag.name] = tag
            self.author_ids.add(version.author_id)
        self.tags = list(tags.values())
        return self

    def set_authors(self, user_map: dict) -> None:
        self.authors = [
            AuthorBaseModel(**user_map[author_id])
            for author_id in self.author_ids
            if author_id in user_map
        ]

    @field_validator('description')
    @classmethod
    def truncate_long_description(cls, value: Optional[str]) -> Optional[str]:
        if value:
            return value[:ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API]
        return value


class MultipleSkillListModel(BaseModel):
    skills: List[SkillListModel]

    @model_validator(mode='after')
    def parse_authors_data(self):
        if not self.skills:
            return self

        all_authors = set()
        for skill in self.skills:
            all_authors.update(skill.author_ids)

        if not all_authors:
            return self

        users = get_authors_data(list(all_authors))
        user_map = {i['id']: i for i in users}

        for skill in self.skills:
            skill.set_authors(user_map)

        return self


class SkillDetailModel(BaseModel):
    id: int
    name: str
    description: str
    owner_id: int
    created_at: datetime
    versions: List[SkillVersionListModel]
    version_details: Optional[SkillVersionDetailModel] = None
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=True)


class SkillUpdateModel(SkillArgsForwardingModel):
    name: Optional[SkillName] = Field(None, min_length=1, max_length=64)
    description: Optional[str] = Field(None, min_length=1, max_length=2304)
    version: Optional[SkillVersionUpdateModel] = None
    meta: Optional[dict] = None


class SkillExportModel(SkillArgsForwardingModel):
    id: int
    import_uuid: Optional[str] = None
    name: str
    description: str
    owner_id: int = Field(..., exclude=True)
    versions: List[SkillVersionExportModel]
    created_at: datetime
    meta: Optional[dict] = None

    model_config = ConfigDict(from_attributes=False)

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        hash_ = hash((self.__class__.__name__, self.id, self.owner_id, self.name))
        self.import_uuid = str(uuid.UUID(int=abs(hash_)))
        return self


class SkillImportModel(SkillArgsForwardingModel):
    name: SkillName = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1, max_length=2304)
    versions: List[SkillVersionImportModel]
    meta: Optional[dict] = None


class SkillUpdateRelationModel(BaseModel):
    """Toggle the relation between a skill and an agent (application) version.

    Mirrors ``ToolUpdateRelationModel`` (Link Agent to Toolkit), except there is
    no ``entity_id``: the ``entity_skill_mapping`` table is keyed by
    ``entity_version_id`` alone. ``skill_version_id`` is required when attaching.
    """
    entity_version_id: int
    entity_type: SkillEntityTypes = SkillEntityTypes.agent
    has_relation: bool = False
    skill_version_id: Optional[int] = None

    @model_validator(mode='after')
    def check_skill_version_id(self):
        if self.has_relation and self.skill_version_id is None:
            raise ValueError('skill_version_id is required when has_relation is True')
        return self
