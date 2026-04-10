from datetime import datetime
from typing import List, Optional, Literal
import uuid

from .tool import (
    ToolUpdateModel,
    ToolDetails,
    ToolValidatedDetails,
    ToolBase,
    ToolCreateModel,
    ToolApplicationExportDetails
)
from ..enums.all import AgentTypes
from ...models.enums.all import PublishStatus
from ...models.pd.llm import LLMSettingsModel
from ...models.pd.collection_base import TagBaseModel, AuthorBaseModel, PromptTagUpdateModel
from ...models.pd.tag import TagDetailModel
from ...utils.pipeline_utils import validate_yaml_from_str

# todo: switch to rpc call
from ...utils.authors import get_authors_data

from pydantic import (
    BaseModel, field_validator, Field, model_validator, ConfigDict, ValidationInfo
)


DEFAULT_STEP_LIMIT = 25


def agent_root_pipeline_validator(values: dict):
    if values.get('agent_type') == AgentTypes.pipeline.value:
        yaml_data = values.get('instructions')
        if not yaml_data:
            return values
            # raise ValueError("YAML data is missing")
        validate_yaml_from_str(yaml_data)
    return values


class TagListModel(TagBaseModel):
    id: int


class ApplicationVariableModel(BaseModel):
    name: str
    value: str

    model_config = ConfigDict(from_attributes=True)


class ApplicationVariableDetailedModel(ApplicationVariableModel):
    id: Optional[int] = None


class ApplicationVersionArgsForwardingModel(BaseModel):
    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)

    @model_validator(mode='before')
    @classmethod
    def args_forwarding(cls, values):
        for tool in values.get('tools', []):
            tool['project_id'] = values.get('project_id')
            tool['user_id'] = values.get('user_id')
        return values


class ApplicationVersionBaseModel(BaseModel):
    name: str = Field(min_length=1)
    author_id: int
    tags: Optional[List[TagBaseModel]] = None
    instructions: Optional[str] = None
    application_id: Optional[int] = None
    shared_id: Optional[int] = None
    shared_owner_id: Optional[int] = None
    llm_settings: Optional[LLMSettingsModel] = None
    variables: Optional[List[ApplicationVariableModel]] = None
    tools: Optional[List[ToolBase]] = None
    conversation_starters: Optional[List] = None
    agent_type: AgentTypes = AgentTypes.openai.value
    welcome_message: Optional[str] = None
    pipeline_settings: Optional[dict] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


class ApplicationVersionCreateModel(ApplicationVersionBaseModel, ApplicationVersionArgsForwardingModel):
    tools: Optional[List[ToolCreateModel]] = None
    meta: Optional[dict] = {}

    @model_validator(mode='before')
    @classmethod
    def validate_diagram_yaml(cls, values: dict):
        return agent_root_pipeline_validator(values)

    @field_validator('name')
    @classmethod
    def check_base(cls, value: str) -> str:
        assert value != 'base', "Name of created application cannot be 'base'"
        return value

    @model_validator(mode='before')
    @classmethod
    def author_id_forwarding(cls, values: dict):
        author_id = values.get('author_id')
        for tool in values.get('tools', []):
            tool['author_id'] = author_id
        return values

    @model_validator(mode='after')
    def set_default_meta(self):
        self.meta = self.meta or {}
        if 'step_limit' not in self.meta:
            self.meta['step_limit'] = DEFAULT_STEP_LIMIT
        return self


class ApplicationVersionForkCreateModel(ApplicationVersionBaseModel, ApplicationVersionArgsForwardingModel):
    tools: Optional[List[ToolCreateModel]] = None
    meta: Optional[dict] = {}


class ApplicationVersionBaseCreateModel(ApplicationVersionBaseModel, ApplicationVersionArgsForwardingModel):
    name: Literal['base'] = 'base'
    llm_settings: LLMSettingsModel
    tools: Optional[List[ToolCreateModel]] = None
    meta: Optional[dict] = {}

    @model_validator(mode='before')
    @classmethod
    def validate_diagram_yaml(cls, values: dict):
        return agent_root_pipeline_validator(values)

    @model_validator(mode='before')
    @classmethod
    def author_id_forwarding(cls, values: dict):
        author_id = values.get('author_id')
        for tool in values.get('tools', []):
            tool['author_id'] = author_id
        return values

    @model_validator(mode='after')
    def set_default_meta(self):
        self.meta = self.meta or {}
        if 'step_limit' not in self.meta:
            self.meta['step_limit'] = DEFAULT_STEP_LIMIT
        return self


class ApplicationVersionDetailModel(ApplicationVersionBaseModel):
    id: int
    status: PublishStatus
    created_at: datetime
    author: Optional[AuthorBaseModel] = None
    tags: Optional[List[TagDetailModel]] = None
    tools: Optional[List[ToolDetails]] = None
    variables: Optional[List[ApplicationVariableDetailedModel]] = None
    meta: Optional[dict] = {}
    is_forked: bool = False
    icon_meta: Optional[dict] = {}

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode='wrap')
    @classmethod
    def apply_selected_tools(cls, values, handler, info):
        """Override model validation to apply selected_tools intersection from tool_mappings"""
        from ...utils.application_utils import apply_selected_tools_intersection
        
        # Let Pydantic do its normal validation
        if info.mode == 'python' and hasattr(values, '__dict__'):
            # This is from_orm case - values is the ORM object
            obj = values
            instance = handler(values)
            
            # Apply selected_tools intersection if tool_mappings exists and was loaded
            if hasattr(obj, 'tool_mappings') and instance.tools:
                apply_selected_tools_intersection(instance.tools, obj.tool_mappings)
            
            return instance
        else:
            # Normal dict/model validation
            return handler(values)

    @model_validator(mode='after')
    def add_author_data(self, info: ValidationInfo):
        # Populate author from author_id if not already set
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

    @model_validator(mode='after')
    def set_is_forked(self):
        meta = self.meta or {}
        if 'parent_entity_id' in meta and 'parent_project_id' in meta:
            self.is_forked = True
        return self

    @model_validator(mode='after')
    def set_icon_meta(self):
        meta = self.meta or {}

        if 'icon_meta' in meta:
            self.icon_meta = meta['icon_meta'] or {}

        return self


class ApplicationVersionDetailToolValidatedModel(ApplicationVersionDetailModel, ApplicationVersionArgsForwardingModel):
    tools: Optional[List[ToolValidatedDetails]] = []


class ApplicationExportVersionDetailModel(ApplicationVersionDetailModel, ApplicationVersionArgsForwardingModel):
    import_version_uuid: str = None
    tools: Optional[List[ToolApplicationExportDetails]] = None
    author_id: int = Field(..., exclude=True)
    shared_id: Optional[int] = Field(None, exclude=True)
    shared_owner_id: Optional[int] = Field(None, exclude=True)
    application_id: Optional[int] = Field(None, exclude=True)
    status: PublishStatus = Field(..., exclude=True)
    author: Optional[AuthorBaseModel] = Field(None, exclude=True)

    @model_validator(mode='after')
    def exclude_icon_meta(self):
        if self.meta:
            self.meta['icon_meta'] = {}
        return self

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        hash_ = hash((self.__class__.__name__, self.id, self.author_id, self.name))
        self.import_version_uuid = str(uuid.UUID(int=abs(hash_)))
        return self

    model_config = ConfigDict(from_attributes=False)


class ApplicationVersionListModel(BaseModel):
    id: int
    name: str
    status: PublishStatus
    created_at: datetime  # probably delete this
    author_id: int = Field(..., exclude=True)
    meta: Optional[dict] = Field(default_factory=dict)
    tags: List[TagListModel] = Field(..., exclude=True)
    agent_type: AgentTypes = Field(..., exclude=True)
    instructions: str

    model_config = ConfigDict(from_attributes=True)


class ApplicationVersionFullUpdateModel(ApplicationVersionBaseModel, ApplicationVersionArgsForwardingModel):
    id: int
    application_id: int
    name: Optional[str] = None
    author_id: int
    tools: Optional[List[ToolUpdateModel]] = None
    variables: Optional[List[ApplicationVariableDetailedModel]] = None
    pipeline_settings: Optional[dict] = Field(default_factory=dict)
    meta: Optional[dict] = Field(default_factory=dict)

    project_id: int = Field(..., exclude=True)
    user_id: int = Field(..., exclude=True)

    @model_validator(mode='before')
    @classmethod
    def validate_diagram_yaml(cls, values: dict):
        return agent_root_pipeline_validator(values)

    @model_validator(mode='before')
    @classmethod
    def author_id_forwarding(cls, values: dict):
        author_id = values.get('author_id')
        for tool in values.get('tools', []):
            # author_ids can be different in each toolkits
            # so skip forwarding if already set by client
            if 'author_id' not in tool:
                tool['author_id'] = author_id

        return values

    @model_validator(mode='after')
    def validate_attachment_toolkit_id(self):
        meta = self.meta or {}
        attachment_toolkit_id = meta.get('attachment_toolkit_id')
        if attachment_toolkit_id is None:
            return self

        project_id = self.project_id

        from ...utils.application_utils import check_if_usable_attachment_toolkit

        check_if_usable_attachment_toolkit(
            project_id=project_id,
            attachment_toolkit_id=attachment_toolkit_id,
            application_id=self.application_id,
            version_id=self.id
        )

        return self


class ApplicationVersionUpdateModel(ApplicationVersionBaseModel):
    id: int
    application_id: int
    tags: Optional[List[PromptTagUpdateModel]] = []
    pipeline_settings: Optional[dict] = None
    meta: Optional[dict] = Field(default_factory=dict)

    project_id: int = Field(..., exclude=True)

    @model_validator(mode='before')
    @classmethod
    def validate_diagram_yaml(cls, values: dict):
        return agent_root_pipeline_validator(values)

    @model_validator(mode='after')
    def validate_attachment_toolkit_id(self):
        meta = self.meta or {}
        attachment_toolkit_id = meta.get('attachment_toolkit_id')
        if attachment_toolkit_id is None:
            return self

        project_id = self.project_id

        from ...utils.application_utils import check_if_usable_attachment_toolkit

        check_if_usable_attachment_toolkit(
            project_id=project_id,
            attachment_toolkit_id=attachment_toolkit_id,
            application_id=self.application_id,
            version_id=self.id
        )

        return self


class ApplicationVersionModel(ApplicationVersionBaseModel):
    status: Optional[PublishStatus] = None
    tags: Optional[List[TagListModel]] = None

    model_config = ConfigDict(from_attributes=True)

