import uuid
from typing import List, Optional, Any

from pydantic import model_validator, Field, ConfigDict
from .application import ApplicationBaseModel, ApplicationArgsForwardingModel
from .tool import ToolApplicationExportDetails
from .version import ApplicationVersionDetailModel, ApplicationVersionArgsForwardingModel
from ...models.pd.llm import LLMSettingsModel


class ApplicationVersionForkModel(ApplicationVersionDetailModel, ApplicationVersionArgsForwardingModel):
    id: int
    llm_settings: LLMSettingsModel
    import_version_uuid: str = None
    tools: Optional[List[ToolApplicationExportDetails]] = None

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        hash_ = hash((self.__class__.__name__, self.id, self.author_id, self.name))
        self.import_version_uuid = str(uuid.UUID(int=abs(hash_)))
        return self

    @model_validator(mode='after')
    def exclude_icon_meta(self):
        if self.meta:
            self.meta['icon_meta'] = {}
        return self


class ApplicationForkModel(ApplicationBaseModel, ApplicationArgsForwardingModel):
    id: int
    import_uuid: str = None
    owner_id: int
    versions: List[ApplicationVersionForkModel]
    webhook_secret: Any = Field(None, exclude=True)  # Override parent to exclude

    @model_validator(mode='after')
    def validate_repeatable_uuid(self):
        hash_ = hash((self.__class__.__name__, self.id, self.owner_id, self.name))
        self.import_uuid = str(uuid.UUID(int=abs(hash_)))
        return self

    model_config = ConfigDict(from_attributes=True)

