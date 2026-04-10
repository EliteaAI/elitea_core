from typing import Optional, Dict, Union, Type

from pydantic import BaseModel, Field, ConfigDict, model_validator

from ..enums.all import ParticipantTypes


class ParticipantBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: Optional[int] = None
    entity_name: str
    entity_meta: dict
    meta: Optional[dict] = Field(default_factory=dict)
    entity_settings: Optional[dict] = Field(default_factory=dict)


class ParticipantDetails(ParticipantBase):
    pass


class ParticipantEntityDummy(BaseModel):
    pass


class ParticipantEntityUser(BaseModel):
    id: int


class ParticipantEntityLlm(BaseModel):
    model_name: str


class ParticipantEntityDatasource(BaseModel):
    id: int
    project_id: int


class ParticipantEntityApplication(BaseModel):
    id: int
    project_id: int
#    model_name: Optional[str]
#    integration_uid: Optional[str]


class ParticipantEntityToolkit(BaseModel):
    id: int
    project_id: int


EntityMetaType = Union[
    dict,
    ParticipantEntityDummy,
    ParticipantEntityUser,
    ParticipantEntityLlm,
    ParticipantEntityDatasource,
    ParticipantEntityApplication,
    ParticipantEntityToolkit
]

MappingValueType = Union[
    Type[ParticipantEntityDummy],
    Type[ParticipantEntityUser],
    Type[ParticipantEntityLlm],
    Type[ParticipantEntityDatasource],
    Type[ParticipantEntityApplication],
    Type[ParticipantEntityToolkit]
]

entity_meta_mapping: Dict[ParticipantTypes, MappingValueType] = {
    ParticipantTypes.dummy: ParticipantEntityDummy,
    ParticipantTypes.user: ParticipantEntityUser,
    ParticipantTypes.llm: ParticipantEntityLlm,
    ParticipantTypes.datasource: ParticipantEntityDatasource,
    ParticipantTypes.application: ParticipantEntityApplication,
    ParticipantTypes.toolkit: ParticipantEntityToolkit,
    # ParticipantTypes.pipeline: ParticipantEntityApplication,
}


class ParticipantCreate(BaseModel):
    entity_name: ParticipantTypes
    entity_meta: EntityMetaType
    entity_settings: Optional[dict] = Field(default_factory=dict)

    @model_validator(mode='after')
    def validate_entity_meta(self):
        if self.entity_name not in entity_meta_mapping:
            raise ValueError(f'Unsupported entity {self.entity_name}')
        validation_class = entity_meta_mapping[self.entity_name]
        self.entity_meta = validation_class.model_validate(self.entity_meta)
        
        return self
