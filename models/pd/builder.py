"""Request bodies of the Elitea Builder MCP tools (#7383)."""
from typing import Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

EntityType = Literal['agent', 'pipeline', 'skill', 'project_context', 'toolkit', 'model', 'configuration',
                     'operation']


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid')


class BuilderFindModel(_Strict):
    type: EntityType = Field(description="toolkit also finds MCP servers (marked mcp: true); there is no separate mcp "
                                         "type. toolkit, model and configuration are lookups: use the id, don't read.")
    query: str = Field('', description="Name or words to search for. For type operation: what you want to do.")
    limit: int = Field(10, ge=1, le=25)


class BuilderReadModel(_Strict):
    type: EntityType = Field(description="agent, pipeline, skill, project_context or operation. Toolkits, MCPs, models "
                                         "and configurations can't be read.")
    id: Optional[Union[int, str]] = Field(None, description="Id from builder_find (op_id for type operation).")
    version_id: Optional[int] = None


class BuilderProposeModel(_Strict):
    type: EntityType
    op: Literal['create', 'update', 'delete', 'call']
    id: Optional[Union[int, str]] = Field(None, description="Target id from builder_find; op_id for type operation.")
    name: Optional[str] = Field(None, description="Target by exact name instead of id. For create: the new entity's name.")
    version_id: Optional[int] = None
    changes: dict = Field(
        default_factory=dict,
        description=("Only the fields that change. Text fields accept {old_text,new_text} | {append} | {replace}. "
                     "For type operation: the operation's arguments."),
    )
    base_revision: Optional[str] = Field(None, description="revision returned by builder_read.")


class BuilderApplyModel(_Strict):
    changeset_id: str


class BuilderReferenceModel(_Strict):
    topic: str = Field(description="playbook:<agent|pipeline|skill|project_context> or pipeline:overview")
