"""Request and response of POST /generate_pipeline_node_draft."""
from typing import List, Literal, Optional, get_args

from pydantic import BaseModel, Field, constr

from ..predict_llm import LLMSettingsRequest
from .draft import LIMITS


class GeneratePipelineNodeDraftRequest(BaseModel):
    application_id: int
    version_id: int
    node_id: constr(strip_whitespace=True, min_length=1)
    node_type: str
    mode: Literal['build', 'edit']
    requirement: constr(strip_whitespace=True, min_length=1, max_length=LIMITS.requirement_max_length)
    current_node: dict = Field(default_factory=dict, description='The node as it is in the editor now.')
    draft_node: Optional[dict] = Field(default=None, description='Refine: the draft being adjusted.')
    pipeline_yaml: str = Field(description='The unsaved editor YAML, so the context matches what the user sees.')
    llm_settings: Optional[LLMSettingsRequest] = None


class NodeDraftAttachment(BaseModel):
    entity_type: Literal['toolkit', 'mcp', 'agent', 'pipeline']
    id: int
    name: str
    needs_login: bool = False


StateType = Literal['str', 'list', 'dict', 'int', 'float', 'bool']
STATE_TYPES = get_args(StateType)


class NodeDraftStateVariable(BaseModel):
    name: str
    type: StateType


class GeneratePipelineNodeDraftResponse(BaseModel):
    summary: str
    node: dict
    changed_paths: List[str]
    attachments: List[NodeDraftAttachment] = Field(default_factory=list)
    new_state: List[NodeDraftStateVariable] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
