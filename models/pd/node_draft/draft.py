"""Internal models of a node draft run and the limits it runs with."""
from typing import Dict, FrozenSet, List, Literal, Optional, Set

from pydantic import BaseModel, Field


class NodeDraftLimits(BaseModel):
    """Every tunable number of node drafting in one place."""
    budget_seconds: int = 60
    min_repair_seconds: int = 20
    temperature: float = 0.2
    max_tokens: int = 1024
    long_max_tokens: int = 2048
    long_types: FrozenSet[str] = frozenset({'llm', 'code', 'state_modifier'})
    reasoning_max_tokens: int = 8192
    reasoning_effort: str = 'low'
    resource_cap: int = 15
    description_limit: int = 160
    params_for_top: int = 3
    min_token_length: int = 3
    max_details: int = 10
    repair_details_limit: int = 1500
    summary_limit: int = 200
    requirement_max_length: int = 2000
    max_body_bytes: int = 200 * 1024

    def max_tokens_for(self, node_type: str) -> int:
        return self.long_max_tokens if node_type in self.long_types else self.max_tokens


LIMITS = NodeDraftLimits()


class ParsedPipeline(BaseModel):
    state: List[dict]
    node_ids: List[str]
    incoming: List[str]
    entry_point: Optional[str] = None
    unattended: bool = False
    nodes: List[dict] = Field(default_factory=list)


class DraftTarget(BaseModel):
    """What a draft is about. ``resources`` is ``{kind: {name: {id, attached, needs_login, entity_type, tools}}}``
    with ``tools`` as ``{tool: {param: required}}``; kinds are toolkit, mcp, agent."""
    node_id: str
    node_type: str
    mode: Literal['build', 'edit']
    current: dict
    base: Optional[dict] = None
    state: Dict[str, str]
    node_ids: Set[str]
    resources: Dict[str, Dict[str, dict]]


class ModelReply(BaseModel):
    text: str = ''
    timed_out: bool = False
    cut_short: bool = False
    error: Optional[str] = None
