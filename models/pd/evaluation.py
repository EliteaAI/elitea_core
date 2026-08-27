"""Pydantic request/response models for the Agent-Evaluation **Library** (EVAL-P1-B1).

Covers the library *definition* types (§16, §3.1, §19):
  * dimension definitions   -> EvalDimension  (AI / Human / Code criteria; a Code-engine
                               dimension carries a script instead of a rubric, §2.1)

and the **suite + binding** models (EVAL-P1-B2, §13, §16.2):
  * suite                    -> EvalSuite    (named binding set on an agent + version)
  * binding                  -> EvalBinding  (a library item applied with weight/target/engine)

and the **human-score** write/read models (EVAL-P1-B6, §15.5, §15.6):
  * human score              -> EvalHumanScore (append-only annotation on a case × dimension)

The ORM lives in ``models/evaluation.py``; these models are the API boundary
(validation + serialization). Value vocabularies are imported from the ORM module so
the allowed strings stay single-sourced.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

from ..evaluation import (
    EvalTier, EvalEngine, EvalScaleType, EvalPolarity, EvalCaseSource, EvalRunTrigger,
)

_ENGINES = {EvalEngine.ai, EvalEngine.human, EvalEngine.code}
_SCALE_TYPES = {EvalScaleType.binary, EvalScaleType.ordinal, EvalScaleType.continuous}
_POLARITIES = {EvalPolarity.higher_better, EvalPolarity.lower_better}
_OPERATORS = {'>=', '>', '<=', '<', '=='}
_RETURN_CONTRACTS = {'bool', 'number'}
_EVIDENCE_KEYS = {'structure', 'input', 'output', 'expected'}
# project library is home (§16); platform tier is seeded via the admin console, not this API.
_PROJECT_WRITABLE_TIERS = {EvalTier.project, EvalTier.agent_adhoc}
_CASE_SOURCES = {EvalCaseSource.manual, EvalCaseSource.import_, EvalCaseSource.conversation}
_IMPORT_FORMATS = {'csv', 'json'}
_RUN_TRIGGERS = {EvalRunTrigger.offline_batch, EvalRunTrigger.on_demand}


def _check_evidence_scope(v: dict) -> dict:
    bad = set(v) - _EVIDENCE_KEYS
    if bad:
        raise ValueError(f'evidence_scope keys must be a subset of {sorted(_EVIDENCE_KEYS)}')
    if any(not isinstance(val, bool) for val in v.values()):
        raise ValueError('evidence_scope values must be booleans')
    if not any(v.get(key, False) for key in ('structure', 'input', 'output')):
        raise ValueError('evidence_scope must have at least one of structure/input/output set to true')
    return v


# ----------------------------------------------------------------------------
# Dimension definitions
# ----------------------------------------------------------------------------

class EvalDimensionBaseModel(BaseModel):
    description: Optional[str] = None
    allowed_engines: List[str] = Field(default_factory=lambda: [EvalEngine.ai])
    scale_type: str = EvalScaleType.continuous
    scale_min: float = 0.0
    scale_max: float = 100.0
    polarity: str = EvalPolarity.higher_better
    # A negative weight would invert the criterion inside the weighted mean (§20.6): a good score
    # would pull the headline down, and a large enough negative weight can push the denominator
    # to zero or negative and produce a headline outside 0..100 entirely.
    default_weight: float = Field(1.0, ge=0)
    default_target: Optional[float] = None
    default_target_operator: Optional[str] = None
    # Code-engine authoring (§2.1): a dimension with allowed_engines == ['code'] carries a
    # script instead of a rubric. Required together, forbidden for any AI/Human dimension.
    code: Optional[str] = Field(None, min_length=1)
    return_contract: Optional[str] = None
    meta: dict = Field(default_factory=dict)

    @field_validator('allowed_engines')
    @classmethod
    def _validate_engines(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError('allowed_engines must not be empty')
        bad = [e for e in v if e not in _ENGINES]
        if bad:
            raise ValueError(f'unknown engine(s): {bad}; allowed: {sorted(_ENGINES)}')
        if EvalEngine.code in v and len(v) > 1:
            raise ValueError('a code-engine dimension cannot also allow ai/human')
        return v

    @field_validator('scale_type')
    @classmethod
    def _validate_scale_type(cls, v: str) -> str:
        if v not in _SCALE_TYPES:
            raise ValueError(f'scale_type must be one of {sorted(_SCALE_TYPES)}')
        return v

    @field_validator('polarity')
    @classmethod
    def _validate_polarity(cls, v: str) -> str:
        if v not in _POLARITIES:
            raise ValueError(f'polarity must be one of {sorted(_POLARITIES)}')
        return v

    @field_validator('default_target_operator')
    @classmethod
    def _validate_operator(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _OPERATORS:
            raise ValueError(f'default_target_operator must be one of {sorted(_OPERATORS)}')
        return v

    @field_validator('return_contract')
    @classmethod
    def _validate_contract(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _RETURN_CONTRACTS:
            raise ValueError(f'return_contract must be one of {sorted(_RETURN_CONTRACTS)}')
        return v

    @model_validator(mode='after')
    def _validate_scale_bounds(self):
        if self.scale_min >= self.scale_max:
            raise ValueError('scale_min must be strictly less than scale_max')
        return self

    @model_validator(mode='after')
    def _validate_code_fields(self):
        is_code = self.allowed_engines == [EvalEngine.code]
        if is_code and not self.code:
            raise ValueError('code is required when allowed_engines is [\'code\']')
        if not is_code and (self.code is not None or self.return_contract is not None):
            raise ValueError('code / return_contract are only valid for a code-engine dimension')
        if is_code and self.return_contract is None:
            self.return_contract = 'bool'
        return self


class EvalDimensionCreateModel(EvalDimensionBaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    # project library is home; platform-tier authoring is not exposed here.
    tier: str = EvalTier.project
    # Owning agent, required when tier=agent_adhoc (scopes visibility to that agent) and
    # forbidden otherwise (project/platform dimensions are not owned by a single agent).
    agent_id: Optional[int] = None

    @field_validator('tier')
    @classmethod
    def _validate_tier(cls, v: str) -> str:
        if v not in _PROJECT_WRITABLE_TIERS:
            raise ValueError(
                f'tier must be one of {sorted(_PROJECT_WRITABLE_TIERS)} '
                '(platform-tier definitions are managed via the admin console)'
            )
        return v

    @model_validator(mode='after')
    def _validate_agent_id(self):
        if self.tier == EvalTier.agent_adhoc and self.agent_id is None:
            raise ValueError('agent_id is required when tier is agent_adhoc')
        if self.tier != EvalTier.agent_adhoc and self.agent_id is not None:
            raise ValueError('agent_id must not be set unless tier is agent_adhoc')
        return self


class EvalDimensionUpdateModel(EvalDimensionBaseModel):
    # name optional on update; everything else may be edited. tier is immutable post-create.
    name: Optional[str] = Field(None, min_length=1, max_length=128)

    @model_validator(mode='after')
    def _validate_code_fields(self):
        # Overrides the base check: update_dimension() applies this model with
        # exclude_unset=True, so a PUT that only sends {"code": "..."} without re-sending
        # allowed_engines must not be judged against the ['ai'] default it never asked for
        # (that previously made every code-only edit fail with "code / return_contract are
        # only valid for a code-engine dimension"). Only enforce the pairing when the caller
        # actually set allowed_engines in this request.
        if 'allowed_engines' not in self.model_fields_set:
            return self
        is_code = self.allowed_engines == [EvalEngine.code]
        if is_code and not self.code:
            raise ValueError('code is required when allowed_engines is [\'code\']')
        if not is_code and (self.code is not None or self.return_contract is not None):
            raise ValueError('code / return_contract are only valid for a code-engine dimension')
        if is_code and self.return_contract is None:
            self.return_contract = 'bool'
        return self


class EvalDimensionDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    tier: str
    name: str
    description: Optional[str] = None
    agent_id: Optional[int] = None
    allowed_engines: List[str]
    scale_type: str
    scale_min: float
    scale_max: float
    polarity: str
    default_weight: float
    default_target: Optional[float] = None
    default_target_operator: Optional[str] = None
    code: Optional[str] = None
    return_contract: Optional[str] = None
    owner_id: int
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


# ----------------------------------------------------------------------------
# Bindings — a library item applied within a suite (§16.2, §13.2)
# ----------------------------------------------------------------------------

class EvalBindingBaseModel(BaseModel):
    """Binding knobs that override definition defaults (§16.2). ``evidence_scope`` is the
    Axis-C selector the judge/code sees."""
    engine: str = EvalEngine.ai
    evidence_scope: dict = Field(
        default_factory=lambda: {'structure': False, 'input': True, 'output': True}
    )
    # ge=0 for the same reason as EvalDimensionBaseModel.default_weight: a negative weight
    # inverts the criterion in the weighted mean and can move the headline out of 0..100.
    # 0 stays legal — it is the documented "informational only" weight (§20.6).
    weight: float = Field(1.0, ge=0)
    target: Optional[float] = None
    target_operator: Optional[str] = None
    order_index: int = 0
    # version pin (§16.3): the concrete ApplicationVersion this binding applies to
    application_version_id: Optional[int] = None
    meta: dict = Field(default_factory=dict)

    @field_validator('engine')
    @classmethod
    def _validate_engine(cls, v: str) -> str:
        if v not in _ENGINES:
            raise ValueError(f'engine must be one of {sorted(_ENGINES)}')
        return v

    @field_validator('target_operator')
    @classmethod
    def _validate_operator(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _OPERATORS:
            raise ValueError(f'target_operator must be one of {sorted(_OPERATORS)}')
        return v

    @field_validator('evidence_scope')
    @classmethod
    def _validate_evidence(cls, v: dict) -> dict:
        return _check_evidence_scope(v)


class EvalBindingCreateModel(EvalBindingBaseModel):
    # exactly one source of the item being bound (§16.3)
    dimension_id: Optional[int] = None
    platform_key: Optional[str] = Field(None, max_length=128)

    @model_validator(mode='after')
    def _validate_single_source(self):
        sources = [self.dimension_id, self.platform_key]
        provided = [s for s in sources if s is not None]
        if len(provided) != 1:
            raise ValueError('exactly one of dimension_id / platform_key must be set')
        return self


class EvalBindingUpdateModel(EvalBindingBaseModel):
    # binding knobs only; the bound source (dimension/code/platform) is immutable post-create.
    engine: Optional[str] = None
    evidence_scope: Optional[dict] = None
    weight: Optional[float] = Field(None, ge=0)
    order_index: Optional[int] = None

    @field_validator('engine')
    @classmethod
    def _validate_engine(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _ENGINES:
            raise ValueError(f'engine must be one of {sorted(_ENGINES)}')
        return v

    @field_validator('evidence_scope')
    @classmethod
    def _validate_evidence(cls, v: Optional[dict]) -> Optional[dict]:
        return v if v is None else _check_evidence_scope(v)


class EvalBindingDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    suite_id: int
    application_version_id: Optional[int] = None
    dimension_id: Optional[int] = None
    platform_key: Optional[str] = None
    engine: str
    evidence_scope: dict = Field(default_factory=dict)
    weight: float
    target: Optional[float] = None
    target_operator: Optional[str] = None
    order_index: int
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EvalBindingReorderModel(BaseModel):
    """Full ordered list of binding ids for a suite; index in the list becomes order_index."""
    binding_ids: List[int] = Field(..., min_length=1)


# ----------------------------------------------------------------------------
# Suites — a named binding set on an agent + version (§13, §16.2)
# ----------------------------------------------------------------------------

class EvalSuiteBaseModel(BaseModel):
    dataset_id: Optional[int] = None
    judge_model: Optional[dict] = None           # per-suite judge override (§18.7)
    baseline_run_id: Optional[int] = None         # comparison baseline pointer (§21.6)
    trigger_config: dict = Field(default_factory=dict)
    meta: dict = Field(default_factory=dict)


class EvalSuiteCreateModel(EvalSuiteBaseModel):
    application_id: int
    # ORM default is 'Default suite' — an empty name bootstraps the default suite (AC).
    name: str = Field('Default suite', min_length=1, max_length=128)


class EvalSuiteUpdateModel(EvalSuiteBaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)


class EvalSuiteDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    application_id: int
    name: str
    dataset_id: Optional[int] = None
    judge_model: Optional[dict] = None
    baseline_run_id: Optional[int] = None
    trigger_config: dict = Field(default_factory=dict)
    owner_id: int
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    bindings: List[EvalBindingDetailModel] = Field(default_factory=list)

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


# ----------------------------------------------------------------------------
# Human scores — append-only annotation on a case × dimension (§15.5, §15.6, D2)
# ----------------------------------------------------------------------------

class EvalHumanScoreCreateModel(BaseModel):
    """A single human annotation. Append-only: every write is a new row (D2). The
    reviewer is taken from the auth context, not the body; ``normalized_score`` is
    computed server-side from the dimension's scale (§20.3), never sent by the client."""
    dataset_case_id: int
    dimension_id: int
    native_score: float
    note: Optional[str] = None


class EvalHumanScoreDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: int
    dataset_case_id: Optional[int] = None
    dimension_id: Optional[int] = None
    reviewer_id: int
    native_score: Optional[float] = None
    normalized_score: Optional[float] = None
    note: Optional[str] = None
    is_latest: bool
    created_at: Optional[datetime] = None


# ----------------------------------------------------------------------------
# Datasets + cases (EVAL-P1-B3, §17.1, §17.2)
# ----------------------------------------------------------------------------

class EvalDatasetCaseBaseModel(BaseModel):
    """A single golden case (§17.1). ``expected_output`` present → the case supports
    reference-based validations; absent → reference-free only (§17.5)."""
    variables: dict = Field(default_factory=dict)
    expected_output: Optional[str] = None
    source_type: str = EvalCaseSource.manual
    source_ref: Optional[str] = Field(None, max_length=256)
    order_index: int = 0
    meta: dict = Field(default_factory=dict)

    @field_validator('source_type')
    @classmethod
    def _validate_source_type(cls, v: str) -> str:
        if v not in _CASE_SOURCES:
            raise ValueError(f'source_type must be one of {sorted(_CASE_SOURCES)}')
        return v


class EvalDatasetCaseCreateModel(EvalDatasetCaseBaseModel):
    input: str = Field(..., min_length=1)


class EvalDatasetCaseUpdateModel(EvalDatasetCaseBaseModel):
    # input optional on update; source_type is immutable post-create (drop it on write).
    input: Optional[str] = Field(None, min_length=1)
    source_type: Optional[str] = None

    @field_validator('source_type')
    @classmethod
    def _validate_source_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _CASE_SOURCES:
            raise ValueError(f'source_type must be one of {sorted(_CASE_SOURCES)}')
        return v


class EvalDatasetCaseDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_id: int
    order_index: int
    input: str
    variables: dict = Field(default_factory=dict)
    expected_output: Optional[str] = None
    source_type: str
    source_ref: Optional[str] = None
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EvalDatasetBaseModel(BaseModel):
    description: Optional[str] = None
    meta: dict = Field(default_factory=dict)


class EvalDatasetCreateModel(EvalDatasetBaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    # Owning agent (#6350) — every dataset is authored in the context of one agent.
    agent_id: int
    is_shared: bool = False


class EvalDatasetUpdateModel(EvalDatasetBaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=256)
    is_shared: Optional[bool] = None


class EvalDatasetSummaryModel(BaseModel):
    """List-row shape (§17.3): counts instead of the full case set."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    name: str
    description: Optional[str] = None
    agent_id: Optional[int] = None
    is_shared: bool = False
    owner_id: int
    meta: dict = Field(default_factory=dict)
    case_count: int = 0
    with_expected_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


class EvalDatasetDetailModel(BaseModel):
    """Detail shape (§17.4): embeds the ordered case set.

    ``cases`` is a bounded window, not necessarily the whole set — ``case_count`` is the real
    total and ``cases_truncated`` says whether the window stops short of it. The full set is
    paged through the cases collection endpoint."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    name: str
    description: Optional[str] = None
    agent_id: Optional[int] = None
    is_shared: bool = False
    owner_id: int
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    cases: List[EvalDatasetCaseDetailModel] = Field(default_factory=list)
    case_count: int = 0
    cases_truncated: bool = False

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


class EvalDatasetImportModel(BaseModel):
    """Bulk case import (§17.2 CSV/JSON). ``content`` is the raw file text; rows are parsed
    and validated per-row by the import util, which returns an accepted-count + error report."""
    format: str = Field(..., description="csv | json")
    # Capped at the API boundary so a huge body is rejected before it is parsed and held in memory
    # twice (raw text + parsed rows); the parser applies its own per-row and per-cell caps.
    content: str = Field(..., min_length=1, max_length=20_000_000)

    @field_validator('format')
    @classmethod
    def _validate_format(cls, v: str) -> str:
        v = (v or '').lower()
        if v not in _IMPORT_FORMATS:
            raise ValueError(f'format must be one of {sorted(_IMPORT_FORMATS)}')
        return v


class EvalDatasetPromoteModel(BaseModel):
    """Promote-from-conversations (§17.2, §8.3). Each user turn → a case ``input``; the agent
    reply becomes ``expected_output`` when ``include_expected`` (else the case is reference-free).
    ``source_type=conversation`` + ``source_ref=<conversation_id>`` links back to the origin."""
    conversation_id: int
    include_expected: bool = True


# ----------------------------------------------------------------------------
# Runs — start (offline-batch / on-demand) + status/list (EVAL-P1-B4, §7#6, §14.2)
# ----------------------------------------------------------------------------

class EvalRunCreateModel(BaseModel):
    """Start-run request (§14.2). ``trigger_type`` selects the path: ``offline_batch`` scores the
    suite's dataset (or ``dataset_id`` override), ``on_demand`` scores a stored conversation
    (``conversation_id`` required, §14.4 — reference-free only). ``application_version_id`` pins the
    frozen version (D3, §21.6); when omitted it resolves from the bindings' pins. ``judge_model``
    overrides the suite's judge for this run (§18.7). No orchestration here — that is H5."""
    suite_id: int
    trigger_type: str = EvalRunTrigger.offline_batch
    dataset_id: Optional[int] = None
    conversation_id: Optional[int] = None
    application_version_id: Optional[int] = None
    judge_model: Optional[dict] = None

    @field_validator('trigger_type')
    @classmethod
    def _validate_trigger(cls, v: str) -> str:
        if v not in _RUN_TRIGGERS:
            raise ValueError(f'trigger_type must be one of {sorted(_RUN_TRIGGERS)}')
        return v

    @model_validator(mode='after')
    def _validate_on_demand(self):
        if self.trigger_type == EvalRunTrigger.on_demand and self.conversation_id is None:
            raise ValueError('conversation_id is required for on_demand runs')
        return self


class EvalRunSummaryModel(BaseModel):
    """List-row / start-ack shape (§14.2 progress feed) — the run metadata without the (large)
    frozen snapshot."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    suite_id: Optional[int] = None
    application_id: int
    application_version_id: int
    dataset_id: Optional[int] = None
    trigger_type: str
    status: str
    headline_score: Optional[float] = None
    progress: dict = Field(default_factory=dict)
    error: Optional[str] = None
    owner_id: int
    meta: dict = Field(default_factory=dict)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


class EvalRunDetailModel(EvalRunSummaryModel):
    """Detail shape: the summary plus the immutable snapshot the run froze at start (§3.4)."""
    snapshot: dict = Field(default_factory=dict)


# ----------------------------------------------------------------------------
# Results read + re-aggregation (EVAL-P1-B5, §15.5, §20.10)
# ----------------------------------------------------------------------------

class EvalResultDetailModel(BaseModel):
    """One case × validation verdict inside a run (§20.10). Carries both the native score on the
    author's scale and the normalized 0-100 score, plus the frozen alignment keys, the verdict
    envelope (rationale / pass-fail / code stdout / judge raw) and the evidence actually shown."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: int
    dataset_case_id: Optional[int] = None
    dimension_id: Optional[int] = None
    platform_key: Optional[str] = None
    engine: str
    status: str
    native_score: Optional[float] = None
    normalized_score: Optional[float] = None
    verdict: dict = Field(default_factory=dict)
    evidence: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EvalRunResultsModel(BaseModel):
    """Results read envelope (screen #7). Bundles the run detail (incl. the frozen snapshot with
    binding weights), every result row, the latest human annotation per (case, dimension), and the
    server-side ``headline_score`` re-derived from those same normalized items — so a client
    recompute over the returned per-item scores + snapshot weights matches it (EVAL-E2E-09).

    ``results`` is one page of rows and ``total`` is the run's full count; ``headline_score`` always
    spans the whole run, so paging never moves the reported score."""
    run: EvalRunDetailModel
    results: List[EvalResultDetailModel] = Field(default_factory=list)
    human_scores: List[EvalHumanScoreDetailModel] = Field(default_factory=list)
    headline_score: Optional[float] = None
    total: int = 0
    limit: Optional[int] = None
    offset: int = 0
