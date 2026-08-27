"""Agent-evaluation P1 data model (EVAL-H6).

Project-scoped tables (schema ``p_<project_id>``) implementing the entity model in
AGENT_EVALUATION_DESIGN.md §3 / §16.2 / §17.1 / §20.10 / §21.6.

Shape:
  Library (reusable definitions)   -> EvalDimension (engine: ai/human/code)
  Binding (per-agent application)  -> EvalSuite, EvalBinding
  Dataset                          -> EvalDataset, EvalDatasetCase
  Immutable run + results          -> EvalRun (frozen snapshot), EvalResult
  Mutable human layer              -> EvalHumanScore (append-only, §3.4/D2)

Design decisions frozen here:
  * D2 (§3.4 vs §15.3): human scores are **append-only** — every write is a new row,
    aggregate reads the latest by created_at. No overwrite, full audit trail (§15.6).
  * §20.10: every result stores **both** native and normalized scores.
  * §3.4/§16.3: EvalRun carries a frozen ``snapshot`` (suite config + definitions +
    bindings + dataset case set + scale specs) so later edits never mutate history.
    Result rows reference the frozen case/dimension **by id value** (not FK) so that
    editing or deleting a dataset case never corrupts a finished run.
  * D3 (§21.6): the run freezes a concrete ``application_version_id``.

New tenant tables are provisioned automatically by
``db.get_tenant_specific_metadata().create_all()`` (new projects via
projects/utils/project_steps.py; existing projects via the admin db task), so no raw
SQL migration is required for table creation.
"""

import uuid
from datetime import datetime
from typing import List, Optional

from tools import db_tools, db, config as c
from sqlalchemy import (
    Integer, String, Text, DateTime, Float, Boolean, func, ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.mutable import MutableDict

from .all import Application, ApplicationVersion


# --- value vocabularies (String columns, mirroring the PublishStatus pattern) ---

class EvalTier:
    platform = 'platform'
    project = 'project'
    agent_adhoc = 'agent_adhoc'


class EvalEngine:
    ai = 'ai'
    human = 'human'
    code = 'code'


class EvalScaleType:
    binary = 'binary'
    ordinal = 'ordinal'      # e.g. 1..5
    continuous = 'continuous'  # e.g. 0..100


class EvalPolarity:
    higher_better = 'higher_better'
    lower_better = 'lower_better'


class EvalCaseSource:
    manual = 'manual'
    import_ = 'import'
    conversation = 'conversation'


class EvalRunTrigger:
    offline_batch = 'offline_batch'
    on_demand = 'on_demand'


class EvalRunStatus:
    created = 'created'
    running = 'running'
    finished = 'finished'
    errored = 'errored'
    # Stopped on request. Distinct from `errored` so an intentional stop is not read as a
    # failure of the agent or the rubric: a 50-case run can take hours, and without this the
    # only way out was to abandon a row that reads as "in progress" forever.
    cancelled = 'cancelled'


class EvalResultStatus:
    ok = 'ok'
    error = 'error'
    pending_human = 'pending_human'


# ----------------------------------------------------------------------------
# Library — reusable definitions (§3.1, §16.2)
# ----------------------------------------------------------------------------

class EvalDimension(db_tools.AbstractBaseMixin, db.Base):
    """A reusable dimension *definition* (the 'what & how'). Weight/target/evidence-scope
    live on the binding (§16.2); engine choice (AI/Human) also lives on the binding where
    the definition allows both. A Code-engine definition carries a script instead of a
    rubric — the judging instrument swaps with engine, everything else about the entity
    (weight/target/scale/scorecard row) is shared (§2.1)."""
    __tablename__ = 'eval_dimension'
    __table_args__ = (
        UniqueConstraint('tier', 'name', name='_eval_dimension_tier_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    tier: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalTier.project, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)  # rubric = the judge prompt (§18); unused when engine=code

    # Owning agent for tier=agent_adhoc dimensions: nullable so pre-existing rows (created before
    # this column existed) stay visible everywhere rather than becoming orphaned/hidden.
    # Code-engine dimensions never carry tier=agent_adhoc (§16.5) so agent_id stays null for them.
    agent_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{Application.__tablename__}.id', ondelete='CASCADE'),
        nullable=True, index=True,
    )

    # allowed engines this definition permits: ["ai"], ["human"], ["ai", "human"], or ["code"].
    # Code is exclusive of ai/human on a single definition — a rubric and a script aren't
    # reinterpretations of each other (§2.1) — enforced at create/update time, not by the schema.
    allowed_engines: Mapped[list] = mapped_column(JSONB, nullable=False, default=lambda: [EvalEngine.ai])

    # Folded from the former EvalCodeValidation entity (§2.1). Populated only when
    # allowed_engines == ["code"]; null for AI/Human dimensions.
    code: Mapped[str] = mapped_column(Text, nullable=True)  # python body; returns `result` (bool/number)
    return_contract: Mapped[str] = mapped_column(String(16), nullable=True)  # 'bool' -> pass/fail, 'number' -> numeric (§19.4)

    scale_type: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalScaleType.continuous)
    scale_min: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    scale_max: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    polarity: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalPolarity.higher_better)

    # seed values copied onto a binding at clone time (§16.2)
    default_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    default_target: Mapped[float] = mapped_column(Float, nullable=True)
    default_target_operator: Mapped[str] = mapped_column(String(8), nullable=True)  # '>=', '>', ...

    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


# ----------------------------------------------------------------------------
# Binding — library items applied to an agent + version (§3.2, §16.2)
# ----------------------------------------------------------------------------

class EvalSuite(db_tools.AbstractBaseMixin, db.Base):
    """A named binding set on an agent. Holds run-time knobs; the per-dimension
    weight/target/engine/evidence live on EvalBinding rows."""
    __tablename__ = 'eval_suite'
    __table_args__ = (
        UniqueConstraint('application_id', 'name', name='_eval_suite_app_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    application_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{Application.__tablename__}.id', ondelete='CASCADE'),
        nullable=False, index=True,
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False, default='Default suite')

    dataset_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_dataset.id', ondelete='SET NULL'), nullable=True,
    )
    # optional per-suite judge model override (§18.7); null -> project low-tier default
    judge_model: Mapped[dict] = mapped_column(JSONB, nullable=True)
    # Baseline pointer (§21.6) — names a run id, not a copy. Deliberately *not* a foreign key:
    # a comparison baseline is advisory, and an FK here would make purging an old run either fail
    # or silently rewrite live suite config. Readers must therefore tolerate a dangling id and
    # treat "run not found" as "no baseline" rather than an error.
    baseline_run_id: Mapped[int] = mapped_column(Integer, nullable=True)
    trigger_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    bindings: Mapped[List['EvalBinding']] = relationship(
        back_populates='suite', lazy='selectin', cascade='all, delete-orphan',
        order_by='EvalBinding.order_index, EvalBinding.id',
    )


class EvalBinding(db_tools.AbstractBaseMixin, db.Base):
    """One library item (dimension | platform) applied within a suite, pinned to a
    concrete ApplicationVersion (H6 versioning seam). Binding values override definition
    defaults (§16.2). Exactly one of dimension_id / platform_key is set — a dimension may
    be AI/Human/Code engine (§2.1); code validations are no longer a separate slot."""
    __tablename__ = 'eval_binding'
    # A library item may only be bound once per suite: a duplicate binding scores the same
    # criterion twice, silently doubling its weight in the headline. Postgres treats NULLs as
    # distinct, so each constraint only bites on the column that is actually set.
    __table_args__ = (
        UniqueConstraint('suite_id', 'dimension_id', name='_eval_binding_suite_dimension_uc'),
        UniqueConstraint('suite_id', 'platform_key', name='_eval_binding_suite_platform_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    suite_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_suite.id', ondelete='CASCADE'),
        nullable=False, index=True,
    )
    suite: Mapped['EvalSuite'] = relationship(back_populates='bindings', lazy=True)

    # version pin (§16.3 SUITE → Agent + version); the binding applies to this version
    application_version_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{ApplicationVersion.__tablename__}.id', ondelete='CASCADE'),
        nullable=True, index=True,
    )

    # exactly one source of the item being bound
    dimension_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_dimension.id', ondelete='CASCADE'), nullable=True,
    )
    platform_key: Mapped[str] = mapped_column(String(128), nullable=True)  # platform validation catalog key

    engine: Mapped[str] = mapped_column(String(16), nullable=False, default=EvalEngine.ai)
    # evidence scope the judge/code sees:
    # {"structure": bool, "input": bool, "output": bool, "expected": bool}
    evidence_scope: Mapped[dict] = mapped_column(
        JSONB, nullable=False,
        default=lambda: {'structure': False, 'input': True, 'output': True},
    )
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    target: Mapped[float] = mapped_column(Float, nullable=True)
    target_operator: Mapped[str] = mapped_column(String(8), nullable=True)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


# ----------------------------------------------------------------------------
# Dataset (§17.1)
# ----------------------------------------------------------------------------

class EvalDataset(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_dataset'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    # Owning agent (#6350): a dataset is authored in the context of one agent. Nullable only to
    # tolerate any pre-decision rows created before this column existed — new datasets always set it.
    agent_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{Application.__tablename__}.id', ondelete='CASCADE'),
        nullable=True, index=True,
    )
    # Opt-in (#6350): when true, selectable from any agent's suite config in the project (§13);
    # when false, only the owning agent's suite config can pick it.
    is_shared: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    cases: Mapped[List['EvalDatasetCase']] = relationship(
        back_populates='dataset', lazy='selectin', cascade='all, delete-orphan',
        order_by='EvalDatasetCase.order_index',
    )


class EvalDatasetCase(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_dataset_case'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_dataset.id', ondelete='CASCADE'),
        nullable=False, index=True,
    )
    dataset: Mapped['EvalDataset'] = relationship(back_populates='cases', lazy=True)

    order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    input: Mapped[str] = mapped_column(Text, nullable=False)
    variables: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    expected_output: Mapped[str] = mapped_column(Text, nullable=True)  # present -> reference-based
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalCaseSource.manual)
    source_ref: Mapped[str] = mapped_column(String(256), nullable=True)  # e.g. originating conversation_id
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


# ----------------------------------------------------------------------------
# Run (immutable snapshot) + results (§3.4, §20.10)
# ----------------------------------------------------------------------------

class EvalRun(db_tools.AbstractBaseMixin, db.Base):
    """Immutable snapshot of one evaluation execution. ``snapshot`` freezes the suite
    config + dimension definitions (any engine) + bindings + dataset case set + scale
    specs at run time so later edits never mutate history (§3.4)."""
    __tablename__ = 'eval_run'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)

    suite_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_suite.id', ondelete='SET NULL'), nullable=True, index=True,
    )
    application_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    # frozen version under evaluation (D3, §21.6)
    application_version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    dataset_id: Mapped[int] = mapped_column(Integer, nullable=True)

    trigger_type: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalRunTrigger.offline_batch)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=EvalRunStatus.created, index=True)

    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # normalized weighted headline (§20); server-computed, refreshed on human-score writes
    headline_score: Mapped[float] = mapped_column(Float, nullable=True)
    progress: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)  # {"done": n, "total": m}
    error: Mapped[str] = mapped_column(Text, nullable=True)

    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    results: Mapped[List['EvalResult']] = relationship(
        back_populates='run', lazy='dynamic', cascade='all, delete-orphan',
    )


class EvalResult(db_tools.AbstractBaseMixin, db.Base):
    """One case × one validation verdict inside a run. References the frozen case and
    dimension **by id value** (not FK) so editing/deleting the source never corrupts a
    finished run. Stores both native and normalized scores (§20.10)."""
    __tablename__ = 'eval_result'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_run.id', ondelete='CASCADE'),
        nullable=False, index=True,
    )
    run: Mapped['EvalRun'] = relationship(back_populates='results', lazy=True)

    # frozen alignment keys (§21: align by case id + dimension definition id)
    dataset_case_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)
    dimension_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)
    platform_key: Mapped[str] = mapped_column(String(128), nullable=True)

    engine: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=EvalResultStatus.ok)

    native_score: Mapped[float] = mapped_column(Float, nullable=True)
    normalized_score: Mapped[float] = mapped_column(Float, nullable=True)
    # rationale / pass-fail / code stdout+stderr+status / judge raw envelope
    verdict: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # frozen evidence actually shown to the judge/code: structure/input/output
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


# ----------------------------------------------------------------------------
# Human scores — append-only annotation layer (§3.4, §15.6, D2)
# ----------------------------------------------------------------------------

class EvalHumanScore(db_tools.AbstractBaseMixin, db.Base):
    """Append-only human annotation on a case × dimension of a run. Every write is a new
    row; aggregate reads the latest by created_at (D2 = append-only, not overwrite). No
    updated_at by design — rows are never mutated, preserving the audit trail (§15.6)."""
    __tablename__ = 'eval_human_score'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.eval_run.id', ondelete='CASCADE'),
        nullable=False, index=True,
    )
    dataset_case_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)
    dimension_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)

    reviewer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    native_score: Mapped[float] = mapped_column(Float, nullable=True)
    normalized_score: Mapped[float] = mapped_column(Float, nullable=True)
    note: Mapped[str] = mapped_column(Text, nullable=True)
    # supersedes bookkeeping is implicit via created_at ordering; kept for fast "latest" reads
    is_latest: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
