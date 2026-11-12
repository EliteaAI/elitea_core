"""Environment-wide scheduled agent evaluation ("auto-evaluation").

Shared schema, so the admin dashboard reads every project's results in one indexed query rather
than an N-project fan-out:

* ``EvalAutoConfig`` — the single settings row (scope, thresholds, capacity). The cron itself
  stays config-managed (``config.yml`` ``scheduler:`` + the admin schema), like the other
  platform schedules.
* ``EvalAutoJob`` — one tick (scheduled or "Run now").
* ``EvalAutoItem`` — one agent within a job: both the queue (``state``) and the result
  (``status``). The run it launched lives in ``p_<id>.eval_run`` and points back through
  ``eval_run.meta.auto_item_id``.
* ``EvalPlatformDataset`` / ``EvalPlatformDatasetCase`` — the seeded health-check probes,
  copied into a project the first time one of its agents falls through to the platform tier.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Integer, String, Text, DateTime, Float, Boolean, func, ForeignKey, Index, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from tools import db_tools, db, config as c


class EvalAutoItemState:
    pending = 'pending'
    running = 'running'
    done = 'done'


class EvalAutoStatus:
    passed = 'passed'
    warning = 'warning'
    failed = 'failed'
    not_evaluated = 'not_evaluated'


class EvalAutoTrigger:
    scheduled = 'scheduled'
    manual = 'manual'


class EvalAutoConfig(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_auto_config'
    __table_args__ = ({'schema': c.POSTGRES_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Every setting lives in one JSON object, read through ``eval_auto_rules.effective_config``:
    # a new knob needs a default there, never a migration.
    settings: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    updated_by: Mapped[int] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True, onupdate=func.now()
    )


class EvalAutoJob(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_auto_job'
    __table_args__ = ({'schema': c.POSTGRES_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalAutoTrigger.scheduled)
    started_by: Mapped[int] = mapped_column(Integer, nullable=True)
    # The settings the job was enumerated with, so a config edit mid-job is visible as such.
    settings: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    counters: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)


class EvalAutoItem(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_auto_item'
    __table_args__ = (
        UniqueConstraint('job_id', 'project_id', 'application_id', name='_eval_auto_item_job_app_uc'),
        Index('ix_eval_auto_item_latest', 'project_id', 'application_id', 'finished_at'),
        Index('ix_eval_auto_item_state', 'state', 'created_at'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_SCHEMA}.eval_auto_job.id', ondelete='CASCADE'), nullable=False
    )
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    application_version_id: Mapped[int] = mapped_column(Integer, nullable=True)
    agent_name: Mapped[str] = mapped_column(String(256), nullable=True)
    owner_id: Mapped[int] = mapped_column(Integer, nullable=True)

    state: Mapped[str] = mapped_column(String(16), nullable=False, default=EvalAutoItemState.pending)
    status: Mapped[str] = mapped_column(String(16), nullable=True)
    reason_code: Mapped[str] = mapped_column(String(32), nullable=True)
    reason_detail: Mapped[str] = mapped_column(Text, nullable=True)

    eval_run_id: Mapped[int] = mapped_column(Integer, nullable=True)
    suite_id: Mapped[int] = mapped_column(Integer, nullable=True)
    headline_score: Mapped[float] = mapped_column(Float, nullable=True)
    coverage: Mapped[float] = mapped_column(Float, nullable=True)
    dataset_tier: Mapped[str] = mapped_column(String(16), nullable=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default=EvalAutoTrigger.scheduled)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default='live')
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)


class EvalPlatformDataset(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_platform_dataset'
    __table_args__ = (
        UniqueConstraint('name', name='_eval_platform_dataset_name_uc'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True, onupdate=func.now()
    )


class EvalPlatformDatasetCase(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_platform_dataset_case'
    __table_args__ = (
        UniqueConstraint('dataset_id', 'key', name='_eval_platform_dataset_case_key_uc'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_SCHEMA}.eval_platform_dataset.id', ondelete='CASCADE'), nullable=False
    )
    # Stable identity of a seeded probe, so a re-seed updates its text instead of adding a copy.
    key: Mapped[str] = mapped_column(String(64), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    input: Mapped[str] = mapped_column(Text, nullable=False)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
