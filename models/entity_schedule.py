"""Schedule tables (EL-7011). Keep in step with the R-2.0.8 block of db_migrations.txt."""
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Uuid,
    desc,
    func,
    text,
    true,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from . import config as c
from . import db, db_tools

ACTIVE_RUN_STATUSES = "status IN ('running', 'waiting_for_approval')"


class EntitySchedule(db_tools.AbstractBaseMixin, db.Base):
    """One schedule of an agent or pipeline version, owned by `created_by`."""
    __tablename__ = 'entity_schedules'
    __table_args__ = (
        CheckConstraint("entity_type IN ('agent', 'pipeline')", name='ck_entity_schedules_entity_type'),
        CheckConstraint(
            "run_mode IS NULL OR run_mode IN ('entry_point', 'prompt')", name='ck_entity_schedules_run_mode'
        ),
        CheckConstraint(
            "principal->>'kind' IS NOT NULL"
            " AND principal->>'kind' IN ('mandate', 'delegation', 'legacy_system')",
            name='ck_entity_schedules_principal_kind',
        ),
        Index('ix_entity_schedules_next_run_enabled', 'next_run', postgresql_where=text('enabled')),
        Index('ix_entity_schedules_created_by', 'created_by'),
        Index('ix_entity_schedules_application_version', 'application_id', 'version_id'),
        Index('uq_entity_schedules_uuid', 'uuid', unique=True),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # The stable key of the YAML file; the SQL backfill uses gen_random_uuid()
    uuid: Mapped[UUID] = mapped_column(Uuid, nullable=False, default=uuid4)
    entity_type: Mapped[str] = mapped_column(String(16), nullable=False)
    application_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.applications.id', ondelete='CASCADE'), nullable=False
    )
    version_id: Mapped[int | None] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.application_versions.id', ondelete='SET NULL'), nullable=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(String(2304), nullable=True)
    cron: Mapped[str] = mapped_column(String(128), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    run_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    variables: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=true())
    disabled_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    approval_rule: Mapped[str] = mapped_column(
        String(32), nullable=False, default='pause_and_ask', server_default='pause_and_ask'
    )
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    principal: Mapped[dict] = mapped_column(JSONB, nullable=False)
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_run_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_conversation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Who confirmed "Run without asking", when, and the fingerprint of the sensitive tools they saw
    approval_confirmed_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approval_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    approval_fingerprint: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The legacy version trigger a migrated row replaced, read back by restore_version_trigger; NULL on rows moved before the column existed
    original_trigger: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # Where a run posts its result: Run History only, or one chat; the target columns stay NULL in run_history
    delivery_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, default='run_history', server_default='run_history'
    )
    delivery_conversation_uuid: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    delivery_participant_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class EntityScheduleRun(db_tools.AbstractBaseMixin, db.Base):
    """One run of a schedule; at most one active run per schedule."""
    __tablename__ = 'entity_schedule_runs'
    __table_args__ = (
        Index(
            'uq_entity_schedule_runs_active', 'schedule_id', unique=True,
            postgresql_where=text(ACTIVE_RUN_STATUSES), sqlite_where=text(ACTIVE_RUN_STATUSES),
        ),
        # Serves the runs list: one schedule, newest first
        Index('ix_entity_schedule_runs_schedule_started', 'schedule_id', desc('started_at'), desc('id')),
        # Serves the conversation list filtered by schedule
        Index('ix_entity_schedule_runs_conversation', 'conversation_id'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    schedule_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{EntitySchedule.__tablename__}.id', ondelete='CASCADE'),
        nullable=False,
    )
    trigger: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    conversation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approval_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    resumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # The run log of the Test schedule panel (owner CR 2026-10-21); NULL on a run made before it
    events: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # The version the run started; no foreign key, so a deleted version keeps its id in the history
    version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    principal_label: Mapped[str | None] = mapped_column(String(256), nullable=True)
    risk_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)


class EntitySchedulePreference(db_tools.AbstractBaseMixin, db.Base):
    """Per-user schedule notification preference."""
    __tablename__ = 'entity_schedule_preferences'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    get_notified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=true())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
