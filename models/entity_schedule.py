"""Schedule tables; webhook rows ship via the `ensure_webhook_trigger_columns` admin task, toolkit index rows via
`ensure_toolkit_index_schedule_columns`."""
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
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

ACTIVE_STATUSES = ('running', 'waiting_for_approval')
ACTIVE_RUN_STATUSES = 'status IN (' + ', '.join(f"'{status}'" for status in ACTIVE_STATUSES) + ')'
# A webhook may have several active runs; its caps are counted in code (#7400)
ONE_ACTIVE_RUN = f"{ACTIVE_RUN_STATUSES} AND trigger <> 'webhook'"


class EntitySchedule(db_tools.AbstractBaseMixin, db.Base):
    """One trigger of an agent or pipeline version, or one cron schedule of a toolkit index, owned by `created_by`.

    Webhook rows (`trigger_kind='webhook'`) have no `cron`, `timezone` or `next_run`. A toolkit index row
    (`entity_type='toolkit_index'`) names a toolkit and an index and has no application, version or run input.
    """
    __tablename__ = 'entity_schedules'
    __table_args__ = (
        CheckConstraint("entity_type IN ('agent', 'pipeline', 'toolkit_index')", name='ck_entity_schedules_entity_type'),
        CheckConstraint(
            "(entity_type <> 'toolkit_index' AND application_id IS NOT NULL AND toolkit_id IS NULL"
            " AND index_name IS NULL)"
            " OR (entity_type = 'toolkit_index' AND toolkit_id IS NOT NULL AND index_name IS NOT NULL"
            " AND application_id IS NULL AND version_id IS NULL AND run_mode IS NULL AND prompt IS NULL"
            " AND trigger_kind = 'cron')",
            name='ck_entity_schedules_target',
        ),
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
        CheckConstraint("trigger_kind IN ('cron', 'webhook')", name='ck_entity_schedules_trigger_kind'),
        CheckConstraint(
            "trigger_kind = 'webhook' OR (cron IS NOT NULL AND timezone IS NOT NULL)",
            name='ck_entity_schedules_cron_fields',
        ),
        CheckConstraint(
            "trigger_kind <> 'webhook' OR next_run IS NULL", name='ck_entity_schedules_webhook_no_next_run'
        ),
        Index('uq_entity_schedules_uuid', 'uuid', unique=True),
        # One schedule per index: the Schedules page replaces the per-index schedules of the toolkit settings
        Index(
            'uq_entity_schedules_toolkit_index', 'toolkit_id', 'index_name', unique=True,
            postgresql_where=text('toolkit_id IS NOT NULL'),
        ),
        Index(
            'uq_entity_schedules_legacy_version', 'legacy_version_id', unique=True,
            postgresql_where=text('legacy_version_id IS NOT NULL'),
        ),
        Index(
            'uq_entity_schedules_legacy_application', 'legacy_application_id', unique=True,
            postgresql_where=text('legacy_application_id IS NOT NULL'),
        ),
        Index('ix_entity_schedules_credential_refs', 'credential_refs', postgresql_using='gin'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # The stable key of the YAML file; the SQL backfill uses gen_random_uuid()
    uuid: Mapped[UUID] = mapped_column(Uuid, nullable=False, default=uuid4)
    entity_type: Mapped[str] = mapped_column(String(16), nullable=False)
    # NULL on a toolkit index row, which names `toolkit_id` and `index_name` instead
    application_id: Mapped[int | None] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.applications.id', ondelete='CASCADE'), nullable=True
    )
    toolkit_id: Mapped[int | None] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.elitea_tools.id', ondelete='CASCADE'), nullable=True
    )
    index_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    version_id: Mapped[int | None] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.application_versions.id', ondelete='SET NULL'), nullable=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(String(2304), nullable=True)
    trigger_kind: Mapped[str] = mapped_column(
        String(16), nullable=False, default='cron', server_default='cron'
    )
    cron: Mapped[str | None] = mapped_column(String(128), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
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
    webhook_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    gitlab_auth_method: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # Vault references (hidden project secrets), never the values
    secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    signing_secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_secret_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_secret_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Time and outcome of the last authentic delivery (accepted, duplicate, ping, disabled, refused, start_failed)
    last_delivery_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_delivery_outcome: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # The old URL key of a migrated legacy webhook (#7402)
    legacy_version_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    legacy_application_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # configuration_uuids the version uses, written by the Connections check (#7401)
    credential_refs: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # The legacy version trigger a migrated row replaced, read back by restore_version_trigger; NULL on rows moved before the column existed
    original_trigger: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # Governance refusals in a row since the last admitted governed start or resume (#7254); NULL counts as 0. A table
    # column only, not mapped (`exclude_properties`): no ORM INSERT or SELECT names it, so a schema that predates it
    # (the startup safety net adds it) still creates, imports and reads schedules. Written by Core updates on
    # `REFUSAL_STREAK` only: the refusal count, the end of a streak and Resume
    refusal_streak = mapped_column(SmallInteger, nullable=True)
    __mapper_args__ = {'exclude_properties': ['refusal_streak']}  # noqa: RUF012 -- SQLAlchemy reads it
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


    def to_json(self, exclude_fields: tuple = ()) -> dict:
        """The mixin's row dict (and repr) without the unmapped refusal count, which the row does not hold."""
        return super().to_json((*exclude_fields, 'refusal_streak'))

# The unmapped refusal count of a schedule (#7254), for Core statements
REFUSAL_STREAK = EntitySchedule.__table__.c.refusal_streak


class EntityScheduleRun(db_tools.AbstractBaseMixin, db.Base):
    """One run of a schedule; at most one active run per cron schedule."""
    __tablename__ = 'entity_schedule_runs'
    __table_args__ = (
        Index(
            'uq_entity_schedule_runs_active', 'schedule_id', unique=True,
            postgresql_where=text(ONE_ACTIVE_RUN), sqlite_where=text(ONE_ACTIVE_RUN),
        ),
        # Serves the runs list: one schedule, newest first
        Index('ix_entity_schedule_runs_schedule_started', 'schedule_id', desc('started_at'), desc('id')),
        # Serves the conversation list filtered by schedule
        Index('ix_entity_schedule_runs_conversation', 'conversation_id'),
        # Serves the conversation list filtered by version
        Index('ix_entity_schedule_runs_version', 'version_id'),
        # One active Test Run per owner and agent or pipeline; a Test Run has no schedule
        Index(
            'uq_entity_schedule_runs_active_test', 'requested_by', 'application_id', unique=True,
            postgresql_where=text(f'schedule_id IS NULL AND {ACTIVE_RUN_STATUSES}'),
            sqlite_where=text(f'schedule_id IS NULL AND {ACTIVE_RUN_STATUSES}'),
        ),
        # Serves the Test Run list of one owner: newest first
        Index(
            'ix_entity_schedule_runs_test_owner', 'requested_by', 'application_id', desc('started_at'), desc('id'),
            postgresql_where=text('schedule_id IS NULL'), sqlite_where=text('schedule_id IS NULL'),
        ),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # NULL for a Test Run, which runs the unsaved form and belongs to no schedule
    schedule_id: Mapped[int | None] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{EntitySchedule.__tablename__}.id', ondelete='CASCADE'),
        nullable=True,
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
    # The unsaved schedule form and the agent or pipeline of a Test Run. Table columns only, not mapped
    # (`exclude_properties`): no ORM INSERT or SELECT names them, so a schema that predates them (the admin task
    # adds them) still creates and reads saved runs. Core statements use `RUN_FORM` and `RUN_APPLICATION_ID`
    form = mapped_column(JSONB, nullable=True)
    application_id = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.applications.id', ondelete='CASCADE'), nullable=True
    )
    __mapper_args__ = {'exclude_properties': ['form', 'application_id']}  # noqa: RUF012 -- SQLAlchemy reads it

    def to_json(self, exclude_fields: tuple = ()) -> dict:
        """The mixin's row dict without the unmapped Test Run columns, which the row does not hold."""
        return super().to_json((*exclude_fields, 'form', 'application_id'))


# The unmapped Test Run columns, for Core statements
RUN_FORM = EntityScheduleRun.__table__.c.form
RUN_APPLICATION_ID = EntityScheduleRun.__table__.c.application_id


class EntitySchedulePreference(db_tools.AbstractBaseMixin, db.Base):
    """Per-user schedule notification preference."""
    __tablename__ = 'entity_schedule_preferences'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    get_notified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=true())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
