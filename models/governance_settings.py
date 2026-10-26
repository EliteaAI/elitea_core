"""The platform-wide credential governance switch and its history (#7120, C-1009).

One row per scope holds the mode (``off``, ``audit_only``, ``enforce``), the policy revision and the admission flags
(``allow_development_runs``, #7213, is NULL until an operator chooses: the default then follows the mode). Slice 1 has the platform scope only: the CHECK on ``scope_kind``/``scope_id`` is the seam for per-project
overrides later (drop the CHECK, add ``project`` rows, no column change).

These models are NOT on ``db.Base``: ``shared`` runs ``create_all`` on it at boot and this change must not create tables
by itself. They have a private ``MetaData`` and are created only by the ``create_governance_settings_tables`` admin
task; a table that does not exist reads as "no stored row".
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, Integer, MetaData, SmallInteger, String, UniqueConstraint, func,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from tools import config as c

MODES = ('off', 'audit_only', 'enforce')
SCOPE_PLATFORM = 'platform'
SETTINGS_TABLE = 'credential_governance_settings'
HISTORY_TABLE = 'credential_governance_settings_history'

METADATA = MetaData(schema=c.POSTGRES_SCHEMA)
Base = declarative_base(metadata=METADATA)

# SQLite (the unit tests) only auto-numbers a plain INTEGER primary key
_SMALL_ID = SmallInteger().with_variant(Integer(), 'sqlite')
_BIG_ID = BigInteger().with_variant(Integer(), 'sqlite')


class CredentialGovernanceSettings(Base):
    __tablename__ = SETTINGS_TABLE
    __table_args__ = (
        UniqueConstraint('scope_kind', 'scope_id', name='credential_governance_settings_scope_uc'),
        CheckConstraint("mode IN ('off', 'audit_only', 'enforce')", name='credential_governance_settings_mode_ck'),
        CheckConstraint("scope_kind = 'platform' AND scope_id = 0", name='credential_governance_settings_scope_ck'),
        CheckConstraint("mode = 'off' OR policy_revision IS NOT NULL",
                        name='credential_governance_settings_revision_ck'),
    )

    id: Mapped[int] = mapped_column(_SMALL_ID, primary_key=True, autoincrement=True)
    scope_kind: Mapped[str] = mapped_column(String(16), nullable=False, default=SCOPE_PLATFORM,
                                            server_default=SCOPE_PLATFORM)
    scope_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default='0')
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    policy_revision: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    admit_legacy_pat: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default='false')
    allow_runtime_domain_binding: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                                               server_default='false')
    allow_development_runs: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    environment: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default='1')
    updated_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class CredentialGovernanceSettingsHistory(Base):
    __tablename__ = HISTORY_TABLE
    __table_args__ = (
        UniqueConstraint('scope_kind', 'scope_id', 'revision', name='credential_governance_settings_history_uc'),
    )

    id: Mapped[int] = mapped_column(_BIG_ID, primary_key=True, autoincrement=True)
    scope_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    scope_id: Mapped[int] = mapped_column(Integer, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    policy_revision: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    admit_legacy_pat: Mapped[bool] = mapped_column(Boolean, nullable=False)
    allow_runtime_domain_binding: Mapped[bool] = mapped_column(Boolean, nullable=False)
    allow_development_runs: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    environment: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    change_note: Mapped[str] = mapped_column(String(256), nullable=False)
    changed_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


TABLES = (CredentialGovernanceSettings, CredentialGovernanceSettingsHistory)
# Columns added after the tables first shipped: the admin task adds them to an existing table (#7213)
ADDED_COLUMNS = (("allow_development_runs", "BOOLEAN NULL"),)
