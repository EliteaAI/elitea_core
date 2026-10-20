"""Agent identity binding and the per-run execution security context (#6756, C-1009).

Nothing reads these tables yet: the binding is the durable record of which definition of an
agent a security decision was made for, and the context row is what one admitted run is
validated against. The context holds references only - never a token, cookie or secret.
"""
import uuid
from datetime import datetime
from typing import Optional

from tools import db_tools, db, config as c
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, Index, Integer, SmallInteger, String, UniqueConstraint, func, text,
)

IDENTITY_MODES = ('dedicated_agent', 'runtime_domain_binding', 'elitea_native')
BINDING_STATES = ('draft', 'provisioning', 'pending_approval', 'active', 'suspended', 'retiring', 'retired')
CONTEXT_STATES = ('admitted', 'running', 'completed', 'revoked')


def _in_list(column: str, values) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class AgentIdentityBinding(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'agent_identity_binding'
    __table_args__ = (
        UniqueConstraint('project_id', 'application_id', 'version_id', 'identity_mode',
                         name='agent_identity_binding_uc'),
        CheckConstraint(_in_list('identity_mode', IDENTITY_MODES), name='agent_identity_binding_mode_ck'),
        CheckConstraint(_in_list('state', BINDING_STATES), name='agent_identity_binding_state_ck'),
        CheckConstraint("state <> 'active' OR (approval_ref IS NOT NULL AND policy_revision IS NOT NULL)",
                        name='agent_identity_binding_active_ck'),
        Index('ix_agent_identity_binding_app_version', 'application_id', 'version_id'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    identity_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    definition_digest: Mapped[str] = mapped_column(String(80), nullable=False)
    external_agent_identity_ref: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    owner_user_ref: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    policy_revision: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    approval_ref: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default='draft', server_default='draft')
    revocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default='0')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    state_changed_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class ExecutionSecurityContext(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'execution_security_context'
    __table_args__ = (
        UniqueConstraint('run_id', 'attempt', name='execution_security_context_run_attempt_uc'),
        CheckConstraint(_in_list('state', CONTEXT_STATES), name='execution_security_context_state_ck'),
        Index('ix_execution_security_context_state_expires', 'state', 'expires_at'),
        Index('ix_execution_security_context_binding', 'agent_binding_ref', 'state'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1, server_default='1')
    profile: Mapped[str] = mapped_column(String(16), nullable=False, default='governed', server_default='governed')
    policy_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    tenant: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    environment: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    binding_project_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    entry_point: Mapped[str] = mapped_column(String(32), nullable=False)
    caller: Mapped[dict] = mapped_column(JSONB, nullable=False)
    subject: Mapped[dict] = mapped_column(JSONB, nullable=False)
    agent_binding_ref: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    agent_session_ref: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    definition_digest: Mapped[str] = mapped_column(String(80), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default='1')
    parent_context_ref: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    call_lineage: Mapped[list] = mapped_column(JSONB, nullable=False, default=list,
                                               server_default=text("'[]'::jsonb"))
    allowed_bindings: Mapped[list] = mapped_column(JSONB, nullable=False, default=list,
                                                   server_default=text("'[]'::jsonb"))
    revocation_epochs: Mapped[dict] = mapped_column(JSONB, nullable=False)
    trace_ref: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default='admitted', server_default='admitted')
    revoked_reason: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
