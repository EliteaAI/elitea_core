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
    BigInteger, CheckConstraint, DateTime, Index, Integer, SmallInteger, String, UniqueConstraint, event, func,
    inspect, text,
)

IDENTITY_MODES = ('dedicated_agent', 'runtime_domain_binding', 'elitea_native')
BINDING_STATES = ('draft', 'provisioning', 'pending_approval', 'active', 'suspended', 'retiring', 'retired')
CONTEXT_STATES = ('admitted', 'running', 'completed', 'revoked')
MANDATE_STATES = ('draft', 'pending_approval', 'active', 'suspended', 'expired', 'revoked')
LIVE_MANDATE_STATES = ('draft', 'pending_approval', 'active', 'suspended')
SUSPENDED_REASONS = ('MANUAL', 'OWNER_SUSPENDED')
# approval_ref and policy_revision of a binding approval: printable reference text, no spaces
REFERENCE_PATTERN = r'^[A-Za-z0-9._:/@-]{1,128}$'
# Columns that never change after the row exists; ``approved_by`` is set once (a second fence behind the
# conditional UPDATEs in ``utils/mandate_state.py``, which are the only writers of the state columns)
MANDATE_IMMUTABLE = frozenset({
    'id', 'project_id', 'application_id', 'agent_version', 'agent_binding_ref', 'owner_user_id', 'owner_user_ref',
    'allowed_operations', 'allowed_targets', 'max_runtime_s', 'valid_until', 'revision', 'created_by', 'created_at',
    'identity_source',
})


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
    # What the agent may do on the platform on the user's behalf through the internal Elitea MCP (#7214): a subset of
    # ``utils.internal_obo.OPERATIONS``; NULL or [] = ``read`` only. Part of what is approved: set at create or rebind.
    # Deferred: loading a binding never selects it. The run token's scope reads it explicitly, so in a schema that predates
    # it (until the startup safety net adds it) a governed run with internal Elitea tools is refused (fail closed).
    allowed_platform_operations: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True, deferred=True)


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
    # ``governed``: admitted on an active binding; ``development`` (#7213): the editor's test chat of an unbound
    # version, admitted on the author's own identity and credentials, with no binding (``agent_binding_ref`` is NULL)
    profile: Mapped[str] = mapped_column(String(16), nullable=False, default='governed', server_default='governed')
    policy_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    tenant: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    environment: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    binding_project_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    entry_point: Mapped[str] = mapped_column(String(32), nullable=False)
    caller: Mapped[dict] = mapped_column(JSONB, nullable=False)
    subject: Mapped[dict] = mapped_column(JSONB, nullable=False)
    agent_binding_ref: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    agent_session_ref: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # None for a toolkit run (#7229): it tests or indexes one toolkit, no agent version
    application_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    version_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    definition_digest: Mapped[str] = mapped_column(String(80), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default='1')
    # A sub-agent admitted with its parent run (#7227): the context of its caller, and every ancestor context (the run's
    # first). Unset for a run
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


class ImmutableFieldChanged(ValueError):
    """An ORM flush tried to change a column that must stay as it was written."""


class AutonomousMandate(db_tools.AbstractBaseMixin, db.Base):
    """An approved, time-boxed permission for one agent version to run without a signed-in user (#6947, C-1009).

    Scope is never edited: changing it means revoking the mandate and creating the next revision. The row
    holds references and limits only, no credential.
    """
    __tablename__ = 'autonomous_mandate'
    __table_args__ = (
        UniqueConstraint('project_id', 'agent_version', 'revision', name='autonomous_mandate_revision_uc'),
        CheckConstraint(_in_list('state', MANDATE_STATES), name='autonomous_mandate_state_ck'),
        CheckConstraint('suspended_reason IS NULL OR ' + _in_list('suspended_reason', SUSPENDED_REASONS),
                        name='autonomous_mandate_suspended_reason_ck'),
        CheckConstraint("state NOT IN ('active', 'suspended') OR "
                        "(approved_by IS NOT NULL AND approval_ref IS NOT NULL AND policy_revision IS NOT NULL)",
                        name='autonomous_mandate_active_ck'),
        CheckConstraint('approved_by IS NULL OR (approved_by <> created_by AND approved_by <> owner_user_id)',
                        name='autonomous_mandate_approver_ck'),
        CheckConstraint('jsonb_array_length(allowed_operations) >= 1', name='autonomous_mandate_operations_ck'),
        CheckConstraint('max_runtime_s BETWEEN 60 AND 86400', name='autonomous_mandate_runtime_ck'),
        # One live mandate per agent version. ``sqlite_where`` only lets the unit tests run this on SQLite.
        Index('uq_autonomous_mandate_live_version', 'project_id', 'agent_version', unique=True,
              postgresql_where=text(_in_list('state', LIVE_MANDATE_STATES)),
              sqlite_where=text(_in_list('state', LIVE_MANDATE_STATES))),
        Index('ix_autonomous_mandate_owner_state', 'owner_user_id', 'state'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    application_id: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_version: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_binding_ref: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    owner_user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_user_ref: Mapped[dict] = mapped_column(JSONB, nullable=False)
    allowed_operations: Mapped[list] = mapped_column(JSONB, nullable=False)
    allowed_targets: Mapped[list] = mapped_column(JSONB, nullable=False)
    max_runtime_s: Mapped[int] = mapped_column(Integer, nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default='draft', server_default='draft')
    suspended_reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    approval_ref: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    policy_revision: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    # active_history: the old value is loaded when it is replaced, so "set once" holds for an expired attribute too
    approved_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, active_history=True)
    state_changed_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    revocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default='0')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Which identity the mandate's runs act with on resources that need a person (#7131): NULL = none named (today's
    # behaviour: stored secrets, application credentials included; a delegated credential is refused), ``owner_delegated``
    # (the owner's Microsoft sign-in, through their offline consent) or ``application`` (named for the record: the runs
    # use application credentials, as with none). Part of the approved scope: set at create, never changed.
    # ``identity_material_ref``: the owner's consent kept by the secrets plugin (NULL = none yet, or withdrawn);
    # ``identity_consented_at``: when it was given. Deferred: a mandate read never selects them; they are read only
    # where the deployment allows an identity (``mandate_identity_sources``), so a schema that predates them (until the
    # startup safety net adds them) behaves as before.
    identity_source: Mapped[Optional[str]] = mapped_column(String(24), nullable=True, deferred=True)
    identity_material_ref: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True, deferred=True)
    identity_consented_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True,
                                                                     deferred=True)


@event.listens_for(AutonomousMandate, 'before_update')
def _guard_mandate(_mapper, _connection, target):
    attrs = inspect(target).attrs
    for name in MANDATE_IMMUTABLE:
        if attrs[name].history.has_changes():
            raise ImmutableFieldChanged(f"AutonomousMandate.{name} cannot be changed")
    if any(old is not None for old in attrs.approved_by.history.deleted):
        raise ImmutableFieldChanged("AutonomousMandate.approved_by is set once")
