"""
AuditEvent model for read-only analytics and audit trail queries.

The authoritative model and table creation is handled by the tracing plugin
(which owns the write path). This definition uses extend_existing=True to
safely coexist when both plugins are loaded in the same process.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, SmallInteger, Float, Boolean, func, Index
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class AuditEvent(db.Base):
    __tablename__ = 'audit_events'
    __table_args__ = (
        Index('ix_audit_events_timestamp', 'timestamp'),
        Index('ix_audit_events_user_id', 'user_id'),
        Index('ix_audit_events_project_id', 'project_id'),
        Index('ix_audit_events_trace_id', 'trace_id'),
        Index('ix_audit_events_entity', 'entity_type', 'entity_id'),
        {'schema': c.POSTGRES_SCHEMA, 'extend_existing': True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # Who
    user_id: Mapped[int] = mapped_column(Integer, nullable=True)
    user_email: Mapped[str] = mapped_column(String(256), nullable=True)

    # Where
    project_id: Mapped[int] = mapped_column(Integer, nullable=True)

    # What
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(512), nullable=False)
    http_method: Mapped[str] = mapped_column(String(10), nullable=True)
    http_route: Mapped[str] = mapped_column(String(512), nullable=True)

    # Result
    status_code: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    duration_ms: Mapped[float] = mapped_column(Float, nullable=True)
    is_error: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Entity context (what was acted upon)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=True)
    entity_name: Mapped[str] = mapped_column(String(256), nullable=True)

    # Agent/tool details
    tool_name: Mapped[str] = mapped_column(String(256), nullable=True)
    model_name: Mapped[str] = mapped_column(String(256), nullable=True)

    # Trace linkage
    trace_id: Mapped[str] = mapped_column(String(32), nullable=True)
    span_id: Mapped[str] = mapped_column(String(16), nullable=True)
    parent_span_id: Mapped[str] = mapped_column(String(16), nullable=True)
