"""Elitea Builder changesets (#7383): a proposed change, confirmed (when destructive) and applied once."""
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class BuilderChangeset(db.Base):
    __tablename__ = 'builder_changesets'
    __table_args__ = {'schema': c.POSTGRES_TENANT_SCHEMA}

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    op: Mapped[str] = mapped_column(String(16), nullable=False)
    destructive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Hash of the proposed change: an identical pending proposal reuses its changeset (HITL resume re-proposes)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_ts: Mapped[float] = mapped_column(Float, nullable=False)
    confirmed_by: Mapped[int] = mapped_column(Integer, nullable=True)
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


BUILDER_TABLES = (BuilderChangeset,)
