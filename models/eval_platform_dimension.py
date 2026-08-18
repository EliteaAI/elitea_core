"""Registry of platform-wide evaluation dimensions (§16.1).

Lives in the shared schema so the admin console can list and edit the whole catalog in a
single query. Each row is *projected* into every project schema as an ordinary
``p_<id>.eval_dimension`` row with ``tier='platform'``, correlated by ``uuid`` — that
projected row is what ``eval_binding.dimension_id`` points at, so every existing binding,
snapshot, judge and scoring path keeps working untouched. See
``utils/eval_platform_dimension_utils.py``.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Integer, String, Text, DateTime, Float, Boolean, func, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from tools import db_tools, db, config as c


class EvalPlatformDimension(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'eval_platform_dimension'
    # name is unique because the projected row lands under eval_dimension's ('tier', 'name')
    # unique constraint — two platform dimensions sharing a name could not both project.
    __table_args__ = (
        UniqueConstraint('name', name='_eval_platform_dimension_name_uc'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)

    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)  # rubric = the judge prompt (§18)

    # Same meaning as eval_dimension.allowed_engines: which engines may score this dimension.
    allowed_engines: Mapped[list] = mapped_column(JSONB, nullable=False, default=lambda: ['ai'])

    scale_type: Mapped[str] = mapped_column(String(32), nullable=False, default='continuous')
    scale_min: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    scale_max: Mapped[float] = mapped_column(Float, nullable=False, default=100.0)
    polarity: Mapped[str] = mapped_column(String(32), nullable=False, default='higher_better')

    default_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    default_target: Mapped[float] = mapped_column(Float, nullable=True)
    default_target_operator: Mapped[str] = mapped_column(String(8), nullable=True)

    # Soft delete only: eval_binding.dimension_id cascades on delete, so dropping a projected
    # row would silently delete bindings across every project.
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True, onupdate=func.now()
    )
