"""
Read-only model for LLM pricing rates used in cost estimation.

Prices are per 1000 tokens. The table is populated via admin tasks or manual
insertion; analytics queries use it to compute llm_cost at query time or at
event-write time via the utility in utils/llm_cost.py.
"""

from sqlalchemy import Integer, String, Float, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class ModelPricing(db.Base):
    """Per-model token pricing for LLM cost estimation."""
    __tablename__ = 'model_pricing'
    __table_args__ = (
        {'schema': c.POSTGRES_SCHEMA, 'extend_existing': True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Normalized model name (matches AuditEvent.model_name)
    model_name: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    # Display name for UI (optional)
    display_name: Mapped[str] = mapped_column(String(256), nullable=True)
    # USD per 1K input (prompt) tokens
    input_cost_per_1k: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    # USD per 1K output (completion) tokens
    output_cost_per_1k: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    updated_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
