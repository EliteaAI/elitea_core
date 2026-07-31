"""ProjectBudget model — per-project monthly spend limit for shared models.

Lives in the shared schema (one row per project) so the admin UI can list every
project's budget in a single query.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class ProjectBudget(db.Base):
    __tablename__ = 'project_budgets'
    __table_args__ = ({'schema': c.POSTGRES_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)

    monthly_limit: Mapped[float] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default='USD')
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Applies to any member with no limit of their own, between their row and the platform default
    member_default_limit: Mapped[float] = mapped_column(Float, nullable=True)

    # Tracks the highest alert threshold already sent this period, so the 80% notification fires once
    last_alerted_pct: Mapped[int] = mapped_column(Integer, nullable=True)
    last_alerted_period: Mapped[str] = mapped_column(String(8), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def to_json(self):
        return {
            'project_id': self.project_id,
            'monthly_limit': self.monthly_limit,
            'member_default_limit': self.member_default_limit,
            'currency': self.currency,
            'enabled': self.enabled,
        }
