"""ProjectBudget model — per-project monthly spend limit for shared models.

Lives in the shared schema (one row per project) so the admin UI can list every
project's budget in a single query.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


"""FeatureBudget model — per-project, per-feature, per-period spend limit for paid tools (#6980)."""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class FeatureBudget(db.Base):
    __tablename__ = 'feature_budgets'
    __table_args__ = (
        UniqueConstraint('project_id', 'feature', 'period_kind', name='uq_feature_budgets_project_feature_period'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    feature: Mapped[str] = mapped_column(String(64), nullable=False)
    period_kind: Mapped[str] = mapped_column(String(8), nullable=False, default='month', server_default='month')

    # Null means "follow the default"; enabled=false means exempt, as in project_budgets
    limit_usd: Mapped[float] = mapped_column(Float, nullable=True)
    member_default_limit_usd: Mapped[float] = mapped_column(Float, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Feature on/off for this project; null follows the team/personal default
    feature_enabled: Mapped[bool] = mapped_column(Boolean, nullable=True)

    last_alerted_pct: Mapped[int] = mapped_column(Integer, nullable=True)
    last_alerted_period: Mapped[str] = mapped_column(String(16), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def to_json(self):
        return {
            'project_id': self.project_id,
            'feature': self.feature,
            'period_kind': self.period_kind,
            'limit': self.limit_usd,
            'member_default_limit': self.member_default_limit_usd,
            'enabled': self.enabled,
            'feature_enabled': self.feature_enabled,
            # Legacy key so code shaped for project_budgets rows reads these unchanged
            'monthly_limit': self.limit_usd,
        }
