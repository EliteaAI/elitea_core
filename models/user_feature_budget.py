"""ProjectBudget model — per-project monthly spend limit for shared models.

Lives in the shared schema (one row per project) so the admin UI can list every
project's budget in a single query.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


"""UserFeatureBudget model — per-member, per-feature, per-period spend limit for paid tools (#6980)."""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class UserFeatureBudget(db.Base):
    __tablename__ = 'user_feature_budgets'
    __table_args__ = (
        UniqueConstraint(
            'project_id', 'user_id', 'feature', 'period_kind',
            name='uq_user_feature_budgets_project_user_feature_period',
        ),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    feature: Mapped[str] = mapped_column(String(64), nullable=False)
    period_kind: Mapped[str] = mapped_column(String(8), nullable=False, default='month', server_default='month')

    limit_usd: Mapped[float] = mapped_column(Float, nullable=True)
    # enabled=false exempts the member from the platform default, as in user_budgets
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

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
            'user_id': self.user_id,
            'feature': self.feature,
            'period_kind': self.period_kind,
            'limit': self.limit_usd,
            'enabled': self.enabled,
            'monthly_limit': self.limit_usd,
        }
