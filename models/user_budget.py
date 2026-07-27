"""UserBudget model — per-user monthly spend limit within a project.

Stops one member of a shared project consuming the whole project budget.
Lives in the shared schema alongside ProjectBudget so the admin UI can list
all limits centrally.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, Boolean, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class UserBudget(db.Base):
    __tablename__ = 'user_budgets'
    __table_args__ = (
        UniqueConstraint('project_id', 'user_id', name='uq_user_budgets_project_user'),
        {'schema': c.POSTGRES_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    monthly_limit: Mapped[float] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default='USD')
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

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
            'monthly_limit': self.monthly_limit,
            'currency': self.currency,
            'enabled': self.enabled,
        }
