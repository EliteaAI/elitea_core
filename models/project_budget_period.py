"""ProjectBudgetPeriod model — the limit and source that applied to a project in a month (#7510).

project_budgets holds only today's settings and the platform defaults live in config, so once
either changes the limit of a closed month cannot be recovered. This table keeps the resolved
result per month instead of the raw inputs, so later edits cannot rewrite history.

The row of the current month is rewritten whenever the effective limit changes; the last write
of a month is therefore its final effective limit and source.
"""

from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Float, func
from sqlalchemy.orm import Mapped, mapped_column

from tools import db, config as c


class ProjectBudgetPeriod(db.Base):
    __tablename__ = 'project_budget_periods'
    __table_args__ = ({'schema': c.POSTGRES_SCHEMA},)

    project_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # 'YYYYMM', UTC calendar month, the same key the Usage counters use
    period: Mapped[str] = mapped_column(String(6), primary_key=True)

    # NULL means unlimited: a stored row with no limit is not the same as no row at all
    effective_limit: Mapped[float] = mapped_column(Float, nullable=True)
    limit_source: Mapped[str] = mapped_column(String(16), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default='USD')

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    def to_json(self):
        return {
            'project_id': self.project_id,
            'period': self.period,
            'effective_limit': self.effective_limit,
            'limit_source': self.limit_source,
            'currency': self.currency,
        }
