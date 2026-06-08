from datetime import datetime

from tools import db_tools, db, config as c
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, Boolean, DateTime, Text, func


class ProjectContext(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'project_context'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    content: Mapped[str] = mapped_column(Text, nullable=False, default='')
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
