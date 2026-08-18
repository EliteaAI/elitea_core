from datetime import datetime
from typing import List, Optional

from tools import db_tools, db, config as c
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, func, ForeignKey, UniqueConstraint

from .enums.all import ToolTypes, EntityTypes
from ..models.enums.all import ToolEntityTypes


APPLICATION_TOOL_TABLE_NAME = 'elitea_tools'


class EntityToolMapping(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'entity_tool_mapping'
    __table_args__ = (
        UniqueConstraint('entity_version_id', 'tool_id', 'entity_type', name='_entity_version_id_tool_id_uc'),
        {"schema": c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tool_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{APPLICATION_TOOL_TABLE_NAME}.id', ondelete='CASCADE')
    )
    entity_id: Mapped[int] = mapped_column(Integer, index=True)
    entity_version_id: Mapped[int] = mapped_column(Integer, index=True)
    entity_type: Mapped[EntityTypes] = mapped_column(String, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
    selected_tools: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)


class EliteATool(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = APPLICATION_TOOL_TABLE_NAME
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    pins_entity_name: str = 'toolkit'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_versions: Mapped[List['ApplicationVersion']] = relationship(
        secondary=EntityToolMapping.__table__,
        primaryjoin=f'and_(EliteATool.id == EntityToolMapping.tool_id, '
                    f'EntityToolMapping.entity_type == "{ToolEntityTypes.agent}")',
        secondaryjoin=f'ApplicationVersion.id == EntityToolMapping.entity_version_id',
        back_populates='tools',
        lazy=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    type: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=True)

    description: Mapped[str] = mapped_column(String(1024), nullable=True)
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False, default=int)

    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
