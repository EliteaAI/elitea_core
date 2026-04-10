from datetime import datetime
from typing import List, Optional

from sqlalchemy import Integer, String, DateTime, func, ForeignKey, Text, UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .. import db, config as c, PARTICIPANT_TABLE_NAME, MESSAGE_ITEMS_TABLE_NAME
from ..enums.all import CanvasTypes
from .base import MessageItem


class CanvasMessageItem(MessageItem):
    __tablename__ = 'chat_messages_canvas'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    __mapper_args__ = {
        "polymorphic_identity": "canvas_message",
    }

    name: Mapped[str] = mapped_column(Text, nullable=False)
    canvas_type: Mapped[CanvasTypes] = mapped_column(String, nullable=False, default=CanvasTypes.CODE)

    versions: Mapped[List['CanvasVersionItem']] = relationship(
        'CanvasVersionItem',
        back_populates='canvas_item',
        lazy=True,
        cascade='all, delete',
        uselist=True
    )

    id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{MESSAGE_ITEMS_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False, primary_key=True)

    latest_version: Mapped['CanvasVersionItem'] = relationship(
        'CanvasVersionItem',
        primaryjoin="and_(CanvasMessageItem.id==CanvasVersionItem.canvas_item_id)",
        order_by="desc(CanvasVersionItem.created_at)",
        uselist=False,
        lazy="select"
    )


class CanvasVersionItem(db.Base):
    __tablename__ = 'chat_canvas_versions'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code_language: Mapped[str] = mapped_column(String(32), nullable=True)
    canvas_content: Mapped[str] = mapped_column(Text, nullable=False, default='')
    canvas_item_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{CanvasMessageItem.__tablename__}.id',
        ondelete='CASCADE'
    ))
    canvas_item = relationship(
        CanvasMessageItem,
        back_populates='versions',
        foreign_keys=[canvas_item_id],
        lazy='joined',
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), index=True)

    authors: Mapped[list['CanvasVersionAuthors']] = relationship(
        'CanvasVersionAuthors',
        # back_populates='canvas_version',
        lazy='joined',
        # cascade='set null',
        uselist=True
    )


class CanvasVersionAuthors(db.Base):
    __tablename__ = 'chat_canvas_version_authors'
    __table_args__ = (
        UniqueConstraint('participant_id', 'canvas_version_id', name='_participant_id_canvas_version_id_uc'),
        {"schema": c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    participant_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.{PARTICIPANT_TABLE_NAME}.id", ondelete='CASCADE')
    )

    canvas_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.{CanvasVersionItem.__tablename__}.id", ondelete='CASCADE')
    )
