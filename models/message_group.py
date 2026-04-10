from datetime import datetime

import uuid
from typing import List

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, DateTime, func, ForeignKey, Boolean, UUID, String

from . import db, config as c, CONVERSATION_MESSAGE_GROUP_TABLE_NAME
from .conversation import Conversation
from .message_items.base import MessageItem
from .participants import Participant


class ConversationMessageGroup(db.Base):
    __tablename__ = CONVERSATION_MESSAGE_GROUP_TABLE_NAME
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    author_participant_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{Participant.__tablename__}.id'
    ))
    conversation_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{Conversation.__tablename__}.id'
    ))

    sent_to_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{Participant.__tablename__}.id'
    ), nullable=True)
    sent_to: Mapped['Participant'] = relationship(
        'Participant',
        foreign_keys=[sent_to_id],
        lazy=True,
    )

    reply_to_id: Mapped[int] = mapped_column(Integer, ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{CONVERSATION_MESSAGE_GROUP_TABLE_NAME}.id',
        ondelete='SET NULL'
    ), nullable=True)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    message_items: Mapped[List[MessageItem]] = relationship(
        'MessageItem',
        back_populates='message_group',
        lazy='joined',
        cascade='all, delete',
        uselist=True,
        order_by='asc(MessageItem.order_index)'
    )

    is_streaming: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    conversation: Mapped[Conversation] = relationship(
        'Conversation',
        back_populates='message_groups',
        foreign_keys=[conversation_id],
        lazy=True,
    )
    author_participant: Mapped[Participant] = relationship(
        'Participant',
        foreign_keys=[author_participant_id],
        lazy=True,
    )
    sent_to: Mapped[Participant] = relationship(
        'Participant',
        foreign_keys=[sent_to_id],
        lazy='joined',
    )
    reply_to: Mapped['ConversationMessageGroup'] = relationship(
        'ConversationMessageGroup',
        foreign_keys=[reply_to_id],
        lazy=True,
    )
    task_id: Mapped[str] = mapped_column(String(64), nullable=True, default=None)

    # items_order: Mapped[List['MessageGroupItemsOrder']] = relationship(
    #     'MessageGroupItemsOrder',
    #     back_populates='message_group',
    #     lazy='joined',
    #     cascade='all, delete',
    #     order_by='MessageGroupItemsOrder.order_index'
    # )