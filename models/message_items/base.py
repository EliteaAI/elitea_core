import uuid
from datetime import datetime

from sqlalchemy import Integer, ForeignKey, String, DateTime, func, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .. import db, config as c, CONVERSATION_MESSAGE_GROUP_TABLE_NAME, MESSAGE_ITEMS_TABLE_NAME


class MessageItem(db.Base):
    __tablename__ = MESSAGE_ITEMS_TABLE_NAME
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    __mapper_args__ = {
        'polymorphic_identity': 'message_item',
        'polymorphic_on': 'item_type'
    }

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)

    item_type: Mapped[str] = mapped_column(String(50), nullable=False)

    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    message_group_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{CONVERSATION_MESSAGE_GROUP_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False)
    message_group: Mapped['ConversationMessageGroup'] = relationship(
        'ConversationMessageGroup',
        back_populates='message_items',
        foreign_keys=[message_group_id],
        lazy=True,
    )
