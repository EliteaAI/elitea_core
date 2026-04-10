from sqlalchemy import Text, Integer, ForeignKey, String, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import MessageItem
from .. import db, config as c, MESSAGE_ITEMS_TABLE_NAME


class TextMessageItem(MessageItem):
    __tablename__ = 'chat_messages_text'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    __mapper_args__ = {
        "polymorphic_identity": "text_message",
    }

    content: Mapped[str] = mapped_column(Text, nullable=False)

    id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{MESSAGE_ITEMS_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False, primary_key=True)

