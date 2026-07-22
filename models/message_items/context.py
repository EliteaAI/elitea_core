from sqlalchemy import Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import MessageItem
from .. import config as c, MESSAGE_ITEMS_TABLE_NAME


class ContextMessageItem(MessageItem):
    __tablename__ = 'chat_messages_context'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    __mapper_args__ = {
        "polymorphic_identity": "context_message",
    }

    context_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    context_type: Mapped[str] = mapped_column(Text, nullable=True)

    id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{MESSAGE_ITEMS_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False, primary_key=True)
