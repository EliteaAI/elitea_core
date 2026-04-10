from typing import Union, List
from sqlalchemy import ForeignKey, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import MessageItem
from .. import config as c, MESSAGE_ITEMS_TABLE_NAME


class AttachmentMessageItem(MessageItem):
    """Message item for file attachments."""
    __tablename__ = 'chat_messages_attachment'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    __mapper_args__ = {
        "polymorphic_identity": "attachment_message",
    }

    name: Mapped[str] = mapped_column(String(256), nullable=False)
    bucket: Mapped[str] = mapped_column(String(256), nullable=False)
    attachment_type: Mapped[str] = mapped_column(String(256), nullable=False)
    content: Mapped[Union[dict, List[dict]]] = mapped_column(JSON, nullable=True, default=list)

    id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{MESSAGE_ITEMS_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False, primary_key=True)

    @property
    def filepath(self) -> str:
        """Compute filepath from bucket and name."""
        return f"/{self.bucket}/{self.name}"
