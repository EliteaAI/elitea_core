import uuid
from datetime import datetime
from typing import List

from sqlalchemy import Integer, String, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import db, config as c


class ConversationFolder(db.Base):
    __tablename__ = 'chat_conversation_folders'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False)  # Folder name
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)  # User or entity that owns the folder
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # Position for ordering (per user), 0 = unpositioned
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)  # Additional metadata if needed

    conversations: Mapped[List['Conversation']] = relationship(
        'Conversation',
        back_populates='folder',
        lazy='dynamic',
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
