import uuid
from datetime import datetime
from typing import List

from sqlalchemy import Integer, String, DateTime, func, ForeignKey, Boolean, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import db, config as c, CONVERSATION_TABLE_NAME
from .folder import ConversationFolder
from .participants import Participant, ParticipantMapping


class Conversation(db.Base):
    __tablename__ = CONVERSATION_TABLE_NAME
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False)
    is_private: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    source: Mapped[str] = mapped_column(String(64), nullable=False, default='elitea')
    instructions: Mapped[str] = mapped_column(String, nullable=True)

    attachment_participant_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.chat_participants.id"), nullable=True
    )

    folder_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.chat_conversation_folders.id"), nullable=True
    )
    # ALTER TABLE {pid}.chat_conversations ADD COLUMN folder_id INTEGER, ADD CONSTRAINT fk_folder FOREIGN KEY (folder_id) REFERENCES {pid}.chat_conversation_folders(id);
    folder: Mapped[ConversationFolder] = relationship(
        'ConversationFolder',
        back_populates='conversations',
        lazy=True,
    )

    participants: Mapped[List['Participant']] = relationship(
        'Participant',
        secondary=ParticipantMapping.__table__,
        back_populates='conversations',
        lazy='joined',
        foreign_keys=[ParticipantMapping.participant_id, ParticipantMapping.conversation_id]
    )

    message_groups: Mapped[List['ConversationMessageGroup']] = relationship(
        'ConversationMessageGroup',
        back_populates='conversation',
        lazy='dynamic',
        cascade='all, delete',
        # order_by='desc(ConversationMessageGroup.created_at)'
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
