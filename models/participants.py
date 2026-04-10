import uuid
from datetime import datetime

from sqlalchemy import Integer, String, DateTime, func, ForeignKey, JSON, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import CONVERSATION_TABLE_NAME, db, config as c, PARTICIPANT_TABLE_NAME
from .enums.all import ParticipantTypes


class ParticipantMapping(db.Base):
    __tablename__ = 'chat_participant_mapping'
    __table_args__ = (
        UniqueConstraint('participant_id', 'conversation_id', name='_participant_conversation_uc'),
        {"schema": c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    conversation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.{CONVERSATION_TABLE_NAME}.id")
    )
    participant_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{c.POSTGRES_TENANT_SCHEMA}.{PARTICIPANT_TABLE_NAME}.id")
    )
    entity_settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


class Participant(db.Base):
    __tablename__ = PARTICIPANT_TABLE_NAME
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    entity_name: Mapped[ParticipantTypes] = mapped_column(String, nullable=False)
    entity_meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    meta: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    conversations = relationship(
        'Conversation',
        secondary=ParticipantMapping.__table__,
        back_populates='participants',
        lazy='dynamic',
        foreign_keys=[ParticipantMapping.participant_id, ParticipantMapping.conversation_id]
    )
