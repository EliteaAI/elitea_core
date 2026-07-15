from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, String, Text, Boolean, DateTime, ForeignKey, Index

from . import db, config as c, CONVERSATION_MESSAGE_GROUP_TABLE_NAME, MESSAGE_TRACE_STEP_TABLE_NAME


class MessageTraceStep(db.Base):
    __tablename__ = MESSAGE_TRACE_STEP_TABLE_NAME
    __table_args__ = (
        # Render order is derived from (started_at, id) at read time, not a stored seq —
        # the SDK emits timestamps, never an ordinal, and the FE already sorts by timestamp.
        Index('ix_chat_message_trace_step_group_started', 'message_group_id', 'started_at'),
        Index('ix_chat_message_trace_step_group_kind', 'message_group_id', 'kind'),
        Index('ix_chat_message_trace_step_run_id', 'run_id'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    message_group_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{CONVERSATION_MESSAGE_GROUP_TABLE_NAME}.id',
        ondelete='CASCADE'
    ), nullable=False)

    kind: Mapped[str] = mapped_column(Text, nullable=False)  # 'tool_call' | 'thinking_step'

    # spine (both kinds)
    run_id: Mapped[str] = mapped_column(Text, nullable=True)
    parent_agent_name: Mapped[str] = mapped_column(Text, nullable=True)
    # Per-invocation key: separates parallel/sequential invocations of the SAME sub-agent
    # into distinct UI accordions (SDK stamps it; NULL for pre-column rows → name grouping).
    parent_agent_call_id: Mapped[str] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    is_error: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # tool_call hot fields (promoted; UI-drawn, user-queried)
    tool_name: Mapped[str] = mapped_column(Text, nullable=True)
    tool_inputs: Mapped[dict] = mapped_column(JSONB, nullable=True)
    tool_output: Mapped[str] = mapped_column(Text, nullable=True)
    finish_reason: Mapped[str] = mapped_column(Text, nullable=True)

    # thinking_step hot fields (promoted)
    step_type: Mapped[str] = mapped_column(Text, nullable=True)  # ChatGeneration / AIMessageChunk
    text: Mapped[str] = mapped_column(Text, nullable=True)
    thinking: Mapped[str] = mapped_column(Text, nullable=True)
    model_name: Mapped[str] = mapped_column(Text, nullable=True)

    # Small, bounded display-only sub-objects the FE draws but that aren't promoted columns:
    # tool_call -> {'metadata': {...}, 'tool_meta': {...}}; thinking -> {'response_metadata': {...}}.
    # Never holds tool_output (its own column), so it cannot grow into a freeze-class blob.
    attrs: Mapped[dict] = mapped_column(JSONB, nullable=True)

    message_group: Mapped['ConversationMessageGroup'] = relationship(
        'ConversationMessageGroup',
        foreign_keys=[message_group_id],
        lazy=True,
    )
