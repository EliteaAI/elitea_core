import uuid
from datetime import datetime
from typing import List, Optional

from flask import request
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, DateTime, func, ForeignKey, Boolean, UUID, Text, asc, cast, and_

from pylon.core.tools import log
from tools import api_tools, auth, db, serialize, config as c

from ...models.conversation import Conversation
from ...models.participants import Participant

from ...models.message_items.text import TextMessageItem
from ...models.message_group import ConversationMessageGroup


class MigrateMessagesPayload(BaseModel):
    project_ids: Optional[List[int]] = None


class OldMessage(db.Base):
    __tablename__ = 'chat_messages'
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
    reply_to_id: Mapped[int] = mapped_column(Integer, ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.chat_messages.id'
    ), nullable=True)

    meta: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    content: Mapped[str] = mapped_column(Text, nullable=False, default='')
    is_streaming: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    conversation: Mapped['Conversation'] = relationship(
        'Conversation',
        foreign_keys=[conversation_id],
        lazy=True,
    )
    author_participant: Mapped['Participant'] = relationship(
        'Participant',
        foreign_keys=[author_participant_id],
        lazy=True,
    )
    sent_to: Mapped['Participant'] = relationship(
        'Participant',
        foreign_keys=[sent_to_id],
        lazy='joined',
    )
    reply_to: Mapped['OldMessage'] = relationship(
        'OldMessage',
        foreign_keys=[reply_to_id],
        lazy=True,
    )


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.message_groups_migration.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self):
        payload = dict(request.json)

        try:
            migrate_messages_payload = MigrateMessagesPayload.parse_obj(payload)
        except ValidationError as e:
            return e.errors(), 400

        def get_all_project_ids():
            return [
                i['id'] for i in self.module.context.rpc_manager.call.project_list(
                    filter_={'create_success': True}
                )
            ]

        project_ids = migrate_messages_payload.project_ids or get_all_project_ids()
        errors = list()

        for pid in project_ids:
            with (db.get_session(pid) as session):
                try:
                    old_messages = session.query(OldMessage).order_by(asc(OldMessage.created_at)).all()
                    for old_message in old_messages:
                        meta = {
                            "old_reply_to_id": old_message.reply_to_id,
                            "old_id": old_message.id,
                            **old_message.meta
                        }

                        new_msg_group = ConversationMessageGroup(
                            conversation=old_message.conversation,
                            author_participant=old_message.author_participant,
                            sent_to=old_message.sent_to,
                            is_streaming=old_message.is_streaming,
                            created_at=old_message.created_at,
                            updated_at=old_message.updated_at,
                            meta=meta,
                            message_items=[
                                TextMessageItem(
                                    content=old_message.content,
                                    created_at=old_message.created_at,
                                    updated_at=old_message.updated_at,
                                    order_index=0
                                )
                            ]
                        )
                        session.add(new_msg_group)

                    session.commit()
                except SQLAlchemyError as e:
                    log.error(f"Database error while migrating messages in project {pid}: {e}")
                    errors.append(e)
                    session.rollback()

                id_mapping_subquery = session.query(
                    ConversationMessageGroup.id,
                    cast(ConversationMessageGroup.meta['old_id'].astext, Integer).label('old_id')
                ).subquery()

                session.query(ConversationMessageGroup).filter(
                    and_(
                        cast(ConversationMessageGroup.meta['old_reply_to_id'].astext,
                             Integer) == id_mapping_subquery.c.old_id,
                        ConversationMessageGroup.meta['old_reply_to_id'] != None
                    )
                ).update(
                    {ConversationMessageGroup.reply_to_id: id_mapping_subquery.c.id},
                    synchronize_session=False
                )
                session.commit()
        return serialize({'errors': errors}), 200


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI,
    }