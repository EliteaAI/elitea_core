"""Reads an agent's stored chat turns for history mode; every decision on them is in
:mod:`eval_auto_history`.

One agent's participant, the conversations it is in (pinned to the version the settings ask for),
the user messages sent to it in the window (newest first, at most ``MAX_SCAN_GROUPS``), its replies
to them and their text.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.base import MessageItem
from ..models.message_items.text import TextMessageItem
from ..models.participants import Participant, ParticipantMapping
from . import eval_auto_history as history
from .eval_auto_rules import HISTORY_VERSION_ANY
from .evaluation_turn_extraction import group_text


def load_turns(session, project_id: int, application_id: int, version_id: Optional[int], settings: dict,
               now: Optional[datetime] = None) -> List[dict]:
    """The agent's turns in the window, as :func:`eval_auto_history.assemble_turns` builds them."""
    participant_ids = [row.id for row in session.query(Participant.id).filter(
        Participant.entity_name == ParticipantTypes.application.value,
        Participant.entity_meta['id'].astext == str(application_id),
        # A project's chats can hold agents from another project (the public one) with the same id.
        Participant.entity_meta['project_id'].astext == str(project_id),
    )]
    if not participant_ids:
        return []
    mappings = session.query(ParticipantMapping.conversation_id, ParticipantMapping.entity_settings).filter(
        ParticipantMapping.participant_id.in_(participant_ids))
    if settings.get('history_version') != HISTORY_VERSION_ANY and version_id is not None:
        mappings = mappings.filter(ParticipantMapping.entity_settings['version_id'].astext == str(version_id))
    version_by_conversation = {row.conversation_id: (row.entity_settings or {}).get('version_id') for row in mappings}
    if not version_by_conversation:
        return []
    conversations = {row.id: {'is_private': row.is_private, 'source': row.source, 'meta': row.meta,
                              'version_id': version_by_conversation.get(row.id)}
                     for row in session.query(Conversation.id, Conversation.is_private, Conversation.source,
                                              Conversation.meta).filter(
                         Conversation.id.in_(sorted(version_by_conversation)))}
    conversations = {key: value for key, value in conversations.items()
                     if not history.is_evaluation_conversation(value)}
    if not conversations:
        return []

    # message_group.created_at is naive UTC (server_default now())
    since = (now or datetime.now(timezone.utc)).replace(tzinfo=None) - timedelta(days=settings['history_window_days'])
    user_rows = session.query(
        ConversationMessageGroup.id, ConversationMessageGroup.conversation_id, ConversationMessageGroup.created_at,
        ConversationMessageGroup.meta, ConversationMessageGroup.is_streaming, Participant.entity_meta,
    ).join(Participant, Participant.id == ConversationMessageGroup.author_participant_id).filter(
        ConversationMessageGroup.sent_to_id.in_(participant_ids),
        ConversationMessageGroup.conversation_id.in_(sorted(conversations)),
        ConversationMessageGroup.created_at >= since,
        Participant.entity_name == ParticipantTypes.user.value,
    ).order_by(ConversationMessageGroup.created_at.desc(), ConversationMessageGroup.id.desc()
               ).limit(history.MAX_SCAN_GROUPS).all()
    if not user_rows:
        return []
    user_groups = [{'id': row.id, 'conversation_id': row.conversation_id, 'created_at': row.created_at,
                    'meta': row.meta, 'is_streaming': row.is_streaming,
                    'user_id': (row.entity_meta or {}).get('id')} for row in user_rows]
    agent_groups = [row._asdict() for row in session.query(
        ConversationMessageGroup.id, ConversationMessageGroup.reply_to_id, ConversationMessageGroup.meta,
        ConversationMessageGroup.is_streaming,
    ).filter(
        ConversationMessageGroup.reply_to_id.in_([g['id'] for g in user_groups]),
        ConversationMessageGroup.author_participant_id.in_(participant_ids),
    )]
    group_ids = [g['id'] for g in user_groups] + [g['id'] for g in agent_groups]
    items_by_group = {}
    for item in session.query(TextMessageItem).filter(MessageItem.message_group_id.in_(group_ids)):
        items_by_group.setdefault(item.message_group_id, []).append(item)
    texts = {group_id: group_text(items) for group_id, items in items_by_group.items()}
    return history.assemble_turns(user_groups, agent_groups, texts, conversations)
