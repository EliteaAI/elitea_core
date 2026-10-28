from typing import Optional

from sqlalchemy import Integer, and_, case, desc, func, or_

from .conversation_utils import calculate_conversation_durations_batch
from ..models.enums.all import ParticipantTypes
from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.participants import Participant, ParticipantMapping


def skill_participant_ids(session, skill_id: int, skill_project_id: Optional[int]):
    query = session.query(Participant.id).filter(
        Participant.entity_name == ParticipantTypes.skill.value,
        Participant.entity_meta['id'].astext.cast(Integer) == skill_id,
    )
    if skill_project_id is not None:
        query = query.filter(Participant.entity_meta['project_id'].astext.cast(Integer) == skill_project_id)
    return query


def skill_run_conversation_ids(session, skill_id: int, skill_project_id: Optional[int]):
    return session.query(ConversationMessageGroup.conversation_id).filter(
        ConversationMessageGroup.author_participant_id.in_(
            skill_participant_ids(session, skill_id, skill_project_id)
        ),
    )


def skill_run_started_at(session, skill_id: int, skill_project_id: Optional[int]):
    first_skill_reply_at = session.query(func.min(ConversationMessageGroup.created_at)).filter(
        ConversationMessageGroup.conversation_id == Conversation.id,
        ConversationMessageGroup.author_participant_id.in_(
            skill_participant_ids(session, skill_id, skill_project_id)
        ),
    ).scalar_subquery()
    return case(
        (Conversation.meta.has_key('single_participant'), Conversation.created_at),
        else_=func.coalesce(first_skill_reply_at, Conversation.created_at),
    )


def openable_skill_chat_ids(session, skill_id: int, skill_project_id: Optional[int], user_id: int):
    return session.query(Conversation.id).join(
        ParticipantMapping, Conversation.id == ParticipantMapping.conversation_id,
    ).join(
        Participant, Participant.id == ParticipantMapping.participant_id,
    ).filter(
        Conversation.id.in_(skill_run_conversation_ids(session, skill_id, skill_project_id)),
        ~Conversation.meta.has_key('single_participant'),
        or_(
            Conversation.is_private == False,
            and_(
                Participant.entity_name == ParticipantTypes.user.value,
                Participant.entity_meta['id'].astext.cast(Integer) == user_id,
            ),
        ),
    )


def chat_skill_runs(session, conversation_ids: list[int], skill_id: int, skill_project_id: Optional[int]) -> dict:
    if not conversation_ids:
        return {}
    participant_ids = skill_participant_ids(session, skill_id, skill_project_id)
    durations = calculate_conversation_durations_batch(conversation_ids, session, participant_ids)
    latest_versions = {}
    for conversation_id, version_id in (
        session.query(
            ConversationMessageGroup.conversation_id,
            ConversationMessageGroup.meta['skill_run']['skill_version_id'].astext.cast(Integer),
        )
        .filter(
            ConversationMessageGroup.conversation_id.in_(conversation_ids),
            ConversationMessageGroup.author_participant_id.in_(participant_ids),
            ConversationMessageGroup.meta.has_key('skill_run'),
        )
        .order_by(desc(ConversationMessageGroup.created_at), desc(ConversationMessageGroup.id))
        .all()
    ):
        latest_versions.setdefault(conversation_id, version_id)
    run_started_at = dict(
        session.query(ConversationMessageGroup.conversation_id, func.min(ConversationMessageGroup.created_at))
        .filter(
            ConversationMessageGroup.conversation_id.in_(conversation_ids),
            ConversationMessageGroup.author_participant_id.in_(participant_ids),
        )
        .group_by(ConversationMessageGroup.conversation_id)
        .all()
    )
    return {
        conversation_id: {
            'is_chat_run': True,
            'duration': durations.get(conversation_id, 0.0),
            'skill_version_id': latest_versions.get(conversation_id),
            'run_started_at': run_started_at.get(conversation_id),
        }
        for conversation_id in conversation_ids
    }
