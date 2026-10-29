from dataclasses import dataclass
from functools import partial

from pylon.core.tools import log
from tools import db

from .input_guardrail import redacted_input
from .mcp_versioning import INTERNAL_MCP_ENVIRON_KEY
from .participant_utils import get_or_create_one
from .pipeline_execution import TriggerType, with_execution_type
from .predict_utils import user_input_preview
from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.participants import ParticipantMapping
from ..models.pd.participant import ParticipantEntitySkill, ParticipantEntityUser


SKILL_RUN_SOURCE = ParticipantTypes.skill.value


@dataclass(frozen=True)
class SkillApiRunConversation:
    project_id: int
    conversation_uuid: str
    response_message_id: str

    def discard(self) -> None:
        try:
            with db.get_session(self.project_id) as session:
                conversation = session.query(Conversation).filter(
                    Conversation.uuid == self.conversation_uuid,
                ).first()
                if conversation is not None:
                    session.delete(conversation)
                    session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception('Could not discard the run conversation %s', self.conversation_uuid)


def skill_api_run_meta(skill_project_id: int, target, trigger_type: str = TriggerType.API) -> dict:
    return with_execution_type({
        'single_participant': {
            'entity_name': ParticipantTypes.skill.value,
            'entity_meta': {'id': target.skill_id, 'project_id': skill_project_id},
            'entity_settings': {'version_id': target.version_id},
        },
    }, trigger_type)


def open_skill_api_run_conversation(
    *, caller_project_id: int, skill_project_id: int, target, user_id: int, user_input,
    trigger_type: str = TriggerType.API,
) -> SkillApiRunConversation:
    with db.get_session(caller_project_id) as session:
        conversation = Conversation(
            name=f'API run: {target.skill_name}',
            source=SKILL_RUN_SOURCE,
            author_id=user_id,
            is_private=True,
            meta=skill_api_run_meta(skill_project_id, target, trigger_type),
        )
        session.add(conversation)
        session.flush()

        user_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=user_id),
        )
        skill_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.skill,
            entity_meta=ParticipantEntitySkill(id=target.skill_id, project_id=skill_project_id),
        )
        conversation.participants.extend([user_participant, skill_participant])
        session.flush()
        session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation.id,
            ParticipantMapping.participant_id == skill_participant.id,
        ).update({'entity_settings': {'version_id': target.version_id}})

        question = ConversationMessageGroup(
            conversation=conversation,
            author_participant=user_participant,
            sent_to_id=skill_participant.id,
        )
        session.add(question)
        session.add(TextMessageItem(
            message_group=question,
            item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
            content=user_input_preview(redacted_input(user_input), limit=None) or '',
            order_index=0,
        ))
        session.flush()

        answer = ConversationMessageGroup(
            conversation=conversation,
            author_participant_id=skill_participant.id,
            is_streaming=True,
            reply_to_id=question.id,
        )
        session.add(answer)
        session.commit()
        return SkillApiRunConversation(caller_project_id, str(conversation.uuid), str(answer.uuid))


def run_conversation_opener(environ):
    trigger_type = TriggerType.MCP if environ.get(INTERNAL_MCP_ENVIRON_KEY) else TriggerType.API
    return partial(open_skill_api_run_conversation, trigger_type=trigger_type)
