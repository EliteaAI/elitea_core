from dataclasses import dataclass
from typing import Optional

from sqlalchemy import desc
from tools import db

from .skill_run_utils import SkillRunError, SkillRunTarget, build_skill_run, load_skill_run_target, \
    resolve_skill_llm_settings
from .utils import get_public_project_id
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.participants import ParticipantMapping
from ..models.pd.participant import ParticipantEntitySkill
from ..models.skill import Skill


SKILL_DISPATCH_KEY = '_skill_dispatch'
FOREIGN_SKILL_ERROR = 'A skill participant must come from this project or the public catalog'
SKILL_NOT_FOUND_ERROR = 'Skill not found'


class SkillParticipantError(ValueError):
    pass


@dataclass(frozen=True)
class SkillParticipantSource:
    project_id: int
    skill_id: int
    published_only: bool


def resolve_skill_participant(entity_meta, chat_project_id: int) -> SkillParticipantSource:
    meta = ParticipantEntitySkill.model_validate(
        entity_meta if isinstance(entity_meta, dict) else entity_meta.model_dump()
    )
    public_project_id = get_public_project_id()
    if meta.project_id not in (chat_project_id, public_project_id):
        raise SkillParticipantError(FOREIGN_SKILL_ERROR)
    return SkillParticipantSource(
        project_id=meta.project_id,
        skill_id=meta.id,
        published_only=meta.project_id == public_project_id,
    )


def load_skill_participant_target(
    entity_meta, chat_project_id: int, version_id: Optional[int],
) -> tuple[SkillParticipantSource, SkillRunTarget]:
    source = resolve_skill_participant(entity_meta, chat_project_id)
    try:
        target = load_skill_run_target(source.project_id, source.skill_id, version_id, source.published_only)
    except SkillRunError as e:
        raise SkillParticipantError(e.message) from e
    return source, target


def validate_skill_participants(participants, chat_project_id: int) -> None:
    for participant in participants:
        if participant.entity_name == ParticipantTypes.skill:
            load_skill_participant_target(
                participant.entity_meta, chat_project_id, participant.entity_settings.get('version_id'),
            )


def skill_participant_details(entity_meta) -> dict:
    meta = ParticipantEntitySkill.model_validate(
        entity_meta if isinstance(entity_meta, dict) else entity_meta.model_dump()
    )
    with db.with_project_schema_session(meta.project_id) as session:
        skill = session.query(Skill).filter(Skill.id == meta.id).first()
        if skill is None:
            raise SkillParticipantError(SKILL_NOT_FOUND_ERROR)
        default_version = skill.get_default_version()
        return {
            'name': skill.name,
            'icon_meta': ((default_version.meta or {}) if default_version else {}).get('icon_meta') or {},
        }


def conversation_llm_override(entity_settings: dict, predict_payload) -> Optional[dict]:
    message_override = predict_payload.llm_settings.dict(exclude_none=True) if predict_payload.llm_settings else None
    return message_override or entity_settings.get('llm_settings') or None


def previous_thread_id(session, msg_group) -> Optional[str]:
    # offset(1) skips the response row created for this turn
    last_skill_message = session.query(ConversationMessageGroup).where(
        ConversationMessageGroup.author_participant_id == msg_group.sent_to_id,
        ConversationMessageGroup.conversation_id == msg_group.conversation_id,
    ).order_by(desc(ConversationMessageGroup.created_at)).offset(1).first()
    return (last_skill_message.meta or {}).get('thread_id') if last_skill_message else None


def build_skill_participant_payload(session, msg_group, predict_payload, entity_settings: dict) -> dict:
    chat_project_id = predict_payload.project_id
    source, target = load_skill_participant_target(
        msg_group.sent_to.entity_meta, chat_project_id, entity_settings.get('version_id'),
    )
    try:
        run = build_skill_run(
            caller_project_id=chat_project_id,
            skill_project_id=source.project_id,
            target=target,
            user_input=None,
            llm_override=conversation_llm_override(entity_settings, predict_payload),
        )
    except SkillRunError as e:
        raise SkillParticipantError(e.message) from e

    chat_owned_keys = ('stream_id', 'message_id', 'user_input', 'chat_history')
    payload = {key: value for key, value in run.data.items() if key not in chat_owned_keys}
    payload['entity_name'] = target.skill_name
    payload['thread_id'] = previous_thread_id(session, msg_group)
    payload['_routing_projection'] = {'instructions': target.instructions}
    payload[SKILL_DISPATCH_KEY] = {
        'usage_entity': run.usage_entity,
        'applied_skills': run.applied_skills,
        'sid_project_id': chat_project_id,
        'start_event_content': run.meta(),
    }
    return payload


def pop_skill_dispatch(payload: dict, start_event_content: dict) -> tuple[dict, dict]:
    dispatch = payload.pop(SKILL_DISPATCH_KEY, None)
    if not dispatch:
        return {}, start_event_content
    rpc_kwargs = {key: dispatch[key] for key in ('usage_entity', 'applied_skills', 'sid_project_id')}
    return rpc_kwargs, {**start_event_content, **dispatch['start_event_content']}


def skill_attachment_llm_settings(session, conversation_id: int, participant, chat_project_id: int) -> Optional[dict]:
    mapping = session.query(ParticipantMapping.entity_settings).where(
        ParticipantMapping.participant_id == participant.id,
        ParticipantMapping.conversation_id == conversation_id,
    ).first()
    entity_settings = (mapping.entity_settings if mapping else None) or {}
    try:
        _, target = load_skill_participant_target(
            participant.entity_meta, chat_project_id, entity_settings.get('version_id'),
        )
        llm_settings, _ = resolve_skill_llm_settings(
            chat_project_id, target.run_settings.get('llm_settings'), entity_settings.get('llm_settings'),
        )
    except (SkillParticipantError, SkillRunError):
        return None
    return llm_settings
