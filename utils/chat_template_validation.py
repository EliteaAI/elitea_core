from collections import Counter

from .skill_participant_utils import load_skill_participant_target
from ..models.enums.all import ParticipantTypes
from ..models.pd.chat_template import ChatTemplateParticipant

TEMPLATE_ENTITY_NAMES = frozenset({'application', 'pipeline', 'toolkit', 'mcp', 'user', ParticipantTypes.skill})


class ChatTemplateParticipantError(ValueError):
    pass


def _entry_identity(participant: ChatTemplateParticipant) -> tuple:
    return tuple(sorted(participant.model_dump().items()))


def _participant_key(participant: ChatTemplateParticipant) -> tuple:
    return participant.entity_name, participant.id, participant.project_id


def _validate_skill_entry(participant: ChatTemplateParticipant, template_project_id: int) -> None:
    if participant.project_id is None:
        raise ChatTemplateParticipantError(f'Skill participant #{participant.id} requires project_id')
    load_skill_participant_target(
        {'id': participant.id, 'project_id': participant.project_id}, template_project_id, None,
    )


def _reject_new_duplicates(participants: list, stored: list) -> None:
    stored_counts = Counter(_participant_key(p) for p in stored)
    for key, count in Counter(_participant_key(p) for p in participants).items():
        if count > 1 and count > stored_counts[key]:
            entity_name, entity_id, _ = key
            raise ChatTemplateParticipantError(f'Participant {entity_name} #{entity_id} is listed more than once')


def validate_template_participants(participants: list, stored_participants: list, template_project_id: int) -> None:
    stored = [ChatTemplateParticipant.model_validate(p) for p in stored_participants or []]
    _reject_new_duplicates(participants, stored)
    stored_identities = {_entry_identity(p) for p in stored}
    for participant in participants:
        if _entry_identity(participant) in stored_identities:
            continue
        if participant.entity_name not in TEMPLATE_ENTITY_NAMES:
            raise ChatTemplateParticipantError(f'Unsupported participant type: {participant.entity_name}')
        if participant.entity_name == ParticipantTypes.skill:
            _validate_skill_entry(participant, template_project_id)
