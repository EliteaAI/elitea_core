from ..models.enums.all import AgentTypes, ParticipantTypes


SKILL_ORIGIN_PARTICIPANT = 'participant'


def merge_mention_candidates(attached_skills: list, chat_skills: list, project_skills: list = None) -> list:
    return [*attached_skills, *chat_skills, *(project_skills or [])]


def skill_name_key(skill: dict) -> str:
    return (skill.get('name') or '').strip().lower()


def merge_disclosable_skills(own_skills: list, chat_skills: list) -> list:
    taken = {skill_name_key(skill) for skill in own_skills}
    merged = list(own_skills)
    for skill in chat_skills:
        key = skill_name_key(skill)
        if key and key not in taken:
            taken.add(key)
            merged.append({**skill, 'instruction_referenced': False, 'origin': SKILL_ORIGIN_PARTICIPANT})
    return merged


def without_bodies(skills: list) -> list:
    return [{key: value for key, value in skill.items() if key != 'instructions'} for skill in skills or []]


def resolves_mentions_in_predict(entity_name: str, version_details) -> bool:
    if entity_name == ParticipantTypes.skill:
        return True
    return (
        entity_name == ParticipantTypes.application
        and isinstance(version_details, dict)
        and version_details.get('agent_type') != AgentTypes.pipeline.value
    )
