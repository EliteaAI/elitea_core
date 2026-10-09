from ..models.enums.all import AgentTypes, ParticipantTypes


def merge_mention_candidates(attached_skills: list, chat_skills: list) -> list:
    return [*attached_skills, *chat_skills]


def resolves_mentions_in_predict(entity_name: str, version_details) -> bool:
    if entity_name == ParticipantTypes.skill:
        return True
    return (
        entity_name == ParticipantTypes.application
        and isinstance(version_details, dict)
        and version_details.get('agent_type') != AgentTypes.pipeline.value
    )
