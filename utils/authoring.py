"""Is this turn the editor's test chat? (#7213, C-1009)

The agent and pipeline editors test a version in a private conversation they create for that one application
(``source`` ``agent`` or ``pipeline``, ``meta.single_participant`` naming it). ``chat_all.predict_sio`` reads the
conversation row it loaded and tells the application predict so, and admission may then run the version as a
``development`` run: the signed-in author's identity and the author's own credentials, no binding. Nothing here is
read from the turn's payload. A shared conversation, a public agent chatted from another project, or any other
participant is not authoring.
"""
from typing import Mapping

AUTHORING_SOURCES = frozenset({"agent", "pipeline"})   # what the two editors write at conversation creation


def is_authoring_conversation(conversation, participant, chat_project_id) -> bool:
    """A private editor conversation made for exactly this application participant, in the application's own project."""
    if conversation is None or participant is None or not getattr(conversation, "is_private", False):
        return False
    if getattr(conversation, "source", None) not in AUTHORING_SOURCES:
        return False
    if str(getattr(participant, "entity_name", "")) != "application":   # the enum prints as its value
        return False
    meta = conversation.meta if isinstance(conversation.meta, Mapping) else {}
    single = meta.get("single_participant")
    entity = participant.entity_meta if isinstance(participant.entity_meta, Mapping) else {}
    if not isinstance(single, Mapping) or single.get("entity_name") != "application":
        return False
    single_entity = single.get("entity_meta") if isinstance(single.get("entity_meta"), Mapping) else {}
    application_id = entity.get("id")
    return (application_id is not None and single_entity.get("id") == application_id
            and entity.get("project_id") == chat_project_id)
