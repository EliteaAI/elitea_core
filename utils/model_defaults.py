"""Resolve creation defaults without reinterpreting legacy missing model settings."""
from tools import db, rpc_tools

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.llm import merge_llm_selection_override


def creation_llm_settings(project_id, settings=None, *, surface, agent_type=None):
    settings = dict(settings or {})
    if settings.get('model_name') or settings.get('selection') is not None:
        return settings
    effective_surface = 'pipeline' if agent_type == 'pipeline' else surface
    default = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
        project_id=project_id, section='llm', include_shared=True, surface=effective_surface)
    if (default.get('selection') or {}).get('mode') == 'auto':
        settings.pop('temperature', None)
        if settings.get('reasoning_effort'):
            default = {**default, 'selection': {**default['selection'],
                'reasoning': {'mode': 'explicit', 'preset': settings['reasoning_effort']}}}
    return merge_llm_selection_override(settings, default)


def chat_request_default(project_id, conversation_uuid, user_id):
    """Omitted request selection preserves that caller's saved conversation choice."""
    with db.get_session(project_id) as session:
        mapping = session.query(ParticipantMapping).join(
            Participant, Participant.id == ParticipantMapping.participant_id
        ).join(Conversation, Conversation.id == ParticipantMapping.conversation_id).filter(
            Conversation.uuid == conversation_uuid,
            Participant.entity_name == ParticipantTypes.user,
            Participant.entity_meta['id'].astext == str(user_id),
        ).first()
        saved = (mapping.entity_settings or {}).get('llm_settings') if mapping else None
        if saved and (saved.get('model_name') or saved.get('selection') is not None):
            return dict(saved)
    # Old empty conversations keep concrete fallback; only creation opts into Auto.
    return rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
        project_id=project_id, section='llm', include_shared=True)
