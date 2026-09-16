"""Conversation and participant serialization must retain routing intent."""
import importlib.util
import sys
import types
from enum import Enum
from pathlib import Path

import pytest
from pydantic import ValidationError


@pytest.fixture
def participant(monkeypatch):
    root = Path(__file__).resolve().parents[3]/'models'
    package = 'entity_routing_contract'
    for name, path in [(package, root), (package+'.pd', root/'pd'), (package+'.enums', root/'enums')]:
        module = types.ModuleType(name)
        module.__path__ = [str(path)]
        monkeypatch.setitem(sys.modules, name, module)
    enums = types.ModuleType(package+'.enums.all')
    enums.ChatHistoryTemplates = Enum('ChatHistoryTemplates', {'all': 'all'})
    monkeypatch.setitem(sys.modules, enums.__name__, enums)
    for name in ['llm', 'participant_settings']:
        spec = importlib.util.spec_from_file_location(package+'.pd.'+name, root/'pd'/(name+'.py'))
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
    return module


def selection(reasoning=None):
    return {'mode': 'auto', 'profile_ref': {'id': 'quality', 'revision': 1},
            'scope_mode': 'task_episode', 'reasoning': reasoning or {'mode': 'auto'}}


def test_existing_conversation_payload_does_not_gain_selection(participant):
    old = {'temperature': 0.0, 'reasoning_effort': None, 'max_tokens': 1024,
           'model_name': 'chosen', 'model_project_id': 7, 'chat_history_template': 'all'}
    for cls in [participant.EntitySettingsLlm, participant.EntitySettingsLlmWrite]:
        assert cls(**old).model_dump() == old


def test_auto_survives_nested_participant_read_write(participant):
    value = participant.EntitySettingsLlmWrite(selection=selection())
    envelope = participant.EntitySettingsUser(llm_settings=value.model_dump())
    assert envelope.model_dump()['llm_settings']['selection'] == selection()
    with pytest.raises(ValidationError):
        participant.EntitySettingsLlmWrite(selection=selection(), model_name='other')


def test_new_user_participant_keeps_legacy_empty_default(participant):
    # Conversation creation supplies no llm_settings; its legacy default is a
    # dict, which Pydantic passes through the nested wrap serializer directly.
    value = participant.EntitySettingsUser(chat_history_template='all')
    assert value.model_dump() == {'llm_settings': {}, 'chat_history_template': 'all'}


def test_provider_specific_explicit_preset_round_trips(participant):
    expected = selection({'mode': 'explicit', 'preset': 'xhigh'})
    first = participant.EntitySettingsLlmWrite(selection=expected)
    restored = participant.EntitySettingsLlm.model_validate_json(first.model_dump_json())
    assert restored.reasoning_effort == 'xhigh'
    assert restored.selection.reasoning.preset == 'xhigh'
