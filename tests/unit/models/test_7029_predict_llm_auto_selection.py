"""Issue #7029: /predict_llm carries the model selection, so a skill tested on Auto runs on Auto."""
import pytest
from pydantic import ValidationError
from fixtures.helpers import load_module_with_stubs

AUTO = {'mode': 'auto', 'profile_ref': {'id': 'quality-auto', 'revision': 1},
        'scope_mode': 'agent_task', 'reasoning': {'mode': 'auto'}}


@pytest.fixture(scope='module')
def predict_llm(models_path):
    return load_module_with_stubs(models_path / 'pd' / 'predict_llm.py', 'test_7029_predict_llm')


@pytest.fixture(scope='module')
def llm(models_path):
    return load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_7029_llm')


def chat_llm_settings(predict_llm, llm_settings):
    request = predict_llm.LLMPredictRequest(user_input='hi', llm_settings=llm_settings)
    return request.model_dump(exclude={'return_chat_history', 'include_project_context'})['llm_settings']


def test_auto_selection_reaches_the_chat_request(predict_llm, llm):
    llm_settings = chat_llm_settings(predict_llm, {'selection': AUTO, 'max_tokens': 4096})

    settings = llm.LLMSettingsModel.model_validate(llm_settings)

    assert settings.selection.model_dump() == AUTO
    assert settings.model_name is None
    assert settings.max_tokens == 4096


def test_fixed_model_requests_keep_their_shape(predict_llm, llm):
    llm_settings = chat_llm_settings(predict_llm, {'model_name': 'gpt-4.1', 'model_project_id': 1})

    settings = llm.LLMSettingsModel.model_validate(llm_settings)

    assert settings.selection is None
    assert (settings.model_name, settings.model_project_id) == ('gpt-4.1', 1)


def test_auto_without_a_temperature_sends_none(predict_llm):
    assert chat_llm_settings(predict_llm, {'selection': AUTO})['temperature'] is None


def test_auto_keeps_an_explicit_temperature(predict_llm):
    assert chat_llm_settings(predict_llm, {'selection': AUTO, 'temperature': 0.2})['temperature'] == 0.2


def test_draft_endpoints_still_ignore_a_selection(predict_llm):
    settings = predict_llm.LLMSettingsRequest(selection=AUTO, model_name='gpt-4.1')

    assert 'selection' not in settings.model_dump()


def test_requests_route_as_a_chat_turn_by_default(predict_llm):
    assert predict_llm.LLMPredictRequest(user_input='hi').model_dump()['routing_surface'] == 'chat'


def test_a_skill_test_can_route_as_an_agent_task(predict_llm):
    request = predict_llm.LLMPredictRequest(user_input='hi', routing_surface='agent')

    assert request.model_dump()['routing_surface'] == 'agent'


def test_unknown_routing_surfaces_are_rejected(predict_llm):
    with pytest.raises(ValidationError):
        predict_llm.LLMPredictRequest(user_input='hi', routing_surface='pipeline')
