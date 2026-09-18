"""Routing intent is additive and cannot silently replace an explicit binding."""
import pytest
from pydantic import BaseModel, ValidationError
from fixtures.helpers import load_module_with_stubs


@pytest.fixture(scope='module')
def llm(models_path):
    return load_module_with_stubs(models_path / 'pd' / 'llm.py', 'test_routing_llm_settings')


def auto_selection(**updates):
    return {'mode': 'auto', 'profile_ref': {'id': 'quality-auto', 'revision': 1},
            'scope_mode': 'agent_task', 'reasoning': {'mode': 'auto'}, **updates}


def test_legacy_fixed_dump_has_identical_keys(llm):
    value = {'temperature': None, 'reasoning_effort': 'high', 'max_tokens': 500,
             'model_name': 'chosen-model', 'model_project_id': 42}
    for model in (llm.LLMSettingsModel, llm.LLMSettingsWriteModel):
        assert model(**value).model_dump() == value
        assert model(**value).model_dump(exclude_unset=True) == value


def test_auto_round_trip_does_not_materialize_a_provider(llm):
    selection = auto_selection()
    model = llm.LLMSettingsWriteModel(selection=selection)
    restored = llm.LLMSettingsModel.model_validate_json(model.model_dump_json())
    assert restored.selection.model_dump() == selection
    assert restored.model_name is None and restored.reasoning_effort is None


def test_only_fixed_and_auto_are_new_model_choices(llm):
    for mode in ['inherit', 'pipeline', 'automatic']:
        with pytest.raises(ValidationError):
            llm.LLMSettingsWriteModel(selection={'mode': mode})
    # An embedded child's existing absent settings remain absent; no new mode
    # is required to preserve the caller-binding behavior outside this DTO.
    assert llm.LLMSettingsModel().model_dump(exclude_unset=True) == {}


@pytest.mark.parametrize('extra', [
    {'model_name': 'saved-model'}, {'model_project_id': 42}, {'reasoning_effort': 'high'}
])
def test_conflicting_fixed_fields_are_rejected_even_on_read(llm, extra):
    for model in (llm.LLMSettingsModel, llm.LLMSettingsWriteModel):
        with pytest.raises(ValidationError):
            model(selection=auto_selection(), **extra)


@pytest.mark.parametrize('patch', [
    {'profile_ref': {'id': 'quality-auto', 'revision': True}},
    {'profile_ref': {'id': 'quality-auto', 'revision': 0}},
    {'profile_ref': {'id': 'quality-auto', 'revision': 1, 'authorized': True}},
    {'scope_mode': 'all_pipelines'}, {'reasoning': {'mode': 'auto', 'preset': 'high'}},
    {'model_ref': {'name': 'saved-model', 'project_id': 42}},
])
def test_malformed_or_forged_selection_is_not_ignored(llm, patch):
    with pytest.raises(ValidationError):
        llm.LLMSettingsWriteModel(selection=auto_selection(**patch))


def test_explicit_selection_projects_the_existing_fixed_fields(llm):
    selected = {'mode': 'fixed', 'model_ref': {'name': 'chosen-model', 'project_id': 42},
                'reasoning': {'mode': 'explicit', 'preset': 'high'}}
    value = llm.LLMSettingsWriteModel(selection=selected).model_dump()
    assert (value['model_name'], value['model_project_id'], value['reasoning_effort']) == ('chosen-model', 42, 'high')
    with pytest.raises(ValidationError):
        llm.LLMSettingsWriteModel(selection=selected, model_name='other-model')


def test_manual_auto_effort_remains_explicit(llm):
    selected = auto_selection(reasoning={'mode': 'explicit', 'preset': 'high'})
    value = llm.LLMSettingsWriteModel(selection=selected)
    assert value.reasoning_effort == 'high'
    assert value.selection.reasoning.preset == 'high'
    with pytest.raises(ValidationError):
        llm.LLMSettingsWriteModel(selection=selected, temperature=0.7)


def test_nested_serialization_preserves_legacy_absence(llm):
    class Envelope(BaseModel):
        llm_settings: llm.LLMSettingsModel

    envelope = Envelope(llm_settings={'model_name': 'chosen-model'})
    assert 'selection' not in envelope.model_dump()['llm_settings']
    envelope = Envelope(llm_settings={'selection': auto_selection()})
    assert envelope.model_dump()['llm_settings']['selection']['mode'] == 'auto'


@pytest.mark.parametrize('kind,surface', [('pipeline', None), (None, 'pipeline'), (None, 'pipeline_llm_node')])
def test_pipeline_auto_is_blocked_by_first_release_scope(llm, kind, surface):
    for value in [{'selection': auto_selection()}, llm.LLMSettingsModel(selection=auto_selection())]:
        with pytest.raises(ValueError, match='this release'):
            llm.validate_model_selection_surface(value, agent_type=kind, surface=surface)
    llm.validate_model_selection_surface({'model_name': 'fixed'}, agent_type=kind, surface=surface)


def test_ordinary_agent_child_scope_and_legacy_absence_remain_valid(llm):
    llm.validate_model_selection_surface({'selection': auto_selection()}, agent_type='openai')
    llm.validate_model_selection_surface({}, agent_type='pipeline')
