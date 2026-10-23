"""EL-7011 R1.1 and R1.4 - request and response models of the schedule draft endpoint (US-21)."""
import pathlib

import pytest
from fixtures.helpers import load_module_with_stubs
from pydantic import ValidationError

MODULE = pathlib.Path(__file__).resolve().parents[2] / 'models' / 'pd' / 'generate_schedule_draft.py'

DRAFT_FIELDS = {
    'name', 'description', 'entity_type', 'application_id', 'version_id', 'run_mode',
    'prompt', 'variables', 'cron', 'timezone', 'expires_at', 'approval_rule',
}


@pytest.fixture(scope='module')
def models():
    return load_module_with_stubs(MODULE, 'schedule_draft_models_7011')


def test_el_7011_r1_4_request_accepts_prompt_and_defaults_the_rest(models):
    request = models.GenerateScheduleDraftRequest(prompt='  Every weekday at 9  ')
    assert request.prompt == 'Every weekday at 9'
    assert request.current.model_dump(exclude_unset=True) == {}
    assert request.locked_entity is None
    assert request.timezone is None


@pytest.mark.parametrize('body', [{}, {'prompt': ''}, {'prompt': '   '}, {'prompt': 'x', 'current': 'no'}])
def test_el_7011_r1_4_request_rejects_empty_prompt_and_wrong_shape(models, body):
    with pytest.raises(ValidationError) as caught:
        models.GenerateScheduleDraftRequest(**body)
    assert caught.value.errors()


def test_el_7011_r1_1_response_carries_the_twelve_fields_and_warnings(models):
    assert set(models.ScheduleDraft.model_fields) == DRAFT_FIELDS
    response = models.GenerateScheduleDraftResponse(
        draft={'name': 'Digest', 'cron': '0 9 * * 1-5'},
        warnings=[{'field': 'timezone', 'reason': 'unknown time zone'}],
    )
    dumped = response.model_dump()
    assert set(dumped['draft']) == DRAFT_FIELDS
    assert dumped['draft']['variables'] == []
    assert dumped['draft']['name'] == 'Digest'
    assert dumped['warnings'] == [{'field': 'timezone', 'reason': 'unknown time zone'}]


def test_el_7011_r1_1_response_defaults_to_an_empty_draft_without_warnings(models):
    response = models.GenerateScheduleDraftResponse(draft={})
    assert response.warnings == []


def test_el_7011_r2_3_locked_entity_takes_only_agent_or_pipeline(models):
    request = models.GenerateScheduleDraftRequest(prompt="x", locked_entity={"entity_type": "agent", "application_id": 4})
    assert request.locked_entity.model_dump() == {"entity_type": "agent", "application_id": 4}
    with pytest.raises(ValidationError):
        models.GenerateScheduleDraftRequest(prompt="x", locked_entity={"entity_type": "toolkit", "application_id": 4})
