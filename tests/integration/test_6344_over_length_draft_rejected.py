"""Wire contract for the Project Context generator when the model overshoots the 2500-char cap.

The response model used to slice the draft before its own ``max_length`` could fire, so an
over-length generation came back as a ``200`` ending mid-sentence and neither the chat agent nor
the Settings modal learned anything had been dropped (#6344). It is now a ``422`` whose ``error``
sentence names the field, the cap and the actual length, with the whole draft still under ``raw``
so a caller can trim rather than regenerate from nothing.

The endpoint fixtures come from ``test_6416_draft_llm_failure_surfacing`` - it already stands the
handler up against fakes, and the real response model and the real output contract are what is
under test here.
"""
import importlib.util
import json
import pathlib

import pytest

SIBLING = pathlib.Path(__file__).with_name('test_6416_draft_llm_failure_surfacing.py')

_spec = importlib.util.spec_from_file_location('draft_failure_contract_6416', SIBLING)
_fixtures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixtures)

endpoints = _fixtures.endpoints

MAX_LENGTH = 2500
SENTENCE = 'Trunk-based development, pytest and Playwright for testing. '


def _prose(length):
    return (SENTENCE * (length // len(SENTENCE) + 1))[:length]


def _draft_answer(length):
    return json.dumps({
        'project_background': _prose(length),
        'activation_description': 'stack questions',
    })


def _generate(endpoints, body, background_length):
    return _fixtures._call(
        endpoints.project_context, body, _fixtures._thinking_result(_draft_answer(background_length)),
    )


CREATE_BODY = {'user_description': 'an exhaustive engineering handbook'}
EDIT_BODY = {
    'user_description': 'expand every section',
    'current_project_background': 'React and FastAPI, backed by Postgres.',
}

MODES = {'create': CREATE_BODY, 'edit': EDIT_BODY}


@pytest.mark.parametrize('mode', sorted(MODES))
def test_an_over_length_draft_is_rejected_rather_than_truncated(endpoints, mode):
    payload, status, _ = _generate(endpoints, MODES[mode], 3142)

    assert status == 422
    assert 'Project background' in payload['error']
    assert str(MAX_LENGTH) in payload['error']
    assert '3142' in payload['error']


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_error_tells_the_reader_what_to_do_next(endpoints, mode):
    payload, _status, _ = _generate(endpoints, MODES[mode], 3142)

    assert payload['error'].endswith('Try generating again, or ask for a narrower scope.')


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_schema_key_never_reaches_the_reader(endpoints, mode):
    """The modal renders this string verbatim, and `project_background` names nothing the reader
    of that form has ever seen."""
    payload, _status, _ = _generate(endpoints, MODES[mode], 3142)

    assert 'project_background' not in payload['error']
    assert payload['details'][0]['loc'] == ('project_background',)


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_rejected_draft_still_travels_back_whole(endpoints, mode):
    payload, _status, _ = _generate(endpoints, MODES[mode], 3142)

    assert len(payload['raw']['project_background']) == 3142
    assert payload['details'][0]['loc'] == ('project_background',)


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_details_do_not_repeat_the_draft(endpoints, mode):
    payload, _status, _ = _generate(endpoints, MODES[mode], 3142)

    assert all('input' not in detail for detail in payload['details'])


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_error_is_no_longer_the_unactionable_label(endpoints, mode):
    payload, _status, _ = _generate(endpoints, MODES[mode], 2501)

    assert payload['error'] != 'Generated draft failed validation'


@pytest.mark.parametrize('mode', sorted(MODES))
def test_a_bare_string_answer_does_not_put_the_model_class_in_the_error(endpoints, mode):
    bare_string = json.dumps('React and FastAPI, backed by Postgres.')

    payload, status, _ = _fixtures._call(
        endpoints.project_context, MODES[mode], _fixtures._thinking_result(bare_string),
    )

    assert status == 422
    assert 'GenerateProjectContextDraftResponse' not in payload['error']
    assert payload['error'].startswith('Generated draft failed validation')


@pytest.mark.parametrize('mode', sorted(MODES))
def test_a_draft_at_the_cap_still_succeeds(endpoints, mode):
    payload, status, _ = _generate(endpoints, MODES[mode], MAX_LENGTH)

    assert status == 200
    assert len(payload['project_background']) == MAX_LENGTH


@pytest.mark.parametrize('mode', sorted(MODES))
def test_the_prompt_states_the_cap_is_hard(endpoints, mode):
    _payload, status, handler = _generate(endpoints, MODES[mode], MAX_LENGTH)

    assert status == 200
    assert '2500' in handler.sent_instructions
    assert 'hard limit' in handler.sent_instructions


def test_edit_mode_really_was_exercised(endpoints):
    """Both modes share the endpoint; only a different service prompt separates them."""
    _payload, _status, handler = _generate(endpoints, EDIT_BODY, MAX_LENGTH)

    assert EDIT_BODY['current_project_background'] in handler.sent_instructions
