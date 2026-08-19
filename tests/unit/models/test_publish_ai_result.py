"""Characterization tests for PublishAIResult (models/pd/publish.py) — EVAL-H1 precondition.

These lock the CURRENT envelope→JSON extraction + field/filter behavior of the publish AI
result parser BEFORE run_ai_validation is re-expressed on the run_llm_judge primitive, so any
behavior drift in the publish path surfaces as a red test. publish.py is pure (json/re/pydantic),
so it loads directly with no stubs.
"""
import pathlib
import sys

import pytest
from pydantic import ValidationError

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402


@pytest.fixture(scope='module')
def pub(models_path):
    return load_module_with_stubs(
        models_path / 'pd' / 'publish.py',
        'plugins.elitea_core.models.pd.publish',
    )


def _envelope(content):
    """predict_sio-shaped envelope with a single assistant message."""
    return {'result': {'chat_history': [{'role': 'assistant', 'content': content}]}}


# --- envelope → JSON extraction ----------------------------------------------

def test_bare_json_assistant_message(pub):
    r = pub.PublishAIResult.model_validate(_envelope('{"summary": "ok"}'))
    assert r.summary == 'ok' and r.critical_issues == [] and r.warnings == []


def test_fenced_json_block(pub):
    content = 'Here you go:\n```json\n{"summary": "fenced"}\n```'
    assert pub.PublishAIResult.model_validate(_envelope(content)).summary == 'fenced'


def test_embedded_json_span(pub):
    content = 'prose before {"summary": "embedded"} prose after'
    assert pub.PublishAIResult.model_validate(_envelope(content)).summary == 'embedded'


def test_direct_inner_without_result_wrapper(pub):
    # extract_from_predict_result uses data.get('result', data) — a bare inner dict works too.
    inner = {'chat_history': [{'role': 'assistant', 'content': '{"summary": "direct"}'}]}
    assert pub.PublishAIResult.model_validate(inner).summary == 'direct'


def test_last_assistant_message_wins(pub):
    env = {'result': {'chat_history': [
        {'role': 'assistant', 'content': '{"summary": "first"}'},
        {'role': 'user', 'content': 'again'},
        {'role': 'assistant', 'content': '{"summary": "last"}'},
    ]}}
    assert pub.PublishAIResult.model_validate(env).summary == 'last'


def test_ai_role_and_type_recognized(pub):
    env = {'result': {'chat_history': [{'type': 'ai', 'content': '{"summary": "typed"}'}]}}
    assert pub.PublishAIResult.model_validate(env).summary == 'typed'


# --- rejection paths ----------------------------------------------------------

def test_non_dict_input_rejected(pub):
    with pytest.raises(ValidationError):
        pub.PublishAIResult.model_validate('not a dict')


def test_no_assistant_response_rejected(pub):
    with pytest.raises(ValidationError):
        pub.PublishAIResult.model_validate({'result': {'chat_history': []}})


def test_assistant_response_not_json_rejected(pub):
    with pytest.raises(ValidationError):
        pub.PublishAIResult.model_validate(_envelope('sorry, no json here'))


# --- field defaults + filter_empty_items -------------------------------------

def test_defaults_when_keys_missing(pub):
    r = pub.PublishAIResult.model_validate(_envelope('{}'))
    assert r.summary == '' and r.critical_issues == [] and r.recommendations == []


def test_issue_dropped_when_field_and_issue_blank(pub):
    content = '{"critical_issues": [{"fix": "do x"}, {"field": "name", "issue": "bad"}]}'
    r = pub.PublishAIResult.model_validate(_envelope(content))
    assert len(r.critical_issues) == 1 and r.critical_issues[0].field == 'name'


def test_warning_kept_when_only_issue_present(pub):
    content = '{"warnings": [{"issue": "watch out"}]}'
    r = pub.PublishAIResult.model_validate(_envelope(content))
    assert len(r.warnings) == 1 and r.warnings[0].source == 'ai'


def test_recommendation_default_field_is_generic_and_kept(pub):
    # field defaults to 'Generic' (truthy) so an otherwise-empty recommendation survives the filter.
    r = pub.PublishAIResult.model_validate(_envelope('{"recommendations": [{}]}'))
    assert len(r.recommendations) == 1 and r.recommendations[0].field == 'Generic'


def test_recommendation_dropped_only_when_field_blank_and_no_suggestion(pub):
    content = '{"recommendations": [{"field": "", "suggestion": ""}]}'
    assert pub.PublishAIResult.model_validate(_envelope(content)).recommendations == []
