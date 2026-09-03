"""Issue #6344 - an over-length draft comes back described, not silently trimmed.

``describe_validation_failure`` is what turns pydantic's error list into the sentence the 422
carries to the chat agent and the Settings modal. It has to name the field, the cap it broke and
how far past it the draft went - a cap without an actual length gives a regenerating caller no
sense of how much to cut - while keeping the draft body itself out of the message and the log.

The error payloads here come from validating real over-long strings, not from hand-written dicts:
the shape of ``ValidationError.errors()`` is exactly what this function reads.

Run via:
    python tests/run_tests.py unit/utils/test_6344_validation_failure_message.py -v
"""
import pathlib
import sys
import types

import pytest
from pydantic import BaseModel, Field, ValidationError

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module

PUBLIC_PROJECT = 1
MAX_LENGTH = 2500
SENTENCE = 'Elitea runs on React and FastAPI, deployed by GitHub Actions. '


class _Draft(BaseModel):
    """Carries the response model's length rules without importing its domain into this module."""

    project_background: str = Field(min_length=1, max_length=MAX_LENGTH)
    activation_description: str = Field(min_length=1, max_length=300)


class _Slug(BaseModel):
    """A string rule that is not a length, which the real response model has no field for."""

    slug: str = Field(pattern=r'^[a-z-]+$')


@pytest.fixture(scope='module')
def draft_llm_utils():
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: PUBLIC_PROJECT

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'draft_llm_utils',
        extra_stubs={'plugins.elitea_core.utils.utils': utils_stub},
    )


def _prose(length):
    return (SENTENCE * (length // len(SENTENCE) + 1))[:length]


def _errors_for(payload):
    with pytest.raises(ValidationError) as exc:
        _Draft.model_validate(payload)
    return exc.value.errors()


def _over_length_errors(length):
    return _errors_for(
        {'project_background': _prose(length), 'activation_description': 'stack questions'}
    )


class TestDescribeValidationFailure:
    def test_it_names_the_field(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(_over_length_errors(2501))

        assert sentence.startswith('project_background ')

    def test_it_names_the_cap_that_was_broken(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(_over_length_errors(2501))

        assert str(MAX_LENGTH) in sentence

    @pytest.mark.parametrize('length', [2501, 3142, 9000])
    def test_it_reports_the_actual_length(self, draft_llm_utils, length):
        sentence = draft_llm_utils.describe_validation_failure(_over_length_errors(length))

        assert str(length) in sentence

    @pytest.mark.parametrize('length', [2501, 9000])
    def test_the_draft_body_never_reaches_the_message(self, draft_llm_utils, length):
        errors = _over_length_errors(length)

        sentence = draft_llm_utils.describe_validation_failure(errors)

        assert _prose(length) not in sentence
        assert SENTENCE.strip() not in sentence
        assert len(sentence) < 200

    def test_every_broken_field_is_named(self, draft_llm_utils):
        errors = _errors_for(
            {'project_background': _prose(2501), 'activation_description': _prose(400)}
        )

        sentence = draft_llm_utils.describe_validation_failure(errors)

        assert 'project_background' in sentence
        assert 'activation_description' in sentence
        assert '2501' in sentence
        assert '400' in sentence

    def test_two_broken_fields_stay_two_readable_clauses(self, draft_llm_utils):
        errors = _errors_for(
            {'project_background': _prose(2501), 'activation_description': _prose(400)}
        )

        sentence = draft_llm_utils.describe_validation_failure(errors)

        assert sentence.split('; ') == [
            f'project_background is 2501 characters, the maximum is {MAX_LENGTH}',
            'activation_description is 400 characters, the maximum is 300',
        ]

    def test_a_missing_field_is_reported_without_a_length(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(
            _errors_for({'activation_description': 'stack questions'})
        )

        assert sentence.startswith('project_background: ')
        assert 'characters' not in sentence

    def test_a_non_string_value_is_reported_without_a_length(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(
            _errors_for({'project_background': 17, 'activation_description': 'stack questions'})
        )

        assert sentence.startswith('project_background: ')
        assert 'characters' not in sentence

    def test_an_empty_error_list_still_yields_a_sentence(self, draft_llm_utils):
        assert draft_llm_utils.describe_validation_failure([])


class TestNonFieldAndNonLengthRules:
    """A length is a fact about the value, not about the rule - it belongs only where length is the rule."""

    def test_a_bare_string_payload_does_not_leak_the_model_class(self, draft_llm_utils):
        errors = _errors_for('the model answered with a bare quoted string')

        sentence = draft_llm_utils.describe_validation_failure(errors)

        assert sentence == draft_llm_utils.VALIDATION_FAILURE_FALLBACK
        assert _Draft.__name__ not in sentence

    def test_a_bare_string_payload_is_not_measured(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(
            _errors_for('the model answered with a bare quoted string')
        )

        assert 'characters' not in sentence

    def test_a_broken_pattern_is_not_reported_as_a_length(self, draft_llm_utils):
        with pytest.raises(ValidationError) as exc:
            _Slug.model_validate({'slug': 'Not A Slug'})

        sentence = draft_llm_utils.describe_validation_failure(exc.value.errors())

        assert sentence.startswith('slug: ')
        assert 'characters' not in sentence

    def test_a_length_rule_is_still_measured_alongside(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(_over_length_errors(2501))

        assert sentence == f'project_background is 2501 characters, the maximum is {MAX_LENGTH}'

    def test_errors_stripped_of_their_input_cannot_be_measured(self, draft_llm_utils):
        """The endpoint sends `include_input=False` errors to the client; passing those here instead
        loses the measurement silently, so the coupling is pinned rather than left to be rediscovered."""
        with pytest.raises(ValidationError) as exc:
            _Draft.model_validate(
                {'project_background': _prose(2501), 'activation_description': 'stack questions'}
            )

        sentence = draft_llm_utils.describe_validation_failure(exc.value.errors(include_input=False))

        assert '2501' not in sentence

    def test_a_broken_minimum_is_measured_too(self, draft_llm_utils):
        sentence = draft_llm_utils.describe_validation_failure(
            _errors_for({'project_background': '', 'activation_description': 'stack questions'})
        )

        assert sentence == 'project_background is 0 characters, the minimum is 1'
