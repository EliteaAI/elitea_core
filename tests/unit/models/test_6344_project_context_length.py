"""Issue #6344 - the generated Project Background must be rejected over the cap, not sliced.

``GenerateProjectContextDraftResponse`` declared ``max_length`` and then defeated it with a
``mode="before"`` validator that sliced the value first, so a 3000-character draft came back as a
200 ending mid-sentence at exactly 2500 - the save endpoint's own limit is a 400, and nothing in
between told the caller content had been dropped.

The real model is loaded, together with the real ``models/pd/project_context.py`` it takes the cap
from: a stubbed cap would prove the constraint fires but not that it fires at the length the save
path enforces.

Run via:
    python tests/run_tests.py unit/models/test_6344_project_context_length.py -v
"""
import importlib.util
import pathlib
import sys

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]

PKG = 'elitea_core_6344_draft_project_context'

SENTENCE = 'Trunk-based development, pytest and Playwright for testing. '


def _load(rel_path: str, name: str):
    path = PLUGIN_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path, submodule_search_locations=[])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def draft_module():
    # loaded AS a package so the module's own relative imports resolve to these submodules
    _load('models/pd/predict_llm.py', f'{PKG}.predict_llm')
    _load('models/pd/project_context.py', f'{PKG}.project_context')
    return _load('models/pd/generate_project_context_draft.py', PKG)


@pytest.fixture(scope='module')
def response_model(draft_module):
    return draft_module.GenerateProjectContextDraftResponse


def _prose(length):
    return (SENTENCE * (length // len(SENTENCE) + 1))[:length]


def _draft(background):
    return {'project_background': background, 'activation_description': 'stack questions'}


class TestProjectBackgroundLength:
    def test_the_cap_is_the_one_the_save_endpoint_enforces(self, draft_module):
        assert draft_module.PROJECT_BACKGROUND_MAX_LENGTH == 2500

    def test_exactly_the_cap_is_accepted(self, response_model, draft_module):
        background = _prose(draft_module.PROJECT_BACKGROUND_MAX_LENGTH)

        draft = response_model.model_validate(_draft(background))

        assert draft.project_background == background

    def test_one_character_over_the_cap_is_rejected(self, response_model, draft_module):
        with pytest.raises(ValidationError) as exc:
            response_model.model_validate(
                _draft(_prose(draft_module.PROJECT_BACKGROUND_MAX_LENGTH + 1))
            )

        errors = exc.value.errors()
        assert [error['loc'] for error in errors] == [('project_background',)]
        assert errors[0]['type'] == 'string_too_long'

    def test_a_far_over_length_draft_is_rejected_rather_than_returned_cut(
        self, response_model, draft_module
    ):
        with pytest.raises(ValidationError):
            response_model.model_validate(_draft(_prose(9000)))

    def test_a_draft_under_the_cap_survives_verbatim(self, response_model):
        background = '# Stack\nReact + FastAPI backed by Postgres.\n'

        draft = response_model.model_validate(_draft(background))

        assert draft.project_background == background

    def test_an_empty_background_is_still_rejected(self, response_model):
        with pytest.raises(ValidationError) as exc:
            response_model.model_validate(_draft(''))

        assert [error['loc'] for error in exc.value.errors()] == [('project_background',)]

    def test_the_activation_description_is_still_normalised(self, response_model):
        draft = response_model.model_validate(
            {'project_background': 'x', 'activation_description': '  stack\n\tquestions  '}
        )

        assert draft.activation_description == 'stack questions'

    def test_the_activation_description_is_still_cut_to_fit_rather_than_rejected(
        self, response_model, draft_module
    ):
        cap = draft_module.PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN
        over_length = _prose(cap + 100)

        draft = response_model.model_validate(
            {'project_background': 'x', 'activation_description': over_length}
        )

        assert draft.activation_description == over_length[:cap].rstrip()
