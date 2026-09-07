"""Issue #6413 - the draft generators must reject a blank user_description.

An unconstrained field let "" and "   " reach the LLM, where
`user_input or 'continue'` turned it into a bare "continue" against the system
prompt and the model invented an unrelated draft, returned with a 200.

Run via:
    python tests/run_tests.py unit/models/test_6413_blank_user_description.py -v
"""
import sys
import types
import pathlib
import importlib.util
import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent

DRAFT_MODELS = {
    'draft_skill': ('models/pd/generate_skill_draft.py', 'GenerateSkillDraftRequest'),
    'draft_project_context': (
        'models/pd/generate_project_context_draft.py',
        'GenerateProjectContextDraftRequest',
    ),
    'draft_application': (
        'models/pd/generate_application_draft.py',
        'GenerateApplicationDraftRequest',
    ),
}


def _load(rel_path: str, name: str):
    path = PLUGIN_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path, submodule_search_locations=[])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _skill_stub():
    """Stand in for models/pd/skill.py, which drags in the ORM and Pylon tools."""
    stub = types.ModuleType('elitea_core_6413.skill')
    stub.RESERVED_NAME_WORDS = ('claude', 'anthropic')
    stub.validate_skill_name = lambda value: value
    return stub


def _project_context_stub():
    stub = types.ModuleType('elitea_core_6413.project_context')
    stub.PROJECT_CONTEXT_MAX_LEN = 10000
    stub.PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN = 2304
    return stub


@pytest.fixture(scope='module')
def request_models():
    predict_llm = _load('models/pd/predict_llm.py', 'elitea_core_6413_predict_llm')

    models = {}
    for key, (rel_path, model_name) in DRAFT_MODELS.items():
        # Each draft module is loaded AS a package, so its own relative imports
        # resolve against the stubs registered underneath it.
        pkg_name = f'elitea_core_6413_{key}'
        sys.modules[f'{pkg_name}.predict_llm'] = predict_llm
        sys.modules[f'{pkg_name}.skill'] = _skill_stub()
        sys.modules[f'{pkg_name}.project_context'] = _project_context_stub()

        path = PLUGIN_ROOT / rel_path
        spec = importlib.util.spec_from_file_location(
            pkg_name, path, submodule_search_locations=[]
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules[pkg_name] = mod
        spec.loader.exec_module(mod)
        models[key] = getattr(mod, model_name)
    return models


class TestBlankUserDescriptionIsRejected:
    @pytest.mark.parametrize('key', sorted(DRAFT_MODELS))
    @pytest.mark.parametrize('blank', ['', '   ', '\t\n  '])
    def test_blank_description_is_rejected(self, request_models, key, blank):
        with pytest.raises(ValidationError) as exc:
            request_models[key].model_validate({'user_description': blank})

        assert any(e['loc'] == ('user_description',) for e in exc.value.errors())

    @pytest.mark.parametrize('key', sorted(DRAFT_MODELS))
    def test_a_real_description_is_accepted_and_stripped(self, request_models, key):
        req = request_models[key].model_validate(
            {'user_description': '  review pull requests  '}
        )

        assert req.user_description == 'review pull requests'

    @pytest.mark.parametrize('key', sorted(DRAFT_MODELS))
    def test_a_missing_description_is_rejected(self, request_models, key):
        with pytest.raises(ValidationError) as exc:
            request_models[key].model_validate({})

        assert any(e['loc'] == ('user_description',) for e in exc.value.errors())
