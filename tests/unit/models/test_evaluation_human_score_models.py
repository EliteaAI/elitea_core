"""Unit tests for the human-score Pydantic models (EVAL-P1-B6).

Reuses the pd/evaluation.py loading strategy from test_evaluation_suite_models.py: stub the ORM
vocab sibling, then load pd/evaluation.py under its real package name so the relative import
resolves against the stub.
"""
import sys
import types

import pytest
from pydantic import ValidationError


@pytest.fixture(scope='module')
def pd_eval(models_path):
    def _pkg(name):
        mod = types.ModuleType(name)
        mod.__path__ = []
        return mod

    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
                 'plugins.elitea_core.models.pd'):
        sys.modules.setdefault(name, _pkg(name))

    vocab = types.ModuleType('plugins.elitea_core.models.evaluation')

    class EvalTier:
        platform = 'platform'; project = 'project'; agent_adhoc = 'agent_adhoc'

    class EvalEngine:
        ai = 'ai'; human = 'human'; code = 'code'

    class EvalScaleType:
        binary = 'binary'; ordinal = 'ordinal'; continuous = 'continuous'

    class EvalPolarity:
        higher_better = 'higher_better'; lower_better = 'lower_better'

    class EvalCaseSource:
        manual = 'manual'; import_ = 'import'; conversation = 'conversation'

    class EvalRunTrigger:
        offline_batch = 'offline_batch'; on_demand = 'on_demand'

    vocab.EvalTier = EvalTier
    vocab.EvalEngine = EvalEngine
    vocab.EvalScaleType = EvalScaleType
    vocab.EvalPolarity = EvalPolarity
    vocab.EvalCaseSource = EvalCaseSource
    vocab.EvalRunTrigger = EvalRunTrigger
    sys.modules['plugins.elitea_core.models.evaluation'] = vocab

    from fixtures.helpers import load_module_with_stubs
    return load_module_with_stubs(
        models_path / 'pd' / 'evaluation.py',
        'plugins.elitea_core.models.pd.evaluation',
    )


# --- Create: required identity + score ---------------------------------------

def test_create_requires_case_dimension_and_score(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalHumanScoreCreateModel()


def test_create_requires_dimension(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalHumanScoreCreateModel(dataset_case_id=1, native_score=4)


def test_create_requires_native_score(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalHumanScoreCreateModel(dataset_case_id=1, dimension_id=2)


def test_create_minimal_valid(pd_eval):
    m = pd_eval.EvalHumanScoreCreateModel(dataset_case_id=1, dimension_id=2, native_score=4)
    assert m.dataset_case_id == 1 and m.dimension_id == 2 and m.native_score == 4.0
    assert m.note is None


def test_create_note_optional(pd_eval):
    m = pd_eval.EvalHumanScoreCreateModel(
        dataset_case_id=1, dimension_id=2, native_score=4, note='looks good',
    )
    assert m.note == 'looks good'


def test_create_ignores_client_normalized_score(pd_eval):
    # normalized_score is server-computed; a client-sent value must not be accepted as a field.
    m = pd_eval.EvalHumanScoreCreateModel(
        dataset_case_id=1, dimension_id=2, native_score=4, normalized_score=999,
    )
    assert not hasattr(m, 'normalized_score')


# --- Detail: serialization ----------------------------------------------------

def test_detail_from_attributes(pd_eval):
    class Row:
        id = 7; run_id = 3; dataset_case_id = 1; dimension_id = 2
        reviewer_id = 5; native_score = 4.0; normalized_score = 75.0
        note = 'ok'; is_latest = True; created_at = None

    d = pd_eval.EvalHumanScoreDetailModel.model_validate(Row())
    assert d.id == 7 and d.normalized_score == 75.0 and d.is_latest is True
