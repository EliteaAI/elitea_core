"""Unit tests for the dataset + case Pydantic models (EVAL-P1-B3, §17.1, §17.2).

models/pd/evaluation.py relative-imports the ORM vocab (``from ..evaluation import ...``),
which now includes ``EvalCaseSource``. We stub a lightweight sibling providing only those
vocab classes, then load pd/evaluation.py under its real package name so the relative import
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


# --- Dataset create/update ----------------------------------------------------

def test_dataset_create_requires_name(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCreateModel()


def test_dataset_create_rejects_blank_name(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCreateModel(name='')


def test_dataset_create_minimal(pd_eval):
    m = pd_eval.EvalDatasetCreateModel(name='Support QA v3')
    assert m.name == 'Support QA v3' and m.description is None and m.meta == {}


def test_dataset_update_partial(pd_eval):
    m = pd_eval.EvalDatasetUpdateModel(description='curated')
    assert m.model_dump(exclude_unset=True) == {'description': 'curated'}


def test_dataset_update_name_optional(pd_eval):
    assert pd_eval.EvalDatasetUpdateModel().model_dump(exclude_unset=True) == {}


# --- Case create/update -------------------------------------------------------

def test_case_create_requires_input(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCaseCreateModel()


def test_case_create_rejects_blank_input(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCaseCreateModel(input='')


def test_case_create_defaults(pd_eval):
    m = pd_eval.EvalDatasetCaseCreateModel(input='what is 2+2?')
    assert m.variables == {} and m.expected_output is None
    assert m.source_type == 'manual' and m.order_index == 0


def test_case_create_with_expected_and_vars(pd_eval):
    m = pd_eval.EvalDatasetCaseCreateModel(
        input='refund for order', variables={'order_id': '8842'}, expected_output='30-day window',
    )
    assert m.variables == {'order_id': '8842'} and m.expected_output == '30-day window'


def test_case_bad_source_type_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCaseCreateModel(input='q', source_type='bogus')


def test_case_accepts_conversation_source(pd_eval):
    m = pd_eval.EvalDatasetCaseCreateModel(input='q', source_type='conversation', source_ref='3781')
    assert m.source_type == 'conversation' and m.source_ref == '3781'


def test_case_update_partial_drops_unset(pd_eval):
    m = pd_eval.EvalDatasetCaseUpdateModel(expected_output='fixed')
    assert m.model_dump(exclude_unset=True) == {'expected_output': 'fixed'}


def test_case_update_bad_source_type_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetCaseUpdateModel(source_type='bogus')


# --- Import request -----------------------------------------------------------

def test_import_requires_known_format(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetImportModel(format='xml', content='x')


def test_import_normalizes_format_case(pd_eval):
    m = pd_eval.EvalDatasetImportModel(format='CSV', content='input\nq')
    assert m.format == 'csv'


def test_import_rejects_empty_content(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetImportModel(format='json', content='')


# --- Promote request ----------------------------------------------------------

def test_promote_requires_conversation_id(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDatasetPromoteModel()


def test_promote_defaults_include_expected_true(pd_eval):
    m = pd_eval.EvalDatasetPromoteModel(conversation_id=3781)
    assert m.conversation_id == 3781 and m.include_expected is True
