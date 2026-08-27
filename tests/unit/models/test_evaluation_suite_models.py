"""Unit tests for the suite + binding Pydantic models (EVAL-P1-B2).

models/pd/evaluation.py does a relative import of the ORM vocab (``from ..evaluation import``).
The ORM module pulls in sqlalchemy/db, so we stub a lightweight sibling that provides only the
vocab constant classes the pd module needs, then load pd/evaluation.py under its real package
name so the relative import resolves against the stub.
"""
import sys
import types

import pytest
from pydantic import ValidationError


@pytest.fixture(scope='module')
def pd_eval(models_path):
    # --- stub package tree: plugins.elitea_core.models(.evaluation) ---
    def _pkg(name):
        mod = types.ModuleType(name)
        mod.__path__ = []  # mark as package so submodule imports resolve
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


# --- Binding: one-of source ---------------------------------------------------

def test_binding_requires_exactly_one_source(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel()  # zero sources


def test_binding_rejects_two_sources(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel(dimension_id=1, platform_key='safety')


def test_binding_accepts_single_dimension(pd_eval):
    m = pd_eval.EvalBindingCreateModel(dimension_id=7)
    assert m.dimension_id == 7 and m.platform_key is None


def test_binding_accepts_platform_key(pd_eval):
    m = pd_eval.EvalBindingCreateModel(platform_key='safety')
    assert m.platform_key == 'safety'


# --- Binding: engine / operator / evidence validators -------------------------

def test_binding_bad_engine_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel(dimension_id=1, engine='magic')


def test_binding_bad_operator_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel(dimension_id=1, target_operator='~=')


def test_binding_bad_evidence_key_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel(dimension_id=1, evidence_scope={'bogus': True})


def test_binding_non_bool_evidence_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingCreateModel(dimension_id=1, evidence_scope={'input': 'yes'})


def test_binding_default_evidence_scope(pd_eval):
    m = pd_eval.EvalBindingCreateModel(dimension_id=1)
    assert m.evidence_scope == {'structure': False, 'input': True, 'output': True}


def test_binding_accepts_expected_evidence_key(pd_eval):
    # reference-based scoring (§17.5): 'expected' opts the binding into seeing expected_output.
    # It is additive — at least one of structure/input/output must still be set.
    m = pd_eval.EvalBindingCreateModel(
        dimension_id=1, evidence_scope={'output': True, 'expected': True},
    )
    assert m.evidence_scope == {'output': True, 'expected': True}


# --- Binding update: partial + immutable source ------------------------------

def test_binding_update_partial_is_valid(pd_eval):
    m = pd_eval.EvalBindingUpdateModel(weight=3)
    assert m.model_dump(exclude_unset=True) == {'weight': 3.0}


def test_binding_update_evidence_validated_when_present(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingUpdateModel(evidence_scope={'nope': True})


# --- Binding reorder ----------------------------------------------------------

def test_reorder_requires_nonempty(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalBindingReorderModel(binding_ids=[])


def test_reorder_accepts_id_list(pd_eval):
    assert pd_eval.EvalBindingReorderModel(binding_ids=[3, 1, 2]).binding_ids == [3, 1, 2]


# --- Suite: create defaults + bootstrap name ---------------------------------

def test_suite_create_defaults_name_to_default_suite(pd_eval):
    m = pd_eval.EvalSuiteCreateModel(application_id=5)
    assert m.name == 'Default suite'


def test_suite_create_requires_application_id(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalSuiteCreateModel()


def test_suite_optional_overrides_persist(pd_eval):
    m = pd_eval.EvalSuiteCreateModel(
        application_id=5, judge_model={'model_name': 'gpt-4o'}, baseline_run_id=42,
    )
    assert m.judge_model == {'model_name': 'gpt-4o'} and m.baseline_run_id == 42


def test_suite_update_partial(pd_eval):
    m = pd_eval.EvalSuiteUpdateModel(name='Regression suite')
    assert m.model_dump(exclude_unset=True) == {'name': 'Regression suite'}
