"""Unit tests for the dimension Pydantic models (EVAL-P1-B1).

See test_evaluation_suite_models.py for why the pd/evaluation.py module is loaded under a
stubbed sibling package instead of imported directly.
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


# --- Create: code / return_contract pairing ----------------------------------

def test_create_code_requires_code_engine(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDimensionCreateModel(name='x', code='return True')


def test_create_code_engine_requires_code(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDimensionCreateModel(name='x', allowed_engines=['code'])


def test_create_code_engine_with_code_defaults_return_contract(pd_eval):
    m = pd_eval.EvalDimensionCreateModel(name='x', allowed_engines=['code'], code='return True')
    assert m.return_contract == 'bool'


# --- Update: partial-update must not spuriously reject code-only edits -------
# Regression for PR #366 review finding #1: EvalDimensionUpdateModel inherited a
# model_validator that judged code/return_contract against allowed_engines' ['ai'] default,
# even though update_dimension() applies the model with exclude_unset=True. A PUT sending
# only {"code": "..."} — the common case of editing a script without re-sending
# allowed_engines — was spuriously rejected.

def test_update_code_only_without_resending_engines_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(code='return score > 50')
    assert m.model_dump(exclude_unset=True) == {'code': 'return score > 50'}


def test_update_return_contract_only_without_resending_engines_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(return_contract='number')
    assert m.model_dump(exclude_unset=True) == {'return_contract': 'number'}


def test_update_still_rejects_code_when_engines_explicitly_set_to_ai(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDimensionUpdateModel(allowed_engines=['ai'], code='return True')


def test_update_still_requires_code_when_engines_explicitly_set_to_code(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalDimensionUpdateModel(allowed_engines=['code'])


def test_update_code_engine_with_code_defaults_return_contract_when_explicit(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(allowed_engines=['code'], code='return True')
    assert m.return_contract == 'bool'


def test_update_name_only_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(name='Renamed')
    assert m.model_dump(exclude_unset=True) == {'name': 'Renamed'}
