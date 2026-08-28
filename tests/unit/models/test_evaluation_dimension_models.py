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


# --- Update: no code/engine pairing validation at the request-fragment level -------------
# Regression for PR #366 review finding #1 (and its follow-up, discussion_r3875690565):
# EvalDimensionUpdateModel used to judge code/return_contract pairing against the request
# fragment alone. Even gating that check on `allowed_engines` actually being sent was not
# enough: {"allowed_engines": ["code"], "name": "Renamed"} is a valid partial update of an
# *existing* code dimension (it keeps its stored `code`), but as a bare fragment it looks
# like a code-engine dimension missing its code and was spuriously rejected. The model can
# never see the existing row, so it must not attempt this validation at all — pairing is
# judged solely against the merged row in update_dimension() (see
# tests/unit/utils/test_evaluation_library_utils.py).

def test_update_code_only_without_resending_engines_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(code='return score > 50')
    assert m.model_dump(exclude_unset=True) == {'code': 'return score > 50'}


def test_update_return_contract_only_without_resending_engines_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(return_contract='number')
    assert m.model_dump(exclude_unset=True) == {'return_contract': 'number'}


def test_update_allows_code_when_engines_explicitly_set_to_ai(pd_eval):
    # Looks contradictory as a bare fragment, but update_dimension() is the sole enforcer now.
    m = pd_eval.EvalDimensionUpdateModel(allowed_engines=['ai'], code='return True')
    assert m.model_dump(exclude_unset=True) == {'allowed_engines': ['ai'], 'code': 'return True'}


def test_update_allows_switching_to_code_engine_without_resending_code(pd_eval):
    # An existing code dimension retaining its stored `code`: renaming it while re-affirming
    # allowed_engines=['code'] must not require resending the (unchanged) script body.
    m = pd_eval.EvalDimensionUpdateModel(allowed_engines=['code'], name='Renamed')
    assert m.model_dump(exclude_unset=True) == {'allowed_engines': ['code'], 'name': 'Renamed'}


def test_update_does_not_default_return_contract_itself(pd_eval):
    # Defaulting return_contract to 'bool' for a code dimension now happens in
    # update_dimension() against the merged row, not here.
    m = pd_eval.EvalDimensionUpdateModel(allowed_engines=['code'], code='return True')
    assert m.return_contract is None


def test_update_name_only_is_valid(pd_eval):
    m = pd_eval.EvalDimensionUpdateModel(name='Renamed')
    assert m.model_dump(exclude_unset=True) == {'name': 'Renamed'}
