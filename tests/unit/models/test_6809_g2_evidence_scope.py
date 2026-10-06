"""#6809 G2: ``trajectory`` and ``usage`` evidence scopes need the agent to run.

Both flags are opt-in like ``structure``. Unlike it, they are evidence of what the agent *did*,
so the API must accept a binding scoped to them alone, and the dataset-less structure-only
shortcut must not swallow such a binding. Existing scopes keep their behaviour.

Run via:
    python tests/run_tests.py unit/models/test_6809_g2_evidence_scope.py -v
"""
import pathlib
import sys
import types
import importlib.util

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

PKG = 'elitea_core_6809_g2'


class _AnyMember(type):
    """An enum stand-in: every member is its own name, which is all the pd module needs."""
    def __getattr__(cls, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return name.rstrip('_')


def _package(name: str):
    pkg = types.ModuleType(name)
    pkg.__path__ = []
    sys.modules[name] = pkg
    return pkg


@pytest.fixture(scope='module')
def pd():
    _package(PKG)
    _package(f'{PKG}.models')
    _package(f'{PKG}.models.pd')
    enums = types.ModuleType(f'{PKG}.models.evaluation')
    for name in ('EvalTier', 'EvalEngine', 'EvalScaleType', 'EvalPolarity', 'EvalCaseSource',
                 'EvalRunTrigger'):
        setattr(enums, name, _AnyMember(name, (), {}))
    sys.modules[enums.__name__] = enums

    module_name = f'{PKG}.models.pd.evaluation'
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / 'models/pd/evaluation.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def orch(utils_path):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


TRAJECTORY_ONLY = {'structure': False, 'input': False, 'output': False, 'trajectory': True}
USAGE_ONLY = {'structure': False, 'input': False, 'output': False, 'usage': True}
STRUCTURE_ONLY = {'structure': True, 'input': False, 'output': False}


# --- API validation (_check_evidence_scope, via the binding models) ------------------------

@pytest.mark.parametrize('scope', [TRAJECTORY_ONLY, USAGE_ONLY, STRUCTURE_ONLY,
                                   {'input': True, 'output': True}])
def test_binding_accepts_scope(pd, scope):
    assert pd.EvalBindingCreateModel(dimension_id=1, evidence_scope=scope).evidence_scope == scope
    assert pd.EvalBindingUpdateModel(evidence_scope=scope).evidence_scope == scope


def test_scope_with_nothing_on_is_still_rejected(pd):
    with pytest.raises(ValidationError, match='at least one of'):
        pd.EvalBindingCreateModel(dimension_id=1, evidence_scope={
            'structure': False, 'input': False, 'output': False, 'trajectory': False, 'usage': False})


def test_unknown_key_and_non_bool_are_still_rejected(pd):
    with pytest.raises(ValidationError, match='subset of'):
        pd.EvalBindingCreateModel(dimension_id=1, evidence_scope={'output': True, 'steps': True})
    with pytest.raises(ValidationError, match='booleans'):
        pd.EvalBindingCreateModel(dimension_id=1, evidence_scope={'output': True, 'trajectory': 'yes'})


# --- dispatch: the structure-only shortcut ---------------------------------------------------

@pytest.mark.parametrize('scope', [TRAJECTORY_ONLY, USAGE_ONLY])
def test_trajectory_or_usage_binding_needs_the_agent(orch, scope):
    assert orch.is_structure_only_binding({'evidence_scope': scope}) is False
    assert orch.all_bindings_structure_only([{'evidence_scope': STRUCTURE_ONLY},
                                             {'evidence_scope': scope}]) is False


def test_structure_only_still_skips_the_agent(orch):
    assert orch.is_structure_only_binding({'evidence_scope': STRUCTURE_ONLY}) is True
    assert orch.all_bindings_structure_only([{'evidence_scope': STRUCTURE_ONLY}]) is True


# --- judge batching key ------------------------------------------------------------------------

def test_new_flags_split_judge_batches(orch):
    default = orch.evidence_scope_key({})
    assert orch.evidence_scope_key({'trajectory': True}) != default
    assert orch.evidence_scope_key({'usage': True}) != default


def test_existing_scopes_group_as_before(orch):
    """Pre-existing bindings never carry the new flags, so their grouping is unchanged."""
    assert orch.evidence_scope_key({}) == orch.evidence_scope_key({'input': True, 'output': True})
    assert orch.evidence_scope_key({}) == orch.evidence_scope_key(
        {'structure': False, 'input': True, 'output': True, 'trajectory': False, 'usage': False})
    assert orch.evidence_scope_key({'input': True, 'output': False}) != orch.evidence_scope_key({})


# --- G4: suite meta.steps_limit ---------------------------------------------------------------

@pytest.mark.parametrize('meta', [{}, {'steps_limit': None}, {'steps_limit': 1}, {'steps_limit': 100},
                                  {'steps_limit': 3, 'other': 'kept'}])
def test_suite_accepts_steps_limit(pd, meta):
    assert pd.EvalSuiteCreateModel(application_id=1, meta=meta).meta == meta
    assert pd.EvalSuiteUpdateModel(meta=meta).meta == meta


@pytest.mark.parametrize('limit', [0, 101, -1, 2.5, '3', True])
def test_suite_rejects_bad_steps_limit(pd, limit):
    with pytest.raises(ValidationError, match='steps_limit'):
        pd.EvalSuiteCreateModel(application_id=1, meta={'steps_limit': limit})


# --- #6716: suite meta.consumption_budget -------------------------------------------------------

@pytest.mark.parametrize('budget', [
    None, {}, {'per_case': None, 'per_run': None},
    {'per_run': {'tokens': 50000}}, {'per_case': {'tokens': 0, 'cost': 0.25}},
    {'per_case': {'cost': 1}, 'per_run': {'tokens': None, 'cost': 10.5}},
    {'per_run': {'tokens': 500, 'on_breach': 'report'}}, {'per_run': {'tokens': 500, 'on_breach': 'stop'}},
    {'per_run': {'tokens': 500, 'on_breach': None}},
])
def test_suite_accepts_consumption_budget(pd, budget):
    meta = {'consumption_budget': budget}
    assert pd.EvalSuiteCreateModel(application_id=1, meta=meta).meta == meta
    assert pd.EvalSuiteUpdateModel(meta=meta).meta == meta


@pytest.mark.parametrize('budget', [
    'lots', {'per_week': {'tokens': 1}}, {'per_run': 5}, {'per_run': {'calls': 1}},
    {'per_run': {'tokens': -1}}, {'per_run': {'tokens': 2.5}}, {'per_run': {'tokens': True}},
    {'per_case': {'cost': -0.1}}, {'per_case': {'cost': '1'}}, {'per_case': {'cost': False}},
    {'per_run': {'tokens': 500, 'on_breach': 'warn'}}, {'per_case': {'tokens': 500, 'on_breach': 'report'}},
])
def test_suite_rejects_bad_consumption_budget(pd, budget):
    with pytest.raises(ValidationError, match='consumption_budget'):
        pd.EvalSuiteCreateModel(application_id=1, meta={'consumption_budget': budget})
