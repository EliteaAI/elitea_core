"""Unit tests for the platform eval dimension registry Pydantic models (§16.1).

models/pd/eval_platform_dimension.py relatively imports both the ORM vocab and the
project-facing pd/evaluation.py, so both are loaded under their real package names for the
relative imports to resolve — the ORM module itself is stubbed since it pulls in sqlalchemy.
"""
import sys
import types

import pytest
from pydantic import ValidationError


@pytest.fixture(scope='module')
def pd_platform(models_path):
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

    load_module_with_stubs(
        models_path / 'pd' / 'evaluation.py',
        'plugins.elitea_core.models.pd.evaluation',
    )
    return load_module_with_stubs(
        models_path / 'pd' / 'eval_platform_dimension.py',
        'plugins.elitea_core.models.pd.eval_platform_dimension',
    )


# --- create: defaults + required fields ---------------------------------------

def test_create_defaults(pd_platform):
    m = pd_platform.EvalPlatformDimensionCreateModel(name='Toxicity')
    assert m.scale_type == 'continuous'
    assert (m.scale_min, m.scale_max) == (0.0, 100.0)
    assert m.polarity == 'higher_better'
    assert m.default_weight == 1.0
    assert m.default_target is None
    assert m.is_active is True
    assert m.allowed_engines == ['ai']


def test_human_engine_accepted(pd_platform):
    m = pd_platform.EvalPlatformDimensionCreateModel(
        name='Toxicity', allowed_engines=['ai', 'human'],
    )
    assert m.allowed_engines == ['ai', 'human']


def test_empty_engines_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='Toxicity', allowed_engines=[])


def test_code_engine_rejected(pd_platform):
    # Code scoring needs a project-local script, so it cannot be shared via the registry.
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='Toxicity', allowed_engines=['code'])


def test_create_requires_name(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel()


def test_create_rejects_blank_name(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='')


def test_create_rejects_overlong_name(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='x' * 129)


# --- vocab validators ---------------------------------------------------------

def test_bad_scale_type_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='T', scale_type='magic')


def test_bad_polarity_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='T', polarity='sideways')


def test_bad_operator_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='T', default_target_operator='~=')


def test_operator_accepted(pd_platform):
    m = pd_platform.EvalPlatformDimensionCreateModel(
        name='T', default_target=80, default_target_operator='>=',
    )
    assert m.default_target_operator == '>='


# --- scale bounds -------------------------------------------------------------

def test_inverted_scale_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='T', scale_min=10, scale_max=1)


def test_equal_scale_bounds_rejected(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionCreateModel(name='T', scale_min=5, scale_max=5)


# --- update: partial ----------------------------------------------------------

def test_update_is_partial(pd_platform):
    m = pd_platform.EvalPlatformDimensionUpdateModel(name='Renamed')
    assert m.model_dump(exclude_unset=True) == {'name': 'Renamed'}


def test_update_can_deactivate_alone(pd_platform):
    m = pd_platform.EvalPlatformDimensionUpdateModel(is_active=False)
    assert m.model_dump(exclude_unset=True) == {'is_active': False}


def test_update_still_validates_vocab(pd_platform):
    with pytest.raises(ValidationError):
        pd_platform.EvalPlatformDimensionUpdateModel(polarity='sideways')


# --- detail: serialization ----------------------------------------------------

def test_detail_coerces_uuid_to_str(pd_platform):
    import uuid as uuid_module

    value = uuid_module.uuid4()
    m = pd_platform.EvalPlatformDimensionDetailModel(
        id=1, uuid=value, name='Toxicity', scale_type='continuous',
        scale_min=0, scale_max=100, polarity='lower_better',
        default_weight=1.0, is_active=True,
    )
    assert m.uuid == str(value)
