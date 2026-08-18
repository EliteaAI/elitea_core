"""Unit tests for the run start/list Pydantic models (EVAL-P1-B4, §14.2).

models/pd/evaluation.py relative-imports the ORM vocab (``from ..evaluation import``). The ORM
module pulls in sqlalchemy/db, so we stub a lightweight sibling providing only the vocab constant
classes the pd module needs (now including ``EvalRunTrigger``), then load pd/evaluation.py under
its real package name so the relative import resolves against the stub.
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


# --- create: trigger + defaults ----------------------------------------------

def test_run_defaults_to_offline_batch(pd_eval):
    m = pd_eval.EvalRunCreateModel(suite_id=5)
    assert m.trigger_type == 'offline_batch'
    assert m.dataset_id is None and m.conversation_id is None
    assert m.application_version_id is None and m.judge_model is None


def test_run_bad_trigger_rejected(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalRunCreateModel(suite_id=1, trigger_type='streaming')


def test_batch_run_does_not_require_conversation(pd_eval):
    m = pd_eval.EvalRunCreateModel(suite_id=1, trigger_type='offline_batch', dataset_id=9)
    assert m.dataset_id == 9


# --- create: on_demand requires conversation_id (§14.4) ----------------------

def test_on_demand_requires_conversation_id(pd_eval):
    with pytest.raises(ValidationError):
        pd_eval.EvalRunCreateModel(suite_id=1, trigger_type='on_demand')


def test_on_demand_with_conversation_ok(pd_eval):
    m = pd_eval.EvalRunCreateModel(suite_id=1, trigger_type='on_demand', conversation_id=42)
    assert m.conversation_id == 42


def test_run_accepts_version_and_judge_override(pd_eval):
    m = pd_eval.EvalRunCreateModel(
        suite_id=1, application_version_id=77, judge_model={'model_name': 'm'})
    assert m.application_version_id == 77 and m.judge_model == {'model_name': 'm'}


# --- summary / detail response shapes ----------------------------------------

def test_summary_coerces_uuid_and_omits_snapshot(pd_eval):
    import uuid as _uuid
    m = pd_eval.EvalRunSummaryModel.model_validate({
        'id': 3, 'uuid': _uuid.uuid4(), 'suite_id': 1, 'application_id': 10,
        'application_version_id': 99, 'trigger_type': 'offline_batch', 'status': 'created',
        'progress': {'done': 0, 'total': 4}, 'owner_id': 3,
    })
    assert isinstance(m.uuid, str)
    assert m.progress == {'done': 0, 'total': 4}
    assert not hasattr(m, 'snapshot')


def test_detail_carries_snapshot(pd_eval):
    import uuid as _uuid
    m = pd_eval.EvalRunDetailModel.model_validate({
        'id': 3, 'uuid': _uuid.uuid4(), 'application_id': 10, 'application_version_id': 99,
        'trigger_type': 'on_demand', 'status': 'finished', 'owner_id': 3,
        'snapshot': {'suite': {'id': 1}, 'cases': [{'id': 1}]},
    })
    assert m.snapshot['suite']['id'] == 1
    assert m.status == 'finished'
