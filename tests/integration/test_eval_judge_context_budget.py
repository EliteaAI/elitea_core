"""Integration tests for the judge-context-budget auto-split/truncation path.

An evidence_scope group with many AI dimensions is dispatched to the judge in exactly one call
today, uncapped — a suite with enough dimensions bound to one scope, or a case with large enough
evidence, can overflow the judge model's context window with no warning. `_score_ai_group_with_budget`
(evaluation_run_orchestration.py) bounds this: it chunks by `MAX_DIMENSIONS_PER_JUDGE_CALL`, then
further splits any chunk whose estimated tokens exceed `judge_budget_tokens`
(`evaluation_ai_judge.split_dimensions_for_budget`), and truncates evidence for any single
dimension that still overflows alone (`_truncate_evidence_for_budget`). This is only visible
end-to-end — through `execute_run` — as "how many times was the judge called, and did the run still
finish with a scored row per dimension," so it belongs here rather than in the pure-function unit
tests for the two modules.

Loaded into a synthetic package, same pattern as ``test_eval_run_progress_push.py``.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_judge_budget_test'


# ---------------------------------------------------------------------------
# fake ORM (same shape as test_eval_run_progress_push.py)
# ---------------------------------------------------------------------------

class _Criterion:
    def __init__(self, column, value):
        self.column = column
        self.value = value


class _Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return _Criterion(self, other)

    def __hash__(self):
        return hash(self.name)


class EvalRunStatus:
    created = 'created'
    running = 'running'
    finished = 'finished'
    errored = 'errored'
    cancelled = 'cancelled'


class EvalRun:
    id = _Column('id')
    status = _Column('status')
    meta = _Column('meta')
    started_at = _Column('started_at')


class EvalResult:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeRow:
    def __init__(self, run_id, snapshot):
        self.id = run_id
        self.status = EvalRunStatus.created
        self.snapshot = snapshot
        self.owner_id = 7
        self.progress = None
        self.headline_score = None
        self.error = None
        self.started_at = None
        self.finished_at = None
        self.meta = {}


class _Query:
    def __init__(self, row, selected):
        self._row = row
        self._selected = selected
        self._matches = True

    def filter(self, *criteria):
        for crit in criteria:
            attr = crit.column.name
            if getattr(self._row, attr) != crit.value:
                self._matches = False
        return self

    def first(self):
        return self._row if self._matches else None

    def scalar(self):
        if not self._matches:
            return None
        return getattr(self._row, self._selected.name) if self._selected else None

    def update(self, values, synchronize_session=False):  # noqa: ARG002
        if not self._matches:
            return 0
        for column, value in values.items():
            setattr(self._row, column.name, value)
        return 1


class _Session:
    def __init__(self, row, added):
        self._row = row
        self._added = added
        self.commits = 0

    def query(self, target):
        selected = target if isinstance(target, _Column) else None
        return _Query(self._row, selected)

    def add(self, obj):
        self._added.append(obj)

    def commit(self):
        self.commits += 1


# ---------------------------------------------------------------------------
# module loading
# ---------------------------------------------------------------------------

def _install_package(row_holder, added):
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []

    evaluation_models = types.ModuleType(f'{PKG}.models.evaluation')
    evaluation_models.EvalRun = EvalRun
    evaluation_models.EvalResult = EvalResult
    evaluation_models.EvalRunStatus = EvalRunStatus

    code_validation = types.ModuleType(f'{PKG}.utils.code_validation')
    code_validation.make_task_node_executor = lambda node: (lambda *a, **k: {})
    code_validation._RESULT_SENTINEL = '__eval_result__'
    code_validation.run_code_validation = lambda *a, **k: {}

    @contextmanager
    def _get_session(project_id):  # noqa: ARG001
        yield _Session(row_holder['row'], added)

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=_get_session)

    for name, mod in {
        PKG: pkg,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.evaluation': evaluation_models,
        f'{PKG}.utils.code_validation': code_validation,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    for sibling in ('evaluation_scoring', 'evaluation_ai_judge', 'evaluation_run_orchestration'):
        full = f'{PKG}.utils.{sibling}'
        spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / 'utils' / f'{sibling}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)

    return sys.modules[f'{PKG}.utils.evaluation_run_orchestration']


@pytest.fixture
def harness():
    row_holder = {'row': None}
    added = []
    orch = _install_package(row_holder, added)
    yield orch, row_holder, added
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


DIMS = [
    {'id': i, 'name': f'dim{i}', 'definition': 'd', 'scale_type': 'continuous',
     'scale_min': 0, 'scale_max': 100}
    for i in range(1, 6)
]


def _ai_snapshot(orch, case_count=1, dims=None, evidence=None):
    """One evidence_scope group of AI dimensions, judge model set so execute_run resolves it."""
    dims = dims if dims is not None else DIMS
    bindings = [{'engine': 'ai', 'dimension_id': d['id'], 'weight': 1.0, 'evidence_scope': {}}
                for d in dims]
    cases = [{**(evidence or {'input': f'q{i}', 'output': f'a{i}'}), 'id': i}
             for i in range(case_count)]
    return orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S', 'judge_model': {'model_name': 'gpt-4o', 'integration_uid': 'u-1'}},
        dimensions=dims,
        bindings=bindings,
        cases=cases,
        application_id=10,
        application_version_id=99,
        trigger_type=orch.TRIGGER_ON_DEMAND,
    )


def _stub_judge(calls):
    """Records each call's dimension_ids and answers every dimension it was asked to score."""
    def judge(project_id, settings, system_prompt, payload, timeout, *, stream_key=None):
        import json
        dim_ids = json.loads(payload)['dimension_ids']
        calls.append(dim_ids)
        scores = {'scores': [{'dimension_id': d, 'score': 42, 'rationale': 'ok'} for d in dim_ids]}
        return {'status': 'ok', 'data': scores, 'error': None, 'raw': None}
    return judge


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_under_budget_group_stays_a_single_judge_call(harness):
    orch, row_holder, _ = harness
    row_holder['row'] = _FakeRow(1, _ai_snapshot(orch))
    calls = []

    outcome = orch.execute_run(1, 1, task_node=object(), judge=_stub_judge(calls))

    assert outcome['status'] == EvalRunStatus.finished
    assert len(calls) == 1
    assert sorted(calls[0]) == [1, 2, 3, 4, 5]


def test_oversized_group_splits_into_multiple_judge_calls_and_still_scores_every_dimension(harness):
    orch, row_holder, _ = harness
    row_holder['row'] = _FakeRow(2, _ai_snapshot(orch))
    calls = []
    # Force a split without depending on real token counts: the default fallback budget is
    # ~110_000 tokens, so make each dimension "cost" enough that the whole group of 5 overflows
    # it but any 2-dimension batch does not.
    from evalpkg_judge_budget_test.utils import evaluation_ai_judge as aij
    aij.estimate_group_tokens = lambda evidence, dims, model=None: len(dims) * 50_000

    outcome = orch.execute_run(1, 2, task_node=object(), judge=_stub_judge(calls),
                               judge_llm_settings={'model_name': 'gpt-4o', 'integration_uid': 'u-1'})

    assert outcome['status'] == EvalRunStatus.finished
    assert len(calls) > 1
    scored_dim_ids = sorted(d for batch in calls for d in batch)
    assert scored_dim_ids == [1, 2, 3, 4, 5]

    results = [r for r in row_holder['row'].snapshot.get('bindings', [])]  # sanity: snapshot intact
    assert len(results) == 5


def test_more_than_max_dimensions_per_call_splits_even_with_no_budget(harness):
    """MAX_DIMENSIONS_PER_JUDGE_CALL is a count-only cap, independent of token estimation."""
    orch, row_holder, _ = harness
    many_dims = [{'id': i, 'name': f'dim{i}', 'definition': 'd', 'scale_type': 'continuous',
                  'scale_min': 0, 'scale_max': 100} for i in range(1, orch.MAX_DIMENSIONS_PER_JUDGE_CALL + 3)]
    row_holder['row'] = _FakeRow(3, _ai_snapshot(orch, dims=many_dims))
    calls = []

    outcome = orch.execute_run(1, 3, task_node=object(), judge=_stub_judge(calls))

    assert outcome['status'] == EvalRunStatus.finished
    assert len(calls) >= 2
    assert all(len(batch) <= orch.MAX_DIMENSIONS_PER_JUDGE_CALL for batch in calls)
    scored_dim_ids = sorted(d for batch in calls for d in batch)
    assert scored_dim_ids == [d['id'] for d in many_dims]


def test_single_oversized_dimension_gets_evidence_truncated_but_still_scored(harness):
    orch, row_holder, _ = harness
    big_output = 'x' * 5000
    row_holder['row'] = _FakeRow(4, _ai_snapshot(orch, dims=[DIMS[0]],
                                                  evidence={'input': 'q', 'output': big_output}))
    calls = []
    seen_payloads = []
    from evalpkg_judge_budget_test.utils import evaluation_ai_judge as aij

    def judge(project_id, settings, system_prompt, payload, timeout, *, stream_key=None):
        import json
        seen_payloads.append(payload)
        dim_ids = json.loads(payload)['dimension_ids']
        calls.append(dim_ids)
        scores = {'scores': [{'dimension_id': d, 'score': 42, 'rationale': 'ok'} for d in dim_ids]}
        return {'status': 'ok', 'data': scores, 'error': None, 'raw': None}

    # every estimate is over budget: forces the single-dimension truncation fallback
    aij.estimate_group_tokens = lambda evidence, dims, model=None: 999_999

    outcome = orch.execute_run(1, 4, task_node=object(), judge=judge,
                               judge_llm_settings={'model_name': 'gpt-4o', 'integration_uid': 'u-1'})

    assert outcome['status'] == EvalRunStatus.finished
    assert len(calls) == 1
    assert calls[0] == [1]
    import json
    sent_output = json.loads(seen_payloads[0])['output']
    assert len(sent_output) < len(big_output)
