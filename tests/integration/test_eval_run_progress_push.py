"""Integration tests for the eval run progress push (WebSocket migration, phase 3).

``execute_run`` keeps its ORM/SDK imports lazy so the surrounding module loads without pylon, and
is normally only covered end-to-end. The progress publisher is worth pinning down here anyway,
because its two guarantees are invisible from the outside: a frame per finished case *plus* one
terminal frame, and a publisher that throws must not cost the run its results. Both are the kind
of thing an innocent refactor breaks silently — nothing fails, the dialog just stops updating.

The module is loaded into a synthetic package so its in-function relative imports
(``from ..models.evaluation import ...``) resolve against fakes.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_progress_test'


# ---------------------------------------------------------------------------
# fake ORM
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
    """Register the synthetic package + stub siblings execute_run imports lazily."""
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

    for sibling in ('evaluation_scoring', 'evaluation_run_orchestration'):
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


def _snapshot(orch, case_count):
    """Human-scored suite: no judge, no agent runner, no sandbox — only the run machinery."""
    return orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S', 'judge_model': None},
        dimensions=[{'id': 5, 'name': 'tone', 'scale_type': 'continuous',
                     'scale_min': 0, 'scale_max': 10}],
        code_validations=[],
        bindings=[{'engine': 'human', 'dimension_id': 5, 'weight': 1.0}],
        cases=[{'id': i, 'input': f'q{i}', 'output': f'a{i}'} for i in range(case_count)],
        application_id=10,
        application_version_id=99,
        trigger_type=orch.TRIGGER_ON_DEMAND,
    )


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_publishes_one_frame_per_case_then_a_terminal_frame(harness):
    orch, row_holder, _ = harness
    row_holder['row'] = _FakeRow(3, _snapshot(orch, 2))
    frames = []

    orch.execute_run(1, 3, task_node=object(), progress_publisher=frames.append)

    running = [f for f in frames if f['status'] == EvalRunStatus.running]
    assert [f['progress'] for f in running] == [{'done': 1, 'total': 2}, {'done': 2, 'total': 2}]
    assert all(f['run_id'] == 3 and f['project_id'] == 1 for f in frames)

    terminal = frames[-1]
    assert terminal['status'] == EvalRunStatus.finished
    assert terminal['progress'] == {'done': 2, 'total': 2}
    assert 'headline_score' in terminal


def test_a_raising_publisher_does_not_cost_the_run_its_results(harness):
    orch, row_holder, added = harness
    row_holder['row'] = _FakeRow(4, _snapshot(orch, 2))

    def _boom(_payload):
        raise RuntimeError('socket gone')

    outcome = orch.execute_run(1, 4, task_node=object(), progress_publisher=_boom)

    assert outcome['status'] == EvalRunStatus.finished
    assert row_holder['row'].status == EvalRunStatus.finished
    assert len(added) == 2


def test_without_a_publisher_the_run_behaves_identically(harness):
    orch, row_holder, added = harness
    row_holder['row'] = _FakeRow(5, _snapshot(orch, 2))

    outcome = orch.execute_run(1, 5, task_node=object())

    assert outcome['status'] == EvalRunStatus.finished
    assert outcome['progress'] == {'done': 2, 'total': 2}
    assert len(added) == 2


def test_progress_column_is_still_committed_for_the_reaper(harness):
    """The push is additive: the committed row remains the reaper's staleness heartbeat."""
    orch, row_holder, _ = harness
    row_holder['row'] = _FakeRow(6, _snapshot(orch, 2))

    orch.execute_run(1, 6, task_node=object(), progress_publisher=lambda _p: None)

    assert row_holder['row'].progress == {'done': 2, 'total': 2}


def test_orchestration_failure_publishes_an_errored_frame(harness):
    orch, row_holder, _ = harness
    snapshot = _snapshot(orch, 1)
    # AI binding with no judge configured — E4 fail-closed, before any case runs.
    snapshot['bindings'][0]['engine'] = 'ai'
    row_holder['row'] = _FakeRow(7, snapshot)
    frames = []

    with pytest.raises(Exception):
        orch.execute_run(1, 7, task_node=object(), progress_publisher=frames.append)

    assert frames[-1]['status'] == EvalRunStatus.errored
    assert frames[-1]['error']


def test_the_resolved_judge_model_is_frozen_onto_the_run(harness):
    """The snapshot's `suite.judge_model` is only the configured reference, and a run may override
    it, so without this the one input that drifts silently between two runs of the same frozen
    suite — the model — would be the one input the snapshot does not record."""
    orch, row_holder, _ = harness
    row_holder['row'] = _FakeRow(8, _snapshot(orch, 1))

    orch.execute_run(1, 8, task_node=object(),
                     judge_llm_settings={'model_name': 'gpt-4o', 'integration_uid': 'u-1'})

    assert row_holder['row'].snapshot['resolved_judge_model'] == {
        'model_name': 'gpt-4o', 'integration_uid': 'u-1'}
