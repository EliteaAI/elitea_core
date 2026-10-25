"""#6716 Slice B: ``settle_run_usage`` writes the ledger's figures back and never raises.

The settlement runs after the run is terminal. What it must guarantee is invisible from the
outside: matched ``eval_case_usage`` rows take the ledger's figures, unmatched ones are left
alone, ``meta.settlement`` and the rollups are rewritten, and an unreadable ledger or a failed
write degrades to ``unavailable`` instead of raising into the task.

The module is loaded into a synthetic package so its in-function relative imports resolve
against fakes.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'evalpkg_settlement_test'
PRID = '6f1c2a90-3b4d-4e5f-8a7b-0123456789ab'


class _Criterion:
    def __init__(self, name, value):
        self.name, self.value = name, value


class _Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return _Criterion(self.name, other)

    def __hash__(self):
        return hash(self.name)


class EvalRun:
    id = _Column('id')


class EvalCaseUsage:
    run_id = _Column('run_id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class EvalCaseExecution:
    run_id = _Column('run_id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Query:
    def __init__(self, items):
        self._items = items

    def filter(self, *criteria):
        return _Query([i for i in self._items
                       if all(getattr(i, c.name) == c.value for c in criteria)])

    def first(self):
        return self._items[0] if self._items else None

    def all(self):
        return list(self._items)


class _Store:
    def __init__(self, run, usage_rows, fail_commit=False, executions=()):
        self.run, self.usage_rows, self.fail_commit, self.commits = run, usage_rows, fail_commit, 0
        self.executions = list(executions)


class _Session:
    def __init__(self, store):
        self._store = store

    def query(self, target):
        if target is EvalRun:
            return _Query([self._store.run])
        return _Query(self._store.executions if target is EvalCaseExecution else self._store.usage_rows)

    def commit(self):
        if self._store.fail_commit:
            raise RuntimeError('db gone')
        self._store.commits += 1


@pytest.fixture
def harness():
    holder = {}
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    models = types.ModuleType(f'{PKG}.models.evaluation')
    models.EvalRun, models.EvalCaseUsage, models.EvalCaseExecution = EvalRun, EvalCaseUsage, EvalCaseExecution

    @contextmanager
    def _get_session(project_id):  # noqa: ARG001
        yield _Session(holder['store'])

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=_get_session)
    log = types.SimpleNamespace(exception=lambda *a, **k: holder.setdefault('logged', []).append(a[0]))
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = log

    saved = {name: sys.modules.get(name) for name in ('tools', 'pylon', 'pylon.core', 'pylon.core.tools')}
    for name, mod in {PKG: pkg, f'{PKG}.utils': utils_pkg, f'{PKG}.models': models_pkg,
                      f'{PKG}.models.evaluation': models, 'tools': tools,
                      'pylon': types.ModuleType('pylon'), 'pylon.core': types.ModuleType('pylon.core'),
                      'pylon.core.tools': pylon_tools}.items():
        sys.modules[name] = mod
    for sibling in ('evaluation_scoring', 'evaluation_usage', 'evaluation_run_orchestration'):
        full = f'{PKG}.utils.{sibling}'
        spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / 'utils' / f'{sibling}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
    yield sys.modules[f'{PKG}.utils.evaluation_run_orchestration'], holder
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _row(index, role):
    return {'dataset_case_id': 100 + index, 'case_index': index, 'role': role,
            'input_tokens': 100, 'output_tokens': 10, 'cache_read_tokens': 0,
            'cache_creation_tokens': 0, 'reasoning_tokens': 0, 'cost': Decimal('0.01'),
            'model_name': 'gpt-4o', 'usage_state': 'recorded', 'usage_state_reason': None,
            'token_source': 'provider', 'cost_source': 'runtime:costs-catalog', 'case_status': 'ok',
            'settled': False}


def _setup(holder, rows, **kwargs):
    run = types.SimpleNamespace(id=9, meta={'platform_run_id': PRID, 'settlement': {'state': 'pending'},
                                            'stop_reason': 'budget_exhausted'})
    records = [EvalCaseUsage(run_id=9, **r) for r in rows]
    holder['store'] = _Store(run, records, **kwargs)
    return run, records


def _settle(orch, rows, fetch):
    return orch.settle_run_usage(1, 9, PRID, rows, None, started_at=datetime(2026, 10, 6),
                                 sleep=lambda s: None, fetch=fetch)


LEDGER = [{'conversation_id': f'eval_agent_{PRID}_0_a1b2c3d4e5f6', 'llm_calls': 2, 'input_tokens': 300,
           'output_tokens': 30, 'cache_read_tokens': 0, 'cache_creation_tokens': 0,
           'reasoning_tokens': 0, 'cost_nano_usd': 40_000_000, 'unpriced_calls': 0, 'model_name': 'gpt-4o'}]


def test_matched_rows_take_the_ledger_figures(harness):
    orch, holder = harness
    rows = [_row(0, 'agent'), _row(1, 'agent')]
    run, records = _setup(holder, rows)

    settlement = _settle(orch, rows, lambda: LEDGER)

    assert settlement['state'] == 'partial'
    assert (records[0].input_tokens, records[0].cost, records[0].cost_source, records[0].settled) == \
        (300, Decimal('0.04'), 'usage_event', True)
    assert (records[1].input_tokens, records[1].settled) == (100, False)
    assert run.meta['settlement']['state'] == 'partial'
    assert run.meta['agent_usage']['totals']['input_tokens'] == 400
    assert (run.meta['platform_run_id'], run.meta['stop_reason']) == (PRID, 'budget_exhausted')
    assert holder['store'].commits == 1


def test_unreadable_ledger_keeps_the_runtime_figures(harness):
    orch, holder = harness
    rows = [_row(0, 'agent')]
    run, records = _setup(holder, rows)

    def fetch():
        raise RuntimeError('rpc down')

    settlement = _settle(orch, rows, fetch)

    assert settlement == {'state': 'unavailable', 'reason': 'ledger_unreadable'}
    assert (records[0].input_tokens, records[0].settled) == (100, False)
    assert run.meta['settlement'] == settlement
    assert holder['logged']


def test_a_failed_write_does_not_raise(harness):
    orch, holder = harness
    rows = [_row(0, 'agent')]
    _setup(holder, rows, fail_commit=True)

    assert _settle(orch, rows, lambda: LEDGER) == {'state': 'unavailable', 'reason': 'write_failed'}


# --- #7015 P3: each agent's ledger figures land in metrics.agents ---------------------------

def _entity_row(case, entity, inp, out, nano, calls=1):
    return {'conversation_id': f'eval_agent_{PRID}_{case}_a1b2c3d4e5f6', 'llm_calls': calls,
            'input_tokens': inp, 'output_tokens': out, 'cache_read_tokens': 0, 'cache_creation_tokens': 0,
            'reasoning_tokens': 0, 'cost_nano_usd': nano, 'unpriced_calls': 0, 'model_name': 'haiku',
            'entity_type': entity[0], 'entity_id': entity[1], 'entity_name': entity[2]}


EVAL = ('evaluation', 219, None)
ORDER = ('application', 53, 'RD-OrderReader')


def _agents():
    return [{'agent': None, 'llm_calls': 2, 'tool_calls': 2, 'input_tokens': 10, 'output_tokens': 1,
             'cost': None, 'source': 'trajectory'},
            {'agent': 'RD-OrderReader', 'llm_calls': 3, 'tool_calls': 2, 'input_tokens': 20,
             'output_tokens': 2, 'cost': None, 'source': 'trajectory'}]


def test_dispatched_sub_agents_take_their_ledger_figures(harness):
    orch, holder = harness
    rows = [_row(0, 'agent')]
    execution = EvalCaseExecution(run_id=9, case_index=0, metrics={'llm_calls': 5, 'agents': _agents()})
    _setup(holder, rows, executions=[execution])
    ledger = [_entity_row(0, EVAL, 3358, 179, 4_253_000, 2),
              _entity_row(0, ORDER, 11299, 191, 12_254_000, 3)]

    _settle(orch, rows, lambda: ledger)

    parent, child = execution.metrics['agents']
    assert (parent['input_tokens'], parent['source'], parent['ledger_calls']) == (3358, 'ledger', 2)
    assert (child['input_tokens'], child['output_tokens'], child['cost']) == (11299, 191, 0.012254)
    assert (child['llm_calls'], child['tool_calls']) == (3, 2)  # the trajectory's counts stay
    assert execution.metrics['llm_calls'] == 5


def test_in_process_sub_agents_keep_the_trajectory_split(harness):
    orch, holder = harness
    rows = [_row(0, 'agent')]
    execution = EvalCaseExecution(run_id=9, case_index=0, metrics={'agents': _agents()})
    _setup(holder, rows, executions=[execution])

    _settle(orch, rows, lambda: [_entity_row(0, EVAL, 400, 40, 9, 5)])  # every row is the eval's

    assert [a['source'] for a in execution.metrics['agents']] == ['trajectory', 'trajectory']


def test_the_breakdown_is_asked_for_per_entity(harness, monkeypatch):
    orch, holder = harness
    rows = [_row(0, 'agent')]
    _setup(holder, rows)
    asked = {}

    class _Rpc:
        def timeout(self, _seconds):
            return self

        def usage_eval_run_breakdown(self, **kwargs):
            asked.update(kwargs)
            return []

    monkeypatch.setitem(sys.modules, 'tools', types.SimpleNamespace(
        db=sys.modules['tools'].db, context=types.SimpleNamespace(rpc_manager=_Rpc())))
    orch.settle_run_usage(1, 9, PRID, rows, None, started_at=datetime(2026, 10, 6), sleep=lambda s: None)
    assert asked['by_entity'] is True
