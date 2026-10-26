"""``get_case_executions`` is bounded (#502 review): paged, trajectories only for one case or on
request, the column deferred otherwise, and usage limited to the page's cases.

The real function runs real SQL against sqlite; only the three models are mirrored (JSON for the
Postgres JSONB columns, no tenant schema), and its sibling imports that touch no data are faked.
"""
import importlib.util
import pathlib
import re
import sys
import types
from contextlib import contextmanager

import pytest
from sqlalchemy import JSON, Column, Integer, String, create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

# Other tests in the run swap ``sqlalchemy`` in sys.modules for a stub; keep the real one for ours.
create_engine('sqlite://').dispose()  # loads the sqlite dialect too
_REAL_SQLALCHEMY = {k: v for k, v in sys.modules.items() if k == 'sqlalchemy' or k.startswith('sqlalchemy.')}


@pytest.fixture(autouse=True)
def _real_sqlalchemy(monkeypatch):
    for name, mod in _REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, mod)

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'evalpkg_case_executions_paging_test'
Base = declarative_base()


class EvalRun(Base):
    __tablename__ = 'eval_run'
    id = Column(Integer, primary_key=True)


class EvalCaseExecution(Base):
    __tablename__ = 'eval_case_execution'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    dataset_case_id = Column(Integer)
    case_index = Column(Integer, nullable=False)
    status = Column(String(32))
    trajectory_state = Column(String(16), nullable=False)
    trajectory_state_reason = Column(String(32))
    trajectory = Column(JSON)
    metrics = Column(JSON)
    created_at = None


class EvalCaseUsage(Base):
    __tablename__ = 'eval_case_usage'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    case_index = Column(Integer)
    dataset_case_id = Column(Integer)
    role = Column(String(16))

    def to_json(self):
        return {'role': self.role, 'case_index': self.case_index, 'dataset_case_id': self.dataset_case_id}


class _NotFound(Exception):
    pass


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod
    return mod


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def results():
    _module(PKG, __path__=[])
    _module(f'{PKG}.models', __path__=[])
    _module(f'{PKG}.models.evaluation', EvalRun=EvalRun, EvalCaseExecution=EvalCaseExecution,
            EvalCaseUsage=EvalCaseUsage)
    _module(f'{PKG}.utils', __path__=[str(PLUGIN_ROOT / 'utils')])

    @contextmanager
    def _session(session, project_id):  # noqa: ARG001
        yield session
    _module(f'{PKG}.utils.evaluation_library_utils', _session=_session)
    _module(f'{PKG}.utils.evaluation_human_score_utils', EvalRunNotFoundError=_NotFound)
    _module(f'{PKG}.utils.evaluation_scoring', snapshot_weight_map=None, fold_latest_normalized=None,
            aggregate_run_score=None)
    _load(f'{PKG}.utils.evaluation_usage', PLUGIN_ROOT / 'utils' / 'evaluation_usage.py')
    return _load(f'{PKG}.utils.evaluation_result_utils', PLUGIN_ROOT / 'utils' / 'evaluation_result_utils.py')


@pytest.fixture
def session():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine)()
    s.add(EvalRun(id=1))
    for i in range(30):
        s.add(EvalCaseExecution(run_id=1, dataset_case_id=100 + i, case_index=i, status='ok',
                                trajectory_state='recorded', trajectory={'steps': [i]}, metrics={}))
        s.add_all([EvalCaseUsage(run_id=1, case_index=i, dataset_case_id=100 + i, role=role)
                   for role in ('agent', 'judge')])
    s.add(EvalRun(id=2))  # on-demand: judge usage, no executions
    s.add_all([EvalCaseUsage(run_id=2, case_index=i, role='judge') for i in range(30)])
    s.commit()
    statements = []
    event.listen(engine, 'before_cursor_execute',
                 lambda conn, cursor, statement, *a: statements.append(statement))
    s.statements = statements
    yield s
    s.close()


def test_a_list_read_has_no_trajectories_and_does_not_select_the_column(results, session):
    out = results.get_case_executions(1, 1, session=session)
    assert out['total'] == 30 and len(out['executions']) == 30 and out['limit'] == results.MAX_EXECUTION_LIMIT
    assert all(row['trajectory'] is None for row in out['executions'])
    assert not _selects_trajectory(session.statements)


def _selects_trajectory(statements):
    page = [q for q in statements if 'FROM eval_case_execution' in q and 'LIMIT' in q]
    assert page
    return bool(re.search(r'eval_case_execution\.trajectory\b(?!_)', page[0]))


def test_one_case_carries_its_trajectory_by_default(results, session):
    out = results.get_case_executions(1, 1, session=session, dataset_case_id=107)
    assert [row['trajectory'] for row in out['executions']] == [{'steps': [7]}]
    assert {u['role'] for u in out['usage']} == {'agent', 'judge'} and len(out['usage']) == 2


def test_an_explicit_trajectory_list_is_capped_to_a_small_page(results, session):
    out = results.get_case_executions(1, 1, session=session, include_trajectory=True, limit=1000)
    assert out['limit'] == results.MAX_TRAJECTORY_PAGE == len(out['executions'])
    assert out['executions'][0]['trajectory'] == {'steps': [0]}
    assert _selects_trajectory(session.statements)


def test_pages_and_their_usage_follow_case_index(results, session):
    out = results.get_case_executions(1, 1, session=session, limit=5, offset=10)
    assert [row['case_index'] for row in out['executions']] == [10, 11, 12, 13, 14]
    assert sorted({u['case_index'] for u in out['usage']}) == [10, 11, 12, 13, 14]
    assert (out['total'], out['limit'], out['offset']) == (30, 5, 10)


def test_an_on_demand_run_pages_its_usage_by_case_index(results, session):
    out = results.get_case_executions(1, 2, session=session, limit=5, offset=25)
    assert out['executions'] == [] and out['total'] == 0
    assert [u['case_index'] for u in out['usage']] == [25, 26, 27, 28, 29]


def test_a_missing_run_raises(results, session):
    with pytest.raises(_NotFound):
        results.get_case_executions(1, 99, session=session)


def test_an_older_fan_out_case_gets_its_agent_split_on_the_drill_down(results, session):
    # Executions written before #7015 P3 have no metrics.agents; the drill-down derives it.
    steps = [{'kind': 'llm', 'tokens': {'in': 10, 'out': 2}},
             {'kind': 'llm', 'parent_agent': 'Child', 'tokens': {'in': 5, 'out': 1}},
             {'kind': 'tool', 'tool_name': 'Child'}]
    session.add(EvalCaseExecution(run_id=1, dataset_case_id=500, case_index=30, status='ok',
                                  trajectory_state='recorded', trajectory={'steps': steps}, metrics={'llm_calls': 2}))
    session.commit()
    row, = results.get_case_executions(1, 1, session=session, dataset_case_id=500)['executions']
    assert row['metrics']['llm_calls'] == 2
    assert [(a['agent'], a['llm_calls'], a['tool_calls'], a['input_tokens']) for a in row['metrics']['agents']] == \
        [(None, 1, 1, 10), ('Child', 1, 0, 5)]
    # A list read has no trajectory to derive it from, and a stored split is served as it is.
    listed = results.get_case_executions(1, 1, session=session, case_index=30, include_trajectory=False)
    assert 'agents' not in listed['executions'][0]['metrics']


def test_a_stored_agent_split_is_not_recomputed(results, session):
    stored = [{'agent': None, 'source': 'ledger'}]
    session.add(EvalCaseExecution(run_id=1, dataset_case_id=501, case_index=31, status='ok', trajectory_state='recorded',
                                  trajectory={'steps': [{'kind': 'llm', 'parent_agent': 'C'}]}, metrics={'agents': stored}))
    session.commit()
    row, = results.get_case_executions(1, 1, session=session, dataset_case_id=501)['executions']
    assert row['metrics']['agents'] == stored
