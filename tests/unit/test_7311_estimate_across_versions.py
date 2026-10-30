"""``suite_estimate_inputs`` (#7311): a version with no finished run yet borrows the suite's last
finished run on another version, so a new agent version still gets a pre-run estimate. Its own
finished run still wins once it has one.

The function is extracted by AST, because the module's relative imports need the full plugin. It
runs on real SQLAlchemy over in-memory SQLite, with minimal stand-ins for the eval models.
"""
import ast
import datetime
import pathlib
import sys
import types
from contextlib import contextmanager
from typing import List, Optional

import pytest
from sqlalchemy import Column, DateTime, Integer, Numeric, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
FAKE_MODELS = 'evalfake_7311_models'

Base = declarative_base()


class EvalRun(Base):
    __tablename__ = 'eval_run'
    id = Column(Integer, primary_key=True)
    suite_id = Column(Integer)
    application_version_id = Column(Integer)
    status = Column(String)
    finished_at = Column(DateTime)


class EvalCaseUsage(Base):
    __tablename__ = 'eval_case_usage'
    id = Column(Integer, primary_key=True)
    run_id = Column(Integer)
    case_index = Column(Integer)
    role = Column(String)
    usage_state = Column(String)
    input_tokens = Column(Integer)
    output_tokens = Column(Integer)
    cost = Column(Numeric)


class EvalDataset(Base):
    __tablename__ = 'eval_dataset'
    id = Column(Integer, primary_key=True)


class EvalRunStatus:
    running = 'running'
    finished = 'finished'
    failed = 'failed'


class _Relocate(ast.NodeTransformer):
    """Point the function's ``from ..models.evaluation import ...`` at the stand-ins."""

    def visit_ImportFrom(self, node):  # pylint: disable=C0103
        if node.level == 2 and node.module == 'models.evaluation':
            return ast.copy_location(ast.ImportFrom(module=FAKE_MODELS, names=node.names, level=0), node)
        return node


def _load(session):
    src = (PLUGIN_ROOT / 'utils' / 'evaluation_run_utils.py').read_text()
    fn = next(n for n in ast.parse(src).body
              if isinstance(n, ast.FunctionDef) and n.name == 'suite_estimate_inputs')
    module = ast.fix_missing_locations(_Relocate().visit(ast.Module(body=[fn], type_ignores=[])))

    @contextmanager
    def _session(_s, _project_id):
        yield session

    ns = {
        'Optional': Optional, 'List': List, '_session': _session,
        # A structure-only suite with no dataset runs one case, which keeps the stand-ins small.
        '_load_suite_config': lambda s, suite_id: (types.SimpleNamespace(dataset_id=None), [], []),
        '_resolve_version': lambda bindings, override: override,
        'all_bindings_structure_only': lambda bindings: True,
        'effective_cases': lambda cases, excluded: list(cases),
        'excluded_case_ids': lambda s, suite_id: set(),
    }
    exec(compile(module, 'evaluation_run_utils.py', 'exec'), ns)  # pylint: disable=W0122
    return ns['suite_estimate_inputs']


@pytest.fixture()
def db():
    sys.modules[FAKE_MODELS] = types.SimpleNamespace(
        EvalRun=EvalRun, EvalCaseUsage=EvalCaseUsage, EvalDataset=EvalDataset, EvalRunStatus=EvalRunStatus)
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    sys.modules.pop(FAKE_MODELS, None)


def _run(session, run_id, version_id, *, status=EvalRunStatus.finished, day=1, suite_id=7, tokens=100):
    session.add(EvalRun(id=run_id, suite_id=suite_id, application_version_id=version_id, status=status,
                        finished_at=datetime.datetime(2026, 10, day)))
    session.add(EvalCaseUsage(run_id=run_id, case_index=0, role='agent', usage_state='recorded',
                              input_tokens=tokens, output_tokens=10, cost=None))
    session.commit()


def test_a_new_version_borrows_the_last_finished_run_of_another_version(db):
    _run(db, 1, version_id=10, day=1, tokens=100)
    _run(db, 2, version_id=10, day=2, tokens=200)
    out = _load(db)(1, 7, application_version_id=20)
    assert (out['history_run_id'], out['history_application_version_id']) == (2, 10)
    assert [r['input_tokens'] for r in out['history_rows']] == [200]
    assert out['application_version_id'] == 20


def test_a_version_with_its_own_finished_run_keeps_using_it(db):
    _run(db, 1, version_id=20, day=1, tokens=100)
    _run(db, 2, version_id=10, day=2, tokens=200)
    out = _load(db)(1, 7, application_version_id=20)
    assert (out['history_run_id'], out['history_application_version_id']) == (1, 20)


def test_unfinished_runs_and_other_suites_are_not_history(db):
    _run(db, 1, version_id=10, status=EvalRunStatus.failed, day=2)
    _run(db, 2, version_id=10, suite_id=8, day=3)
    out = _load(db)(1, 7, application_version_id=20)
    assert (out['history_run_id'], out['history_application_version_id'], out['history_rows']) == \
        (None, None, [])
