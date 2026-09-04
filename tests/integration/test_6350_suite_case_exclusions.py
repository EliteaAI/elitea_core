"""Per-suite dataset case exclusions (#6350).

A shared dataset stays owned by the agent that authored it, so a borrowing suite may not edit
its cases — but the UI still offered add/remove and the API answered with a bare 404. The fix
keeps the origin dataset immutable and gives the borrower a per-suite overlay instead:

* ``set_case_exclusions`` / ``list_case_exclusions`` — the suite's own opt-out set,
* ``effective_cases`` — the filter the run path applies,
* ``can_edit_dataset`` — the flag read responses now advertise so nobody has to rediscover
  the ownership rule by getting refused.

The real util modules are loaded into a synthetic package against stubbed models and a fake
session: the point is the ownership and set semantics, not the SQL underneath.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_case_exclusions_test'


# ---------------------------------------------------------------------------
# A fake ORM: column descriptors that build predicates, plus a list-backed session
# ---------------------------------------------------------------------------

class _Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ('eq', self.name, other)

    def __hash__(self):
        return hash(self.name)


class _Entity:
    _cols = ()

    def __init__(self, **kwargs):
        for col in self._cols:
            setattr(self, col, kwargs.get(col))

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for col in cls._cols:
            setattr(cls, col, _Col(col))


class EvalSuite(_Entity):
    _cols = ('id', 'application_id', 'name', 'dataset_id')


class EvalDatasetCase(_Entity):
    _cols = ('id', 'dataset_id', 'order_index')


class EvalSuiteCaseExclusion(_Entity):
    _cols = ('id', 'suite_id', 'case_id')


class _Query:
    def __init__(self, store, targets):
        self._store = store
        self._targets = targets
        self._preds = []

    @property
    def _entity(self):
        target = self._targets[0]
        if isinstance(target, _Col):
            for entity, rows in self._store.items():
                if getattr(entity, target.name, None) is target:
                    return entity
            raise AssertionError(f'no entity owns column {target.name}')
        return target

    def filter(self, *preds):
        self._preds.extend(preds)
        return self

    def _rows(self):
        rows = self._store[self._entity]
        for _, name, value in self._preds:
            rows = [r for r in rows if getattr(r, name) == value]
        return rows

    def _project(self, row):
        if isinstance(self._targets[0], _Col):
            return tuple(getattr(row, t.name) for t in self._targets)
        return row

    def all(self):
        return [self._project(r) for r in self._rows()]

    def first(self):
        rows = self._rows()
        return self._project(rows[0]) if rows else None

    def delete(self):
        doomed = self._rows()
        self._store[self._entity] = [
            r for r in self._store[self._entity] if r not in doomed
        ]
        return len(doomed)


class _Session:
    def __init__(self):
        self.store = {EvalSuite: [], EvalDatasetCase: [], EvalSuiteCaseExclusion: []}
        self._next_id = 1

    def query(self, *targets):
        return _Query(self.store, targets)

    def add(self, obj):
        obj.id = self._next_id
        self._next_id += 1
        self.store[type(obj)].append(obj)

    def flush(self):
        pass


def _install_package():
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]

    models_eval = types.ModuleType(f'{PKG}.models.evaluation')
    models_eval.EvalSuite = EvalSuite
    models_eval.EvalDatasetCase = EvalDatasetCase
    models_eval.EvalSuiteCaseExclusion = EvalSuiteCaseExclusion
    models_eval.EvalBinding = type('EvalBinding', (_Entity,), {'_cols': ('id', 'suite_id')})
    models_eval.EvalDimension = type('EvalDimension', (_Entity,), {'_cols': ('id',)})
    models_eval.EvalDataset = type('EvalDataset', (_Entity,), {'_cols': ('id', 'agent_id')})
    models_eval.EvalCaseSource = types.SimpleNamespace(manual='manual')
    models_eval.EvalEngine = types.SimpleNamespace(ai='ai', code='code')
    models_eval.EvalTier = types.SimpleNamespace(
        platform='platform', project='project', agent_adhoc='agent_adhoc')

    models_all = types.ModuleType(f'{PKG}.models.all')
    models_all.ApplicationVersion = type('ApplicationVersion', (_Entity,), {'_cols': ('id',)})
    models_all.Application = type('Application', (_Entity,), {'_cols': ('id',)})

    pd_eval = types.ModuleType(f'{PKG}.models.pd.evaluation')
    for name in (
        'EvalSuiteCreateModel', 'EvalSuiteUpdateModel', 'EvalBindingCreateModel',
        'EvalBindingUpdateModel', 'EvalDatasetCreateModel', 'EvalDatasetUpdateModel',
        'EvalDatasetCaseCreateModel', 'EvalDatasetCaseUpdateModel', 'EvalDatasetImportModel',
        'EvalDatasetPromoteModel', 'EvalDimensionCreateModel', 'EvalDimensionUpdateModel',
    ):
        setattr(pd_eval, name, type(name, (), {}))

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id: None)

    for name, mod in {
        PKG: pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.evaluation': models_eval,
        f'{PKG}.models.all': models_all,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.evaluation': pd_eval,
        f'{PKG}.utils': utils_pkg,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    # evaluation_library_utils owns _session + EvalLibraryError but drags in the code screen;
    # a stub keeps this test on the exclusion logic.
    library_utils = types.ModuleType(f'{PKG}.utils.evaluation_library_utils')

    class _EvalLibraryError(Exception):
        http_status = 400

    @contextmanager
    def _session(session, project_id):  # noqa: ARG001
        assert session is not None, 'these utils are always called with an explicit session here'
        yield session

    library_utils.EvalLibraryError = _EvalLibraryError
    library_utils.EvalNameConflictError = type(
        'EvalNameConflictError', (_EvalLibraryError,), {'http_status': 409})
    library_utils._session = _session
    sys.modules[f'{PKG}.utils.evaluation_library_utils'] = library_utils

    for stub in ('evaluation_dataset_import', 'evaluation_turn_extraction'):
        mod = types.ModuleType(f'{PKG}.utils.{stub}')
        mod.parse_import = lambda *a, **k: ([], [])
        mod.extract_conversation_turns = lambda *a, **k: []
        sys.modules[f'{PKG}.utils.{stub}'] = mod

    loaded = {}
    for name in ('evaluation_suite_utils', 'evaluation_dataset_utils'):
        full = f'{PKG}.utils.{name}'
        spec = importlib.util.spec_from_file_location(
            full, PLUGIN_ROOT / 'utils' / f'{name}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
        loaded[name] = module
    return loaded


@pytest.fixture
def utils():
    saved = sys.modules.get('tools')
    modules = _install_package()
    yield modules
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    if saved is None:
        sys.modules.pop('tools', None)
    else:
        sys.modules['tools'] = saved


@pytest.fixture
def session():
    s = _Session()
    s.store[EvalSuite].append(EvalSuite(id=1, application_id=9, name='Borrower', dataset_id=5))
    # A second suite on the same shared dataset — its runs must stay unaffected.
    s.store[EvalSuite].append(EvalSuite(id=2, application_id=8, name='Owner', dataset_id=5))
    s.store[EvalSuite].append(EvalSuite(id=3, application_id=9, name='No dataset', dataset_id=None))
    for case_id in (10, 11, 12):
        s.store[EvalDatasetCase].append(
            EvalDatasetCase(id=case_id, dataset_id=5, order_index=case_id - 10))
    # A case in a dataset this suite does not use.
    s.store[EvalDatasetCase].append(EvalDatasetCase(id=99, dataset_id=6, order_index=0))
    return s


# ---------------------------------------------------------------------------
# effective_cases — the filter the run path applies
# ---------------------------------------------------------------------------

def test_no_exclusions_runs_the_whole_dataset(utils):
    suite_utils = utils['evaluation_suite_utils']
    cases = [EvalDatasetCase(id=i) for i in (10, 11, 12)]

    assert suite_utils.effective_cases(cases, set()) == cases


def test_excluded_cases_are_dropped_and_order_survives(utils):
    suite_utils = utils['evaluation_suite_utils']
    cases = [EvalDatasetCase(id=i) for i in (10, 11, 12)]

    # A run's case order is the dataset's order_index; filtering must not resequence it.
    assert [c.id for c in suite_utils.effective_cases(cases, {11})] == [10, 12]


def test_a_stale_exclusion_id_cannot_drop_a_real_case(utils):
    suite_utils = utils['evaluation_suite_utils']
    cases = [EvalDatasetCase(id=i) for i in (10, 11, 12)]

    assert len(suite_utils.effective_cases(cases, {99})) == 3


# ---------------------------------------------------------------------------
# all_cases_excluded — the guard create_batch_run turns into a 400
# ---------------------------------------------------------------------------

def test_excluding_every_case_leaves_nothing_to_run(utils):
    """create_batch_run raises EvalRunConfigError on this, rather than freezing a caseless run
    whose headline score would be an unexplained null."""
    suite_utils = utils['evaluation_suite_utils']
    cases = [EvalDatasetCase(id=i) for i in (10, 11, 12)]

    assert suite_utils.all_cases_excluded(cases, {10, 11, 12}) is True


def test_a_partly_excluded_dataset_still_runs(utils):
    suite_utils = utils['evaluation_suite_utils']
    cases = [EvalDatasetCase(id=i) for i in (10, 11, 12)]

    assert suite_utils.all_cases_excluded(cases, {10, 11}) is False


def test_an_empty_dataset_is_not_blamed_on_exclusions(utils):
    """An already-empty dataset predates the overlay, so it must not surface as "every case is
    excluded" — that error would send the caller looking for exclusions that do not exist."""
    suite_utils = utils['evaluation_suite_utils']

    assert suite_utils.all_cases_excluded([], set()) is False


# ---------------------------------------------------------------------------
# set_case_exclusions — set semantics, scoped to the suite's own dataset
# ---------------------------------------------------------------------------

def test_exclusions_are_recorded_for_the_suite(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    assert suite_utils.set_case_exclusions(1, 1, [11], session=session) == [11]
    assert suite_utils.list_case_exclusions(1, 1, session=session) == [11]


def test_excluding_leaves_the_origin_dataset_untouched(utils, session):
    """The whole point of the overlay: the shared dataset's cases are never deleted."""
    suite_utils = utils['evaluation_suite_utils']

    suite_utils.set_case_exclusions(1, 1, [11], session=session)

    assert sorted(c.id for c in session.store[EvalDatasetCase]) == [10, 11, 12, 99]


def test_another_suite_on_the_same_dataset_is_unaffected(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    suite_utils.set_case_exclusions(1, 1, [11], session=session)

    assert suite_utils.list_case_exclusions(1, 2, session=session) == []


def test_the_set_is_replaced_not_merged(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    suite_utils.set_case_exclusions(1, 1, [10, 11], session=session)
    suite_utils.set_case_exclusions(1, 1, [12], session=session)

    assert suite_utils.list_case_exclusions(1, 1, session=session) == [12]


def test_an_empty_list_restores_every_case(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    suite_utils.set_case_exclusions(1, 1, [10, 11], session=session)
    assert suite_utils.set_case_exclusions(1, 1, [], session=session) == []
    assert suite_utils.list_case_exclusions(1, 1, session=session) == []


def test_re_excluding_an_already_excluded_case_is_idempotent(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    suite_utils.set_case_exclusions(1, 1, [11], session=session)
    suite_utils.set_case_exclusions(1, 1, [11, 12], session=session)

    # The (suite_id, case_id) unique constraint would reject a duplicate insert.
    assert len(session.store[EvalSuiteCaseExclusion]) == 2
    assert suite_utils.list_case_exclusions(1, 1, session=session) == [11, 12]


def test_a_case_from_another_dataset_is_refused(utils, session):
    """Silently accepting it would look like it worked while the run kept every case."""
    suite_utils = utils['evaluation_suite_utils']

    with pytest.raises(suite_utils.EvalCaseExclusionError) as exc:
        suite_utils.set_case_exclusions(1, 1, [99], session=session)

    assert '99' in str(exc.value)
    assert exc.value.http_status == 400


def test_a_suite_without_a_dataset_has_nothing_to_exclude(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    with pytest.raises(suite_utils.EvalCaseExclusionError):
        suite_utils.set_case_exclusions(1, 3, [10], session=session)

    # ...but clearing an empty set is still a no-op, not an error.
    assert suite_utils.set_case_exclusions(1, 3, [], session=session) == []


def test_an_unknown_suite_is_a_404(utils, session):
    suite_utils = utils['evaluation_suite_utils']

    with pytest.raises(suite_utils.EvalSuiteNotFoundError) as exc:
        suite_utils.set_case_exclusions(1, 404, [10], session=session)

    assert exc.value.http_status == 404


# ---------------------------------------------------------------------------
# can_edit_dataset — the flag read responses advertise
# ---------------------------------------------------------------------------

def test_owning_agent_may_edit_its_dataset(utils):
    assert utils['evaluation_dataset_utils'].can_edit_dataset(7, 7) is True


def test_borrowing_agent_may_not_edit_a_shared_dataset(utils):
    # The reported bug: sharing grants read, never write, so the flag must say so up front
    # instead of leaving the client to discover it via a 404 on the first save.
    assert utils['evaluation_dataset_utils'].can_edit_dataset(7, 9) is False


def test_unscoped_caller_is_not_restricted(utils):
    assert utils['evaluation_dataset_utils'].can_edit_dataset(7, None) is True


def test_legacy_unowned_dataset_stays_editable(utils):
    assert utils['evaluation_dataset_utils'].can_edit_dataset(None, 9) is True
