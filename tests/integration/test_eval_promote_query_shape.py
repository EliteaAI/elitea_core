"""Integration tests for the eval promote/import read shapes (review #336, T2.2 / T2.3).

Two round-trip bugs on the dataset write path, both invisible from the outside because the returned
data was correct either way:

* ``extract_conversation_turns`` lazy-loaded ``author_participant`` and ``message_items`` for every
  group and had no row cap, so promoting a long conversation cost 2N queries inside a request.
* ``_append_rows`` called ``session.refresh`` once per created row — up to ``MAX_CASES`` (5000)
  sequential statements during an import.

Neither is observable in a unit test of the pure helpers, so the sessions here *record* what was
asked of them and the assertions are about the query shape, not the result.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_promote_shape_test'


# ---------------------------------------------------------------------------
# fake ORM: columns that record comparisons, a query that records the chain
# ---------------------------------------------------------------------------

class _Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ('eq', self.name, other)

    def __ge__(self, other):
        return ('ge', self.name, other)

    def __lt__(self, other):
        return ('lt', self.name, other)

    def __hash__(self):
        return hash(self.name)

    def asc(self):
        return ('asc', self.name)


class _Relation:
    def __init__(self, name):
        self.name = name


class _RecordingQuery:
    def __init__(self, log, rows):
        self._log = log
        self._rows = rows

    def filter(self, *criteria):
        self._log.append(('filter', criteria))
        return self

    def options(self, *opts):
        self._log.append(('options', opts))
        return self

    def order_by(self, *cols):
        self._log.append(('order_by', cols))
        return self

    def limit(self, n):
        self._log.append(('limit', n))
        return self

    def offset(self, n):
        self._log.append(('offset', n))
        return self

    def populate_existing(self):
        self._log.append(('populate_existing', None))
        return self

    def with_for_update(self):
        self._log.append(('with_for_update', None))
        return self

    def all(self):
        self._log.append(('all', None))
        return list(self._rows)

    def first(self):
        self._log.append(('first', None))
        return self._rows[0] if self._rows else None

    def count(self):
        self._log.append(('count', None))
        return len(self._rows)

    def scalar(self):
        self._log.append(('scalar', None))
        return None


class _RecordingSession:
    def __init__(self, rows=()):
        self.log = []
        self.rows = list(rows)
        self.added = []
        self.flushes = 0
        self.refreshes = []

    def query(self, *targets):
        self.log.append(('query', targets))
        return _RecordingQuery(self.log, self.rows)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flushes += 1

    def refresh(self, obj):
        self.refreshes.append(obj)

    def close(self):
        pass


# ---------------------------------------------------------------------------
# module loading
# ---------------------------------------------------------------------------

def _base_package():
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = []

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id: _RecordingSession())
    tools.auth = types.SimpleNamespace(current_user=lambda: {'id': 1})
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda *_a, **_k: types.SimpleNamespace(
            admin_check_user_is_admin=lambda *_a, **_k: True))))

    for name, mod in {
        PKG: pkg,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.pd': pd_pkg,
        'tools': tools,
    }.items():
        sys.modules[name] = mod


def _load(module_name):
    full = f'{PKG}.utils.{module_name}'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'utils' / f'{module_name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cleanup():
    saved = sys.modules.get('tools')
    yield
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    if saved is None:
        sys.modules.pop('tools', None)
    else:
        sys.modules['tools'] = saved


# ---------------------------------------------------------------------------
# T2.2 — turn extraction eager-loads and caps
# ---------------------------------------------------------------------------

class _Group:
    conversation_id = _Column('conversation_id')
    created_at = _Column('created_at')
    id = _Column('id')
    author_participant = _Relation('author_participant')
    message_items = _Relation('message_items')


@pytest.fixture
def extraction(cleanup, monkeypatch):
    # Real sqlalchemy is importable here and would reject the fake relations; the assertions are
    # about *which* attributes the production code asks to eager-load, not about the strategy.
    import sqlalchemy.orm
    monkeypatch.setattr(sqlalchemy.orm, 'selectinload', lambda attr: attr)
    _base_package()
    conversation = types.ModuleType(f'{PKG}.models.conversation')
    conversation.Conversation = object
    message_group = types.ModuleType(f'{PKG}.models.message_group')
    message_group.ConversationMessageGroup = _Group
    sys.modules[f'{PKG}.models.conversation'] = conversation
    sys.modules[f'{PKG}.models.message_group'] = message_group
    return _load('evaluation_turn_extraction')


def test_promote_eager_loads_both_lazily_touched_relations(extraction):
    session = _RecordingSession(rows=[])

    extraction.extract_conversation_turns(1, 42, session=session)

    options = [entry for kind, entry in session.log if kind == 'options']
    assert options, 'the group read must declare eager loading, not fall back to lazy'
    loaded = {opt.name for group in options for opt in group}
    assert loaded == {'author_participant', 'message_items'}


def test_promote_caps_the_number_of_groups_it_reads(extraction):
    session = _RecordingSession(rows=[])

    extraction.extract_conversation_turns(1, 42, session=session)

    limits = [value for kind, value in session.log if kind == 'limit']
    assert limits == [extraction.MAX_GROUPS]
    assert extraction.MAX_GROUPS > 0


def test_promote_still_orders_groups_deterministically(extraction):
    """The cap only makes sense against a stable order — otherwise it truncates at random."""
    session = _RecordingSession(rows=[])

    extraction.extract_conversation_turns(1, 42, session=session)

    ordering = [cols for kind, cols in session.log if kind == 'order_by']
    assert [c[1] for c in ordering[0]] == ['created_at', 'id']


# ---------------------------------------------------------------------------
# T2.3 — _append_rows reads the block back once
# ---------------------------------------------------------------------------

class _Case:
    dataset_id = _Column('dataset_id')
    order_index = _Column('order_index')
    id = _Column('id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Dataset:
    id = _Column('id')


def _permissive(module, explicit):
    """A stub module that conjures a placeholder class for any name imported from it.

    The dataset utils pull in the whole eval model package transitively; only the handful of
    names in ``explicit`` matter to these assertions.
    """
    for name, value in explicit.items():
        setattr(module, name, value)
    module.__getattr__ = lambda name: explicit.get(name) or type(name, (), {})
    return module


@pytest.fixture
def dataset_utils(cleanup):
    _base_package()

    explicit = {
        'EvalDataset': _Dataset,
        'EvalDatasetCase': _Case,
        'EvalCaseSource': types.SimpleNamespace(
            import_='import', conversation='conversation', manual='manual'),
    }
    sys.modules[f'{PKG}.models.evaluation'] = _permissive(
        types.ModuleType(f'{PKG}.models.evaluation'), explicit)
    sys.modules[f'{PKG}.models.pd.evaluation'] = _permissive(
        types.ModuleType(f'{PKG}.models.pd.evaluation'), {})
    sys.modules[f'{PKG}.models.all'] = _permissive(
        types.ModuleType(f'{PKG}.models.all'), {'Application': type('Application', (), {})})
    for extra in ('conversation', 'message_group'):
        sys.modules[f'{PKG}.models.{extra}'] = _permissive(
            types.ModuleType(f'{PKG}.models.{extra}'), {})

    return _load('evaluation_dataset_utils')


def test_appending_rows_issues_no_per_row_refresh(dataset_utils, monkeypatch):
    monkeypatch.setattr(dataset_utils, 'MAX_CASES_PER_DATASET', 250)
    session = _RecordingSession(rows=[])
    rows = [{'input': f'q{i}'} for i in range(250)]

    dataset_utils._append_rows(session, 4, rows, 'import')

    assert len(session.added) == 250
    # The regression this guards: N rows used to mean N sequential `refresh` statements.
    assert session.refreshes == []
    assert session.flushes >= 1


def test_appending_rows_reads_the_whole_block_back_in_one_query(dataset_utils, monkeypatch):
    """`created_at` is a server_default, so the block still has to be re-read — but once."""
    monkeypatch.setattr(dataset_utils, 'MAX_CASES_PER_DATASET', 50)
    session = _RecordingSession(rows=[])

    dataset_utils._append_rows(
        session, 4, [{'input': 'q'} for _ in range(50)], 'import')

    reads = [entry for entry in session.log if entry[0] == 'all']
    assert len(reads) == 1
    assert ('populate_existing', None) in session.log


def test_appending_no_rows_touches_the_database_only_for_the_order_index(dataset_utils):
    session = _RecordingSession(rows=[])

    created = dataset_utils._append_rows(session, 4, [], 'import')

    assert created == []
    assert session.refreshes == []
    assert not any(entry[0] == 'populate_existing' for entry in session.log)


# ---------------------------------------------------------------------------
# #6349 — 10-case-per-dataset hard cap, enforced atomically before any row is added
# ---------------------------------------------------------------------------

def test_append_rows_rejects_a_bulk_import_that_would_exceed_the_cap(dataset_utils):
    limit = dataset_utils.MAX_CASES_PER_DATASET
    session = _RecordingSession(rows=[object()] * (limit - 1))  # dataset already has limit-1 cases

    with pytest.raises(dataset_utils.EvalDatasetCaseLimitError):
        dataset_utils._append_rows(session, 4, [{'input': 'q'} for _ in range(2)], 'import')

    # All-or-nothing: nothing was added once the cap check failed.
    assert session.added == []


def test_append_rows_allows_a_bulk_import_that_exactly_fills_the_cap(dataset_utils):
    limit = dataset_utils.MAX_CASES_PER_DATASET
    session = _RecordingSession(rows=[object()] * (limit - 2))

    created = dataset_utils._append_rows(session, 4, [{'input': 'q'} for _ in range(2)], 'import')

    assert len(session.added) == 2
    assert created is not None


def test_add_case_rejects_the_case_at_the_cap(dataset_utils):
    limit = dataset_utils.MAX_CASES_PER_DATASET
    session = _RecordingSession(rows=[object()] * limit)

    with pytest.raises(dataset_utils.EvalDatasetCaseLimitError):
        dataset_utils.add_case(1, 4, types.SimpleNamespace(
            input='q', variables=None, expected_output=None, source_type='manual',
            source_ref=None, meta=None,
        ), session=session)

    assert session.added == []


def test_case_limit_error_message_names_the_limit_for_a_single_add(dataset_utils):
    exc = dataset_utils.EvalDatasetCaseLimitError(dataset_utils.MAX_CASES_PER_DATASET, 1)

    assert str(dataset_utils.MAX_CASES_PER_DATASET) in str(exc)
    assert exc.http_status == 400


def test_case_limit_error_message_names_the_row_count_for_a_bulk_import(dataset_utils):
    exc = dataset_utils.EvalDatasetCaseLimitError(0, 25)

    assert '25' in str(exc)
    assert str(dataset_utils.MAX_CASES_PER_DATASET) in str(exc)


# ---------------------------------------------------------------------------
# #6350 — agent scoping / opt-in sharing access checks
# ---------------------------------------------------------------------------

def _dataset(agent_id=None, is_shared=False):
    return types.SimpleNamespace(id=4, agent_id=agent_id, is_shared=is_shared)


def test_unscoped_caller_bypasses_the_access_check(dataset_utils):
    # agent_id=None means the caller didn't opt into scoping (pre-#6350 contract).
    dataset_utils._check_dataset_access(_dataset(agent_id=5, is_shared=False), None, require_owner=True)


def test_legacy_dataset_with_no_agent_is_accessible_to_any_agent(dataset_utils):
    dataset_utils._check_dataset_access(_dataset(agent_id=None), agent_id=5, require_owner=True)


def test_owner_always_has_read_and_write_access(dataset_utils):
    dataset_utils._check_dataset_access(_dataset(agent_id=5, is_shared=False), agent_id=5, require_owner=True)


def test_non_owner_can_read_a_shared_dataset(dataset_utils):
    dataset_utils._check_dataset_access(_dataset(agent_id=5, is_shared=True), agent_id=9, require_owner=False)


def test_non_owner_cannot_write_a_shared_dataset(dataset_utils):
    with pytest.raises(dataset_utils.EvalDatasetNotFoundError):
        dataset_utils._check_dataset_access(_dataset(agent_id=5, is_shared=True), agent_id=9, require_owner=True)


def test_non_owner_cannot_read_a_private_dataset(dataset_utils):
    with pytest.raises(dataset_utils.EvalDatasetNotFoundError):
        dataset_utils._check_dataset_access(_dataset(agent_id=5, is_shared=False), agent_id=9, require_owner=False)
