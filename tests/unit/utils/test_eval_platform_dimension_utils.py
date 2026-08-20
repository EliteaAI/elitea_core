"""Unit tests for the platform eval dimension projection (§16.1).

The util does real DB work through ``db.get_session``, so the tools/ORM/model imports are
stubbed with in-memory fakes: one list of projected rows per project schema.
"""
import contextlib
import importlib.util
import pathlib
import sys
import types
import uuid as uuid_module

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


class _Row:
    # Class attributes so the util's `Model.column == value` filter expressions resolve;
    # the fake query ignores the predicate and filters in Python instead.
    tier = None
    uuid = None
    name = None
    is_active = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    def __init__(self, store, on_commit=None):
        self._store = store
        self._pending = []
        self._on_commit = on_commit

    def query(self, _model):
        return _FakeQuery(self._store)

    def add(self, row):
        self._pending.append(row)

    def commit(self):
        self._store.extend(self._pending)
        self._pending = []
        if self._on_commit is not None:
            self._on_commit()

    def expunge(self, _row):
        pass


class _FakeDb:
    """Holds one row list per project id; ``broken`` ids raise on session open."""

    def __init__(self):
        self.schemas = {}
        self.broken = set()
        self.committed = []

    @contextlib.contextmanager
    def get_session(self, project_id):
        if project_id in self.broken:
            raise RuntimeError(f'schema p_{project_id} is broken')
        yield _FakeSession(
            self.schemas.setdefault(project_id, []),
            on_commit=lambda: self.committed.append(project_id),
        )

    @contextlib.contextmanager
    def with_project_schema_session(self, _project_id):
        yield _FakeSession(self.schemas.setdefault('shared', []))


FAKE_DB = _FakeDb()


def _pkg(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


for _name in (
    'plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
    'plugins.elitea_core.models.pd', 'plugins.elitea_core.utils',
):
    sys.modules.setdefault(_name, _pkg(_name))

_pylon = sys.modules.setdefault('pylon', _pkg('pylon'))
_pylon_core = sys.modules.setdefault('pylon.core', _pkg('pylon.core'))
_pylon_tools = sys.modules.setdefault('pylon.core.tools', _pkg('pylon.core.tools'))
if not hasattr(_pylon_tools, 'log'):
    _log = types.SimpleNamespace(
        exception=lambda *a, **k: None, info=lambda *a, **k: None,
        warning=lambda *a, **k: None, error=lambda *a, **k: None,
    )
    _pylon_tools.log = _log

_sa = sys.modules.setdefault('sqlalchemy', _pkg('sqlalchemy'))
_sa_exc = sys.modules.setdefault('sqlalchemy.exc', _pkg('sqlalchemy.exc'))
if not hasattr(_sa_exc, 'IntegrityError'):
    class IntegrityError(Exception):
        pass

    _sa_exc.IntegrityError = IntegrityError

_tools = sys.modules.setdefault('tools', _pkg('tools'))
_tools.db = FAKE_DB
_tools.context = types.SimpleNamespace()

_registry_model = _pkg('plugins.elitea_core.models.eval_platform_dimension')
_registry_model.EvalPlatformDimension = _Row
sys.modules['plugins.elitea_core.models.eval_platform_dimension'] = _registry_model

_vocab = _pkg('plugins.elitea_core.models.evaluation')
_vocab.EvalDimension = _Row
_vocab.EvalEngine = types.SimpleNamespace(ai='ai', human='human', code='code')
_vocab.EvalTier = types.SimpleNamespace(
    platform='platform', project='project', agent_adhoc='agent_adhoc',
)
sys.modules['plugins.elitea_core.models.evaluation'] = _vocab

_pd = _pkg('plugins.elitea_core.models.pd.eval_platform_dimension')
_pd.EvalPlatformDimensionCreateModel = _Row
_pd.EvalPlatformDimensionUpdateModel = _Row
sys.modules['plugins.elitea_core.models.pd.eval_platform_dimension'] = _pd

_library = _pkg('plugins.elitea_core.utils.evaluation_library_utils')


class _EvalLibraryError(Exception):
    http_status = 400


_library.EvalLibraryError = _EvalLibraryError
sys.modules['plugins.elitea_core.utils.evaluation_library_utils'] = _library


def _load():
    name = 'plugins.elitea_core.utils.eval_platform_dimension_utils'
    spec = importlib.util.spec_from_file_location(
        name, PLUGIN_ROOT / 'utils' / 'eval_platform_dimension_utils.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


platform_dimensions = _load()


def _entry(name='Toxicity', **overrides):
    values = dict(
        uuid=uuid_module.uuid4(), name=name, description='rubric',
        allowed_engines=['ai'],
        scale_type='continuous', scale_min=0.0, scale_max=100.0,
        polarity='lower_better', default_weight=1.0, default_target=None,
        default_target_operator=None, is_active=True, owner_id=None, meta=None,
    )
    values.update(overrides)
    return _Row(**values)


@pytest.fixture(autouse=True)
def clean_db():
    FAKE_DB.schemas.clear()
    FAKE_DB.broken.clear()
    FAKE_DB.committed.clear()
    yield


# --- project_to ---------------------------------------------------------------

def test_project_to_inserts_once(clean_db):
    entry = _entry()
    result = platform_dimensions.project_to(2, [entry])
    assert (result['inserted'], result['updated']) == (1, 0)
    assert len(FAKE_DB.schemas[2]) == 1

    row = FAKE_DB.schemas[2][0]
    assert row.tier == 'platform'
    assert row.allowed_engines == ['ai']
    assert row.meta == {}
    # eval_dimension.owner_id is NOT NULL, the registry allows no owner.
    assert row.owner_id == 0


def test_human_engines_are_projected(clean_db):
    # A platform dimension may be scored by a human just like a project-tier one.
    platform_dimensions.project_to(2, [_entry(allowed_engines=['ai', 'human'])])
    assert FAKE_DB.schemas[2][0].allowed_engines == ['ai', 'human']


def test_project_to_is_idempotent(clean_db):
    entry = _entry()
    platform_dimensions.project_to(2, [entry])
    result = platform_dimensions.project_to(2, [entry])
    assert (result['inserted'], result['updated']) == (0, 1)
    assert len(FAKE_DB.schemas[2]) == 1


def test_rename_updates_in_place(clean_db):
    entry = _entry()
    platform_dimensions.project_to(2, [entry])

    entry.name = 'Renamed'
    result = platform_dimensions.project_to(2, [entry])

    assert result['inserted'] == 0
    assert len(FAKE_DB.schemas[2]) == 1
    assert FAKE_DB.schemas[2][0].name == 'Renamed'


def test_matching_is_on_uuid_not_name(clean_db):
    platform_dimensions.project_to(2, [_entry()])
    # Same name, different uuid: a distinct registry entry, so a distinct projected row.
    platform_dimensions.project_to(2, [_entry()])
    assert len(FAKE_DB.schemas[2]) == 2


def test_deactivation_is_not_projected(clean_db):
    # is_active gates the catalog and materialize(), not the projected copy: a project that
    # already attached keeps its row (and its bindings/results) working after deactivation.
    entry = _entry()
    platform_dimensions.project_to(2, [entry])

    entry.is_active = False
    platform_dimensions.project_to(2, [entry])

    assert len(FAKE_DB.schemas[2]) == 1
    assert 'platform_active' not in FAKE_DB.schemas[2][0].meta


def test_existing_meta_is_preserved(clean_db):
    platform_dimensions.project_to(2, [_entry(meta={'source': 'registry'})])
    assert FAKE_DB.schemas[2][0].meta == {'source': 'registry'}


def test_update_only_never_inserts(clean_db):
    result = platform_dimensions.project_to(2, [_entry()], insert_missing=False)
    assert (result['inserted'], result['skipped']) == (0, 1)
    assert FAKE_DB.schemas[2] == []


# --- materialize --------------------------------------------------------------

def test_materialize_inserts_and_returns_local_row(clean_db, monkeypatch):
    entry = _entry()
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)

    row = platform_dimensions.materialize(2, str(entry.uuid))

    assert len(FAKE_DB.schemas[2]) == 1
    assert row is FAKE_DB.schemas[2][0]
    assert row.name == 'Toxicity'


def test_materialize_is_idempotent(clean_db, monkeypatch):
    entry = _entry()
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)

    first = platform_dimensions.materialize(2, str(entry.uuid))
    second = platform_dimensions.materialize(2, str(entry.uuid))

    # A second attach must reuse the row, or the caller's binding would point at a stale id.
    assert first is second
    assert len(FAKE_DB.schemas[2]) == 1


def test_materialize_refuses_unknown_uuid(clean_db, monkeypatch):
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: None)
    with pytest.raises(platform_dimensions.EvalPlatformDimensionNotFoundError):
        platform_dimensions.materialize(2, 'nope')


def test_materialize_refuses_inactive_dimension(clean_db, monkeypatch):
    entry = _entry(is_active=False)
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)

    with pytest.raises(platform_dimensions.EvalPlatformDimensionInactiveError):
        platform_dimensions.materialize(2, str(entry.uuid))
    assert FAKE_DB.schemas.get(2, []) == []


# --- resync -------------------------------------------------------------------

def test_resync_updates_only_projects_holding_the_row(clean_db, monkeypatch):
    entry = _entry()
    monkeypatch.setattr(platform_dimensions, '_active_project_ids', lambda: [1, 2, 3])
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)
    platform_dimensions.project_to(2, [entry])

    entry.description = 'new rubric'
    result = platform_dimensions.resync_dimension(str(entry.uuid))

    assert [item['project_id'] for item in result['synced']] == [2]
    assert FAKE_DB.schemas[2][0].description == 'new rubric'
    # Projects that never attached it stay empty rather than gaining a copy.
    assert FAKE_DB.schemas[1] == [] and FAKE_DB.schemas[3] == []


def test_resync_does_not_open_a_write_transaction_on_projects_without_the_row(clean_db, monkeypatch):
    """Review #33: the single-dimension Sync ran a commit in *every* active schema, which is
    what pushed it past the request timeout. Only holders may be written to."""
    entry = _entry()
    monkeypatch.setattr(platform_dimensions, '_active_project_ids', lambda: [1, 2, 3])
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)
    platform_dimensions.project_to(2, [entry])
    FAKE_DB.committed.clear()

    platform_dimensions.resync_dimension(str(entry.uuid))

    assert FAKE_DB.committed == [2]


def test_resync_of_an_empty_registry_touches_nothing(clean_db, monkeypatch):
    monkeypatch.setattr(platform_dimensions, '_active_project_ids', lambda: [1, 2, 3])
    monkeypatch.setattr(platform_dimensions, 'list_registry', lambda: [])

    result = platform_dimensions.resync_all()

    assert result['synced_projects'] == 0
    assert FAKE_DB.committed == []


def test_resync_survives_one_broken_schema(clean_db, monkeypatch):
    entry = _entry()
    monkeypatch.setattr(platform_dimensions, '_active_project_ids', lambda: [1, 2, 3])
    monkeypatch.setattr(platform_dimensions, 'get_registry', lambda _uuid: entry)
    platform_dimensions.project_to(1, [entry])
    platform_dimensions.project_to(3, [entry])
    FAKE_DB.broken.add(2)

    result = platform_dimensions.resync_dimension(str(entry.uuid))

    assert [failure['project_id'] for failure in result['failures']] == [2]
    assert [item['project_id'] for item in result['synced']] == [1, 3]
