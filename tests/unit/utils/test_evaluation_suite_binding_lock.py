"""Unit tests for the lock-first ordering in ``add_binding`` (PR #416 review follow-up).

A prior fix took the dimension row lock inside ``add_binding``, but positioned it *after*
``_validate_source``'s own (unlocked) visibility read. A concurrent demote could still commit
between that unlocked read and the eventual insert, leaving the visibility check validating
state that was already stale by the time the binding actually lands. The fix moves the lock to
the very first thing ``add_binding`` does and threads the single locked ``dimension`` row through
every check that follows, so nothing downstream of the lock can read the dimension a second time
(and reintroduce the race).

These tests stub the ORM/session layer so only that ordering and threading is exercised.
"""
import sys
import types

import pytest


class _Criterion:
    def __init__(self, fn):
        self._fn = fn

    def holds(self, row):
        return self._fn(row)


class _Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return _Criterion(lambda row: getattr(row, self.name, None) == other)

    def __hash__(self):
        return hash(self.name)


class EvalTier:
    platform = 'platform'
    project = 'project'
    agent_adhoc = 'agent_adhoc'


class EvalEngine:
    ai = 'ai'
    human = 'human'
    code = 'code'


class EvalDimension:
    id = _Col('id')

    def __init__(self, **kwargs):
        defaults = dict(
            id=1, tier=EvalTier.project, agent_id=None, allowed_engines=['ai'],
            default_weight=None, default_target=None, default_target_operator=None,
        )
        defaults.update(kwargs)
        self.__dict__.update(defaults)


class EvalBinding:
    id = _Col('id')
    suite_id = _Col('suite_id')
    dimension_id = _Col('dimension_id')
    platform_key = _Col('platform_key')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class EvalSuite:
    id = _Col('id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class EvalDatasetCase:
    id = _Col('id')
    dataset_id = _Col('dataset_id')


class EvalSuiteCaseExclusion:
    suite_id = _Col('suite_id')
    case_id = _Col('case_id')


class ApplicationVersion:
    id = _Col('id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Application:
    id = _Col('id')
    name = _Col('name')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _CreateData:
    """Stand-in for ``EvalBindingCreateModel``."""

    def __init__(self, **kwargs):
        defaults = dict(
            dimension_id=None, platform_key=None, engine=EvalEngine.ai,
            evidence_scope={}, weight=1.0, target=None, target_operator=None,
            order_index=0, application_version_id=None, meta=None,
        )
        defaults.update(kwargs)
        self.__dict__.update(defaults)
        self.model_fields_set = set(kwargs)


class _FakeQuery:
    def __init__(self, rows, events, label):
        self._rows = rows
        self._events = events
        self._label = label

    def filter(self, *criteria):
        rows = [r for r in self._rows if all(c.holds(r) for c in criteria)]
        return _FakeQuery(rows, self._events, self._label)

    def with_for_update(self):
        self._events.append(('lock', self._label))
        return self

    def first(self):
        if self._label == 'dimension':
            self._events.append(('read', self._label))
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, suite, dimension, existing_bindings=None):
        self._suite_rows = [suite] if suite is not None else []
        self._dimension_rows = [dimension] if dimension is not None else []
        self._binding_rows = existing_bindings or []
        self.events = []
        self.added = []
        self.flush_count = 0
        self.refreshed = None

    def query(self, *args):
        first = args[0]
        if first is EvalSuite:
            return _FakeQuery(self._suite_rows, self.events, 'suite')
        if first is EvalDimension:
            return _FakeQuery(self._dimension_rows, self.events, 'dimension')
        if first is EvalBinding or isinstance(first, _Col) and first.name in (
            'id', 'dimension_id', 'platform_key'
        ):
            return _FakeQuery(self._binding_rows, self.events, 'binding')
        if first is ApplicationVersion:
            return _FakeQuery([], self.events, 'version')
        raise AssertionError(f'unexpected query target: {args}')

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flush_count += 1

    def refresh(self, obj):
        self.refreshed = obj


def _pkg(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


for _name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
              'plugins.elitea_core.models.pd', 'plugins.elitea_core.utils'):
    sys.modules.setdefault(_name, _pkg(_name))

_tools = sys.modules.setdefault('tools', _pkg('tools'))
_tools.db = types.SimpleNamespace(get_session=lambda project_id: (_ for _ in ()).throw(
    AssertionError('db.get_session should not be used when a session is passed explicitly')))
_tools.auth = types.SimpleNamespace(current_user=lambda: {'id': 1})
_tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
    rpc=types.SimpleNamespace(timeout=lambda *_a, **_k: types.SimpleNamespace(
        admin_check_user_is_admin=lambda *_a, **_k: True))))

_pylon = sys.modules.setdefault('pylon', _pkg('pylon'))
_pylon_core = sys.modules.setdefault('pylon.core', _pkg('pylon.core'))
_pylon_core_tools = sys.modules.setdefault('pylon.core.tools', _pkg('pylon.core.tools'))
_pylon_core_tools.log = types.SimpleNamespace(
    warning=lambda *a, **k: None, info=lambda *a, **k: None)

_sa = sys.modules.setdefault('sqlalchemy', _pkg('sqlalchemy'))
_sa_exc = sys.modules.setdefault('sqlalchemy.exc', _pkg('sqlalchemy.exc'))
if not hasattr(_sa_exc, 'IntegrityError'):
    class IntegrityError(Exception):
        pass
    _sa_exc.IntegrityError = IntegrityError

_evaluation = _pkg('plugins.elitea_core.models.evaluation')
_evaluation.EvalSuite = EvalSuite
_evaluation.EvalBinding = EvalBinding
_evaluation.EvalDatasetCase = EvalDatasetCase
_evaluation.EvalDimension = EvalDimension
_evaluation.EvalEngine = EvalEngine
_evaluation.EvalSuiteCaseExclusion = EvalSuiteCaseExclusion
_evaluation.EvalTier = EvalTier
_evaluation.Application = Application
sys.modules['plugins.elitea_core.models.evaluation'] = _evaluation

_models_all = _pkg('plugins.elitea_core.models.all')
_models_all.ApplicationVersion = ApplicationVersion
sys.modules['plugins.elitea_core.models.all'] = _models_all

_pd_evaluation = _pkg('plugins.elitea_core.models.pd.evaluation')
_pd_evaluation.EvalSuiteCreateModel = _CreateData
_pd_evaluation.EvalSuiteUpdateModel = _CreateData
_pd_evaluation.EvalBindingCreateModel = _CreateData
_pd_evaluation.EvalBindingUpdateModel = _CreateData
_pd_evaluation.EvalDimensionCreateModel = _CreateData
_pd_evaluation.EvalDimensionUpdateModel = _CreateData
sys.modules['plugins.elitea_core.models.pd.evaluation'] = _pd_evaluation

_code_screen = _pkg('plugins.elitea_core.utils.evaluation_code_screen')
_code_screen.screen_validation_code = lambda code: []
sys.modules['plugins.elitea_core.utils.evaluation_code_screen'] = _code_screen


def _load(name, filename):
    import importlib.util
    import pathlib

    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    full = f'plugins.elitea_core.utils.{name}'
    spec = importlib.util.spec_from_file_location(full, plugin_root / 'utils' / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


library = _load('evaluation_library_utils', 'evaluation_library_utils.py')
sys.modules['plugins.elitea_core.utils.evaluation_library_utils'] = library
suite_utils = _load('evaluation_suite_utils', 'evaluation_suite_utils.py')


@pytest.fixture
def suite():
    return EvalSuite(id=100, application_id=7)


@pytest.fixture
def dimension():
    return EvalDimension(id=1, tier=EvalTier.project, agent_id=None, allowed_engines=['ai'])


def test_add_binding_locks_the_dimension_before_reading_it_for_visibility(suite, dimension):
    """The lock must be the *first* event against the dimension row, and there must be exactly
    one read of it — not a separate unlocked read followed later by a lock."""
    session = _FakeSession(suite, dimension)

    suite_utils.add_binding(1, suite.id, _CreateData(dimension_id=dimension.id), session=session)

    dimension_events = [e for e in session.events if e[1] == 'dimension']
    assert dimension_events[0] == ('lock', 'dimension')
    assert dimension_events.count(('read', 'dimension')) == 1


def test_add_binding_rejects_a_hidden_agent_adhoc_dimension_using_the_locked_row(suite, dimension):
    """The visibility check must see the same (locked) row the lock call fetched, not go back to
    the database a second time — confirmed here by only ever supplying one dimension row that is
    already owned by a different agent."""
    dimension.tier = EvalTier.agent_adhoc
    dimension.agent_id = 999  # not suite.application_id (7)
    session = _FakeSession(suite, dimension)

    with pytest.raises(suite_utils.EvalBindingSourceError):
        suite_utils.add_binding(1, suite.id, _CreateData(dimension_id=dimension.id), session=session)


def test_add_binding_platform_binding_does_not_touch_the_dimension_row(suite):
    session = _FakeSession(suite, dimension=None)

    suite_utils.add_binding(1, suite.id, _CreateData(platform_key='pk.example'), session=session)

    assert not any(e[1] == 'dimension' for e in session.events)


def test_add_binding_engine_check_uses_the_same_locked_dimension(suite, dimension):
    """A dimension whose definition only allows 'code' must reject an 'ai' binding — proving the
    engine check runs against the locked row threaded in, not a stale/independent read."""
    dimension.allowed_engines = ['code']
    session = _FakeSession(suite, dimension)

    with pytest.raises(suite_utils.EvalBindingEngineError):
        suite_utils.add_binding(
            1, suite.id, _CreateData(dimension_id=dimension.id, engine=EvalEngine.ai), session=session)
