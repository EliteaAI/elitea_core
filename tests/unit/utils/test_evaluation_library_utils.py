"""Unit tests for the merged-state validation added to ``update_dimension`` (PR #366 review,
discussion_r3875690565).

``EvalDimensionUpdateModel`` only checks the code/engine pairing invariant when the caller's
request explicitly sends ``allowed_engines`` (``exclude_unset``): a bare ``{"code": "..."}`` PUT
on an AI/Human dimension sails through the Pydantic layer untouched. ``update_dimension`` must
therefore re-check the invariant against the *merged* row (existing columns overlaid with only the
fields the request actually sent) and must refuse an ``allowed_engines`` change that would strand
an existing ``EvalBinding`` whose stored ``engine`` (fixed at bind time) would no longer be
allowed. These tests stub the ORM/session layer so only that merge logic is exercised.
"""
import sys
import types

import pytest


class _Criterion:
    def __init__(self, fn):
        self._fn = fn

    def holds(self, row):
        return self._fn(row)

    def __invert__(self):
        return _Criterion(lambda row: not self._fn(row))


class _Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return _Criterion(lambda row: getattr(row, self.name, None) == other)

    def __hash__(self):
        return hash(self.name)

    def in_(self, values):
        wanted = set(values)
        return _Criterion(lambda row: getattr(row, self.name, None) in wanted)


class EvalTier:
    platform = 'platform'
    project = 'project'
    agent_adhoc = 'agent_adhoc'


class EvalDimension:
    id = _Col('id')

    def __init__(self, **kwargs):
        defaults = dict(
            id=1, tier=EvalTier.project, name='Dim', agent_id=None,
            allowed_engines=['ai'], code=None, return_contract=None,
        )
        defaults.update(kwargs)
        self.__dict__.update(defaults)


class EvalBinding:
    id = _Col('id')
    dimension_id = _Col('dimension_id')
    engine = _Col('engine')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _UpdateModel:
    """Stand-in for ``EvalDimensionUpdateModel``: echoes only the fields the 'request' set,
    mirroring ``exclude_unset=True`` — omitted keys never appear in ``model_dump``."""

    def __init__(self, **fields):
        self._fields = fields

    def model_dump(self, exclude_unset=True):  # noqa: ARG002 - always True for this model
        return dict(self._fields)


class _FakeQuery:
    def __init__(self, rows, columns=None):
        self._rows = rows
        self._columns = columns

    def filter(self, *criteria):
        rows = [r for r in self._rows if all(c.holds(r) for c in criteria)]
        return _FakeQuery(rows, self._columns)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        if self._columns is None:
            return list(self._rows)
        return [tuple(getattr(r, name) for name in self._columns) for r in self._rows]


class _FakeSession:
    def __init__(self, dimension, bindings):
        self._dimension_rows = [dimension] if dimension is not None else []
        self._binding_rows = bindings
        self.flush_count = 0
        self.rolled_back = False
        self.refreshed = None

    def query(self, *args):
        first = args[0]
        if first is EvalDimension:
            return _FakeQuery(self._dimension_rows)
        if isinstance(first, _Col):
            return _FakeQuery(self._binding_rows, columns=[a.name for a in args])
        raise AssertionError(f'unexpected query target: {args}')

    def flush(self):
        self.flush_count += 1

    def rollback(self):
        self.rolled_back = True

    def refresh(self, _obj):
        self.refreshed = _obj


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

_sa = sys.modules.setdefault('sqlalchemy', _pkg('sqlalchemy'))
_sa_exc = sys.modules.setdefault('sqlalchemy.exc', _pkg('sqlalchemy.exc'))
if not hasattr(_sa_exc, 'IntegrityError'):
    class IntegrityError(Exception):
        pass
    _sa_exc.IntegrityError = IntegrityError

_evaluation = _pkg('plugins.elitea_core.models.evaluation')
_evaluation.EvalDimension = EvalDimension
_evaluation.EvalTier = EvalTier
_evaluation.EvalBinding = EvalBinding
sys.modules['plugins.elitea_core.models.evaluation'] = _evaluation

_pd_evaluation = _pkg('plugins.elitea_core.models.pd.evaluation')
_pd_evaluation.EvalDimensionCreateModel = _UpdateModel
_pd_evaluation.EvalDimensionUpdateModel = _UpdateModel
sys.modules['plugins.elitea_core.models.pd.evaluation'] = _pd_evaluation

_code_screen = _pkg('plugins.elitea_core.utils.evaluation_code_screen')
_code_screen.screen_validation_code = lambda code: []
sys.modules['plugins.elitea_core.utils.evaluation_code_screen'] = _code_screen


def _load():
    import importlib.util
    import pathlib

    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    name = 'plugins.elitea_core.utils.evaluation_library_utils'
    spec = importlib.util.spec_from_file_location(
        name, plugin_root / 'utils' / 'evaluation_library_utils.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


library = _load()


@pytest.fixture(autouse=True)
def _pin_evaluation_models_module():
    """``update_dimension`` imports ``EvalBinding`` lazily (inside the function, not at module
    load time), so it resolves ``plugins.elitea_core.models.evaluation`` from ``sys.modules`` at
    *call* time. Other unit test modules (e.g. ``test_eval_platform_dimension_utils.py``) register
    their own incompatible stub under the same key without restoring it, so pin ours back before
    every test regardless of run order, then restore whatever was there before."""
    previous = sys.modules.get('plugins.elitea_core.models.evaluation')
    sys.modules['plugins.elitea_core.models.evaluation'] = _evaluation
    yield
    if previous is None:
        sys.modules.pop('plugins.elitea_core.models.evaluation', None)
    else:
        sys.modules['plugins.elitea_core.models.evaluation'] = previous


@pytest.fixture
def dimension():
    return EvalDimension(id=1, tier=EvalTier.project, allowed_engines=['ai'], code=None,
                          return_contract=None)


@pytest.fixture
def code_dimension():
    return EvalDimension(id=1, tier=EvalTier.project, allowed_engines=['code'],
                          code='return True', return_contract='bool')


def _update(dimension, bindings=None, **fields):
    session = _FakeSession(dimension, bindings or [])
    return library.update_dimension(
        project_id=1, dimension_id=dimension.id, data=_UpdateModel(**fields), session=session,
    ), session


# ---------------------------------------------------------------------------
# Merged-state validation of the code/engine pairing invariant
# ---------------------------------------------------------------------------

def test_bare_code_put_on_an_ai_dimension_is_rejected_even_without_allowed_engines(dimension):
    """The exact bug from the review comment: allowed_engines is never sent, so the Pydantic
    model's own check is skipped, but the merged row (ai engines + a code body) is still invalid."""
    with pytest.raises(library.EvalDimensionEngineFieldsError):
        _update(dimension, code='return True')


def test_setting_return_contract_alone_on_an_ai_dimension_is_rejected(dimension):
    with pytest.raises(library.EvalDimensionEngineFieldsError):
        _update(dimension, return_contract='number')


def test_switching_to_code_engine_without_a_code_body_is_rejected(dimension):
    with pytest.raises(library.EvalDimensionEngineFieldsError):
        _update(dimension, allowed_engines=['code'])


def test_switching_to_code_engine_with_a_code_body_defaults_the_contract(dimension):
    result, _ = _update(dimension, allowed_engines=['code'], code='return True')
    assert result.return_contract == 'bool'


def test_switching_a_code_dimension_back_to_ai_requires_clearing_code(code_dimension):
    """allowed_engines flips away from ['code'] but the (unset) request still carries the old
    code/contract via the existing row — the merged state is still code-shaped, so it's fine;
    but the caller can't just flip allowed_engines and leave the code body in place."""
    with pytest.raises(library.EvalDimensionEngineFieldsError):
        _update(code_dimension, allowed_engines=['ai'])


def test_switching_to_ai_while_clearing_code_and_contract_succeeds(code_dimension):
    result, _ = _update(code_dimension, allowed_engines=['ai'], code=None, return_contract=None)
    assert result.allowed_engines == ['ai']
    assert result.code is None


def test_code_only_edit_on_an_existing_code_dimension_is_allowed(code_dimension):
    """The scenario the Pydantic model's exclude_unset skip exists to support: editing the script
    body of an already-code dimension without re-sending allowed_engines."""
    result, _ = _update(code_dimension, code='return False')
    assert result.code == 'return False'
    assert result.allowed_engines == ['code']


# ---------------------------------------------------------------------------
# Binding-conflict guard when allowed_engines actually changes
# ---------------------------------------------------------------------------

def test_narrowing_allowed_engines_away_from_a_bound_engine_is_rejected(code_dimension):
    bindings = [EvalBinding(id=10, dimension_id=1, engine='code'),
                EvalBinding(id=11, dimension_id=1, engine='code')]
    with pytest.raises(library.EvalDimensionEngineBindingConflictError) as excinfo:
        _update(code_dimension, bindings,
                allowed_engines=['ai'], code=None, return_contract=None)
    assert sorted(excinfo.value.binding_ids) == [10, 11]
    assert excinfo.value.engine == 'code'


def test_bindings_on_other_dimensions_do_not_block_the_change(code_dimension):
    bindings = [EvalBinding(id=20, dimension_id=999, engine='code')]
    result, _ = _update(code_dimension, bindings,
                         allowed_engines=['ai'], code=None, return_contract=None)
    assert result.allowed_engines == ['ai']


def test_widening_allowed_engines_never_strands_a_binding(dimension):
    """ai -> [ai, human]: existing engine='ai' bindings stay valid, no conflict check needed."""
    bindings = [EvalBinding(id=30, dimension_id=1, engine='ai')]
    result, _ = _update(dimension, bindings, allowed_engines=['ai', 'human'])
    assert result.allowed_engines == ['ai', 'human']


def test_unchanged_allowed_engines_skips_the_binding_check_even_with_stale_data(dimension):
    """If the request omits allowed_engines (or resends the same value), there's nothing new to
    conflict with — the binding query must not even run against unrelated engine values."""
    bindings = [EvalBinding(id=40, dimension_id=1, engine='human')]
    result, _ = _update(dimension, bindings, name='Renamed')
    assert result.name == 'Renamed'


def test_binding_conflict_error_message_names_the_stranded_engine(code_dimension):
    bindings = [EvalBinding(id=50, dimension_id=1, engine='code')]
    with pytest.raises(library.EvalDimensionEngineBindingConflictError) as excinfo:
        _update(code_dimension, bindings, allowed_engines=['ai'], code=None, return_contract=None)
    assert 'code' in str(excinfo.value)
    assert '50' in str(excinfo.value)
