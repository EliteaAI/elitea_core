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

    def __ne__(self, other):
        return _Criterion(lambda row: getattr(row, self.name, None) != other)

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
    suite_id = _Col('suite_id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class EvalSuite:
    id = _Col('id')
    application_id = _Col('application_id')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Application:
    id = _Col('id')
    name = _Col('name')

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
    def __init__(self, rows, columns=None, lock_sink=None):
        self._rows = rows
        self._columns = columns
        self._lock_sink = lock_sink

    def filter(self, *criteria):
        rows = [r for r in self._rows if all(c.holds(r) for c in criteria)]
        return _FakeQuery(rows, self._columns, self._lock_sink)

    def join(self, *args, **kwargs):  # noqa: ARG002 - joins are pre-baked into the fixture rows
        return self

    def distinct(self):
        seen = []
        for r in self._rows:
            key = tuple(getattr(r, name) for name in self._columns) if self._columns else id(r)
            if key not in [s[0] for s in seen]:
                seen.append((key, r))
        return _FakeQuery([r for _, r in seen], self._columns, self._lock_sink)

    def with_for_update(self):
        """No real locking in this fake ORM; records that a lock was requested so tests can
        assert the demote path (and, in evaluation_suite_utils.py, add_binding) actually take
        the row lock rather than just documenting it (PR #416 review)."""
        if self._lock_sink is not None:
            self._lock_sink.append(True)
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        if self._columns is None:
            return list(self._rows)
        return [tuple(getattr(r, name) for name in self._columns) for r in self._rows]


class _FakeSession:
    def __init__(self, dimension, bindings, other_agent_bindings=None, known_agent_ids=None):
        self._dimension_rows = [dimension] if dimension is not None else []
        self._binding_rows = bindings
        # Pre-joined rows standing in for EvalBinding.suite_id -> EvalSuite.application_id ->
        # Application: each is an object exposing both Application.{id,name} and
        # EvalBinding.dimension_id, since the fake join is pre-baked rather than relational.
        self._other_agent_rows = other_agent_bindings or []
        # Agents considered to exist in this project, for the demote-target existence check
        # (`_agent_exists`, PR #416 review). Any agent already present in other_agent_bindings
        # is implicitly known too, since those rows came from a real Application join.
        known = set(known_agent_ids or [])
        known.update(row.id for row in self._other_agent_rows)
        self._known_agent_rows = [types.SimpleNamespace(id=aid) for aid in known]
        self.flush_count = 0
        self.rolled_back = False
        self.refreshed = None
        self.dimension_locks = []

    def query(self, *args):
        first = args[0]
        if first is EvalDimension:
            return _FakeQuery(self._dimension_rows, lock_sink=self.dimension_locks)
        if first is Application.id and len(args) == 1:
            return _FakeQuery(self._known_agent_rows, columns=['id'])
        if first is Application.id:
            return _FakeQuery(self._other_agent_rows, columns=[a.name for a in args])
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

# `auth.current_user()` and `rpc_tools.RpcMixin().rpc` are only consulted on the tier-change
# branch. Tests override these module-level callables directly (monkeypatch-free, since this
# is a hand-rolled stub module, not a real package) to control the caller identity / admin
# check result per test.
CURRENT_USER_ID = 42
IS_PROJECT_ADMIN = True


class _FakeRpc:
    def timeout(self, _seconds):
        return self

    def admin_check_user_is_admin(self, _project_id, _user_id):
        return IS_PROJECT_ADMIN


class _FakeRpcMixin:
    rpc = _FakeRpc()


_tools.auth = types.SimpleNamespace(current_user=lambda: {'id': CURRENT_USER_ID})
_tools.rpc_tools = types.SimpleNamespace(RpcMixin=_FakeRpcMixin)

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
_evaluation.EvalDimension = EvalDimension
_evaluation.EvalTier = EvalTier
_evaluation.EvalBinding = EvalBinding
_evaluation.EvalSuite = EvalSuite
_evaluation.Application = Application
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


def _update(dimension, bindings=None, other_agent_bindings=None, known_agent_ids=None, **fields):
    session = _FakeSession(dimension, bindings or [], other_agent_bindings, known_agent_ids)
    return library.update_dimension(
        project_id=1, dimension_id=dimension.id, data=_UpdateModel(**fields), session=session,
    ), session


@pytest.fixture(autouse=True)
def _reset_admin_flag():
    """Every tier-change test controls admin status via the module global; restore the
    default (admin) afterward so unrelated tests aren't affected by run order."""
    _self = sys.modules[__name__]
    previous = _self.IS_PROJECT_ADMIN
    yield
    _self.IS_PROJECT_ADMIN = previous


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


# ---------------------------------------------------------------------------
# Tier promote/demote (agent_adhoc <-> project)
# ---------------------------------------------------------------------------

class _JoinedAgentRow:
    """Stands in for the pre-joined EvalBinding -> EvalSuite -> Application result: exposes
    both Application.{id,name} and EvalBinding.dimension_id on one object, since the fake
    session's join is pre-baked rather than relational (see _FakeSession)."""

    def __init__(self, id, name, dimension_id):  # noqa: A002 - mirrors the real column name
        self.id = id
        self.name = name
        self.dimension_id = dimension_id


@pytest.fixture
def agent_adhoc_dimension():
    return EvalDimension(id=1, tier=EvalTier.agent_adhoc, agent_id=7, allowed_engines=['ai'],
                          code=None, return_contract=None)


def test_promote_agent_adhoc_to_project_clears_agent_id(agent_adhoc_dimension):
    result, session = _update(agent_adhoc_dimension, tier=EvalTier.project)
    assert result.tier == EvalTier.project
    assert result.agent_id is None
    assert not session.rolled_back


def test_promote_ignores_any_agent_id_sent_alongside_tier(agent_adhoc_dimension):
    """Promoting always clears ownership regardless of what agent_id the caller sends."""
    result, _ = _update(agent_adhoc_dimension, tier=EvalTier.project, agent_id=999)
    assert result.agent_id is None


def test_promote_rejected_for_non_admin(agent_adhoc_dimension):
    sys.modules[__name__].IS_PROJECT_ADMIN = False
    with pytest.raises(library.EvalDimensionTierPermissionError):
        _update(agent_adhoc_dimension, tier=EvalTier.project)


def test_demote_project_to_agent_adhoc_with_no_other_bindings_succeeds(dimension):
    result, _ = _update(dimension, known_agent_ids=[7], tier=EvalTier.agent_adhoc, agent_id=7)
    assert result.tier == EvalTier.agent_adhoc
    assert result.agent_id == 7


def test_demote_requires_an_agent_id(dimension):
    with pytest.raises(library.EvalDimensionDemoteMissingAgentError):
        _update(dimension, tier=EvalTier.agent_adhoc)


def test_demote_rejects_an_agent_id_that_does_not_exist_in_this_project(dimension):
    """PR #416 review: an invalid target agent_id must not fall through to the flush()'s broad
    IntegrityError -> EvalNameConflictError catch, which would misreport it as a duplicate-name
    409 instead of the actual problem."""
    with pytest.raises(library.EvalDimensionDemoteAgentNotFoundError) as excinfo:
        _update(dimension, tier=EvalTier.agent_adhoc, agent_id=999)
    assert excinfo.value.agent_id == 999


def test_demote_locks_the_dimension_row_before_the_other_agents_check(dimension):
    """PR #416 review: the demote path must row-lock the dimension (matching the lock
    add_binding() takes in evaluation_suite_utils.py) so a concurrent bind can't land between
    the other-agents check and this transaction's commit and get silently stranded."""
    _, session = _update(dimension, known_agent_ids=[7], tier=EvalTier.agent_adhoc, agent_id=7)
    assert session.dimension_locks == [True]


def test_promote_does_not_take_the_demote_row_lock(agent_adhoc_dimension):
    """Only demote needs the lock (it's the direction that can strand a binding); promoting
    doesn't add or interpret any binding-visibility state, so it shouldn't pay for one."""
    _, session = _update(agent_adhoc_dimension, tier=EvalTier.project)
    assert session.dimension_locks == []


def test_demote_blocked_when_another_agent_is_bound(dimension):
    others = [_JoinedAgentRow(id=8, name='Other Agent', dimension_id=1)]
    with pytest.raises(library.EvalDimensionDemoteConflictError) as excinfo:
        _update(dimension, other_agent_bindings=others, known_agent_ids=[7],
                tier=EvalTier.agent_adhoc, agent_id=7)
    assert excinfo.value.other_agents == [(8, 'Other Agent')]
    assert 'Other Agent' in str(excinfo.value)


def test_demote_allowed_when_only_the_target_agent_is_bound(dimension):
    """The demote-conflict query excludes the target owner itself — its own binding is not a
    conflict, it's the reason this agent is a sensible new owner."""
    others = [_JoinedAgentRow(id=7, name='Target Agent', dimension_id=1)]
    result, _ = _update(dimension, other_agent_bindings=others, tier=EvalTier.agent_adhoc, agent_id=7)
    assert result.tier == EvalTier.agent_adhoc
    assert result.agent_id == 7


def test_demote_rejected_for_non_admin(dimension):
    sys.modules[__name__].IS_PROJECT_ADMIN = False
    with pytest.raises(library.EvalDimensionTierPermissionError):
        _update(dimension, tier=EvalTier.agent_adhoc, agent_id=7)


def test_unchanged_tier_is_a_no_op_and_skips_the_admin_check(dimension):
    """Sending the dimension's current tier back must not trigger the tier-change branch at
    all — routine field edits should never require project-admin permission."""
    sys.modules[__name__].IS_PROJECT_ADMIN = False
    result, _ = _update(dimension, tier=EvalTier.project, name='Renamed')
    assert result.name == 'Renamed'
    assert result.tier == EvalTier.project


def test_tier_omitted_from_payload_is_a_no_op(dimension):
    result, _ = _update(dimension, name='Renamed')
    assert result.name == 'Renamed'
    assert result.tier == EvalTier.project
