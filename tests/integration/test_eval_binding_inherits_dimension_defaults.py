"""Bindings inherit the dimension's authored defaults when a request omits them (EL-6518).

``eval_binding.target`` is the only target the run snapshot freezes and the results screen reads;
``eval_dimension.default_target`` is a template nothing downstream consults. ``add_binding`` used to
store exactly what the client sent, so any caller that omitted ``target`` got a silent ``NULL`` and a
permanently empty Target column — while the suite screen rendered the dimension's default as a
placeholder, making the omission look like it had worked. The defaults are now applied server-side so
every client benefits and the placeholder tells the truth.

The real util module is loaded against stub models: ``inherit_binding_defaults`` is pure, so nothing
here needs a session or the ORM.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_binding_defaults_test'


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
    for name in (
        'EvalSuite', 'EvalBinding', 'EvalDatasetCase', 'EvalDimension', 'EvalSuiteCaseExclusion',
    ):
        setattr(models_eval, name, type(name, (), {}))
    models_eval.EvalEngine = types.SimpleNamespace(ai='ai', code='code', human='human')
    models_eval.EvalTier = types.SimpleNamespace(
        platform='platform', project='project', agent_adhoc='agent_adhoc')

    models_all = types.ModuleType(f'{PKG}.models.all')
    models_all.ApplicationVersion = type('ApplicationVersion', (), {})

    pd_eval = types.ModuleType(f'{PKG}.models.pd.evaluation')
    for name in (
        'EvalSuiteCreateModel', 'EvalSuiteUpdateModel',
        'EvalBindingCreateModel', 'EvalBindingUpdateModel',
    ):
        setattr(pd_eval, name, type(name, (), {}))

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id: None)

    library_utils = types.ModuleType(f'{PKG}.utils.evaluation_library_utils')

    class _EvalLibraryError(Exception):
        http_status = 400

    @contextmanager
    def _session(session, project_id):  # noqa: ARG001
        yield session

    library_utils.EvalLibraryError = _EvalLibraryError
    library_utils.EvalNameConflictError = type(
        'EvalNameConflictError', (_EvalLibraryError,), {'http_status': 409})
    library_utils._session = _session

    for name, mod in {
        PKG: pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.evaluation': models_eval,
        f'{PKG}.models.all': models_all,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.evaluation': pd_eval,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.evaluation_library_utils': library_utils,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.utils.evaluation_suite_utils'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'utils' / 'evaluation_suite_utils.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def suite_utils():
    saved = sys.modules.get('tools')
    module = _install_package()
    yield module
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    if saved is None:
        sys.modules.pop('tools', None)
    else:
        sys.modules['tools'] = saved


def test_omitted_target_inherits_the_dimension_pair(suite_utils):
    """The reported bug: the attach call sends only dimension_id + engine."""
    assert suite_utils.inherit_binding_defaults(
        {'dimension_id', 'engine'}, 2.0, 70.0, '>=',
    ) == {'weight': 2.0, 'target': 70.0, 'target_operator': '>='}


def test_explicit_null_target_is_not_overwritten(suite_utils):
    """'No target for this suite' must survive a dimension that has one — this is why the check is
    on model_fields_set and not on the value being None."""
    inherited = suite_utils.inherit_binding_defaults(
        {'dimension_id', 'target', 'target_operator'}, None, 70.0, '>=',
    )
    assert 'target' not in inherited
    assert 'target_operator' not in inherited


def test_explicit_values_win_over_defaults(suite_utils):
    inherited = suite_utils.inherit_binding_defaults(
        {'weight', 'target', 'target_operator'}, 2.0, 70.0, '>=',
    )
    assert inherited == {}


def test_half_a_pair_is_never_inherited(suite_utils):
    """A target with no operator can never be evaluated, so a dimension carrying only one of the
    two contributes neither rather than a target that silently never applies."""
    assert suite_utils.inherit_binding_defaults(set(), None, 70.0, None) == {}
    assert suite_utils.inherit_binding_defaults(set(), None, None, '>=') == {}


def test_sending_only_one_half_leaves_both_alone(suite_utils):
    """Overriding the operator alone is an explicit statement about the pair; inheriting the other
    half would silently combine request and template into a target the caller never asked for."""
    assert suite_utils.inherit_binding_defaults({'target_operator'}, None, 70.0, '>=') == {}


def test_platform_binding_inherits_nothing(suite_utils):
    """Platform bindings have no local dimension row, so add_binding passes all-None defaults."""
    assert suite_utils.inherit_binding_defaults({'platform_key'}, None, None, None) == {}


def test_zero_weight_default_is_inherited(suite_utils):
    """0 is the documented 'informational only' weight (§20.6), not an absent value."""
    assert suite_utils.inherit_binding_defaults(set(), 0.0, None, None) == {'weight': 0.0}


def test_zero_target_is_inherited(suite_utils):
    assert suite_utils.inherit_binding_defaults(set(), None, 0.0, '>=') == {
        'target': 0.0, 'target_operator': '>=',
    }
