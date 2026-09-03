"""Wire shape of the #6350 endpoints: suite case exclusions, plus the two computed read flags.

The UI team consumes these directly, so the contract is pinned here: the exclusions collection
round-trips a ``{case_ids: [...]}`` envelope and forwards a util error with its own status; the
dataset read advertises ``can_edit``; the case list annotates ``excluded`` only when the caller
names a ``suite_id``. Handlers are loaded into a synthetic package against fake utils — the
ownership and set semantics themselves are covered in test_6350_suite_case_exclusions.py.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_exclusion_endpoints_test'


class _EvalLibraryError(Exception):
    http_status = 400


class _Args(dict):
    def get(self, key, default=None, type=None):  # noqa: A002 - matches Flask's signature
        value = super().get(key, default)
        if value is None or type is None:
            return value
        try:
            return type(value)
        except (TypeError, ValueError):
            return default


class _Request:
    args = _Args()
    json = {}


class _Model:
    def __init__(self, payload):
        self._payload = payload

    @classmethod
    def model_validate(cls, obj):
        return cls(dict(obj) if isinstance(obj, dict) else dict(obj.__dict__))

    def model_dump(self, mode=None, exclude=None):  # noqa: ARG002
        return {k: v for k, v in self._payload.items() if k not in (exclude or set())}


class _ExclusionsModel:
    def __init__(self, case_ids):
        self.case_ids = case_ids

    @classmethod
    def model_validate(cls, obj):
        return cls(list(obj.get('case_ids', [])))


def _install_package(calls):
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    api_pkg = types.ModuleType(f'{PKG}.api')
    api_pkg.__path__ = []
    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = []

    pd_eval = types.ModuleType(f'{PKG}.models.pd.evaluation')
    for name in (
        'EvalDatasetDetailModel', 'EvalDatasetCaseDetailModel', 'EvalDatasetUpdateModel',
        'EvalDatasetCaseCreateModel',
    ):
        setattr(pd_eval, name, type(name, (_Model,), {}))
    pd_eval.EvalSuiteCaseExclusionsModel = _ExclusionsModel

    state = {'exclusions': [11], 'error': None}

    suite_utils = types.ModuleType(f'{PKG}.utils.evaluation_suite_utils')

    def _list_case_exclusions(project_id, suite_id, session=None):
        if state['error']:
            raise state['error']
        return sorted(state['exclusions'])

    def _set_case_exclusions(project_id, suite_id, case_ids, session=None):
        if state['error']:
            raise state['error']
        calls.append(list(case_ids))
        state['exclusions'] = list(case_ids)
        return sorted(case_ids)

    suite_utils.list_case_exclusions = _list_case_exclusions
    suite_utils.set_case_exclusions = _set_case_exclusions
    suite_utils.excluded_case_ids = lambda session, suite_id: set(state['exclusions'])

    dataset_utils = types.ModuleType(f'{PKG}.utils.evaluation_dataset_utils')
    dataset_utils.DEFAULT_CASE_LIMIT = 200
    dataset_utils.MAX_CASE_LIMIT = 1000
    dataset_utils.get_dataset = (
        lambda project_id, dataset_id, agent_id=None, session=None: {'id': dataset_id, 'agent_id': 7}
    )
    dataset_utils.update_dataset = lambda *a, **k: {}
    dataset_utils.delete_dataset = lambda *a, **k: None
    dataset_utils.add_case = lambda *a, **k: {}
    dataset_utils.list_cases = lambda project_id, dataset_id, agent_id=None, session=None, limit=None, offset=0: {
        'total': 3, 'limit': limit or 200, 'offset': offset,
        'cases': [{'id': 10}, {'id': 11}, {'id': 12}],
    }
    dataset_utils.can_edit_dataset = (
        lambda dataset_agent_id, agent_id:
        agent_id is None or dataset_agent_id is None or dataset_agent_id == agent_id
    )

    library_utils = types.ModuleType(f'{PKG}.utils.evaluation_library_utils')
    library_utils.EvalLibraryError = _EvalLibraryError

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    flask = types.ModuleType('flask')
    flask.request = _Request

    @contextmanager
    def _get_session(project_id):  # noqa: ARG001
        yield object()

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        @staticmethod
        def with_modes(params):
            return params

        @staticmethod
        def endpoint_metrics(func):
            return func

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.db = types.SimpleNamespace(get_session=_get_session)
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration',
                                         DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)))
    tools.register_openapi = lambda *a, **k: (lambda f: f)

    for name, mod in {
        PKG: pkg,
        f'{PKG}.api': api_pkg,
        f'{PKG}.api.v2': v2_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.evaluation': pd_eval,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.evaluation_suite_utils': suite_utils,
        f'{PKG}.utils.evaluation_dataset_utils': dataset_utils,
        f'{PKG}.utils.evaluation_library_utils': library_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    loaded = {}
    for endpoint in ('eval_suite_case_exclusions', 'eval_dataset', 'eval_dataset_cases'):
        full = f'{PKG}.api.v2.{endpoint}'
        spec = importlib.util.spec_from_file_location(
            full, PLUGIN_ROOT / 'api' / 'v2' / f'{endpoint}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
        loaded[endpoint] = module
    return loaded, state


@pytest.fixture
def api():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools')}
    calls = []
    modules, state = _install_package(calls)
    _Request.args = _Args()
    _Request.json = {}
    yield modules, calls, state, _Request
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


# ---------------------------------------------------------------------------
# The exclusions collection
# ---------------------------------------------------------------------------

def test_get_returns_the_case_ids_envelope(api):
    modules, _, _, _ = api

    payload, status = modules['eval_suite_case_exclusions'].PromptLibAPI().get(1, 1)

    assert status == 200
    assert payload == {'case_ids': [11]}


def test_put_replaces_the_set_and_echoes_it(api):
    modules, calls, _, request = api
    request.json = {'case_ids': [12, 10]}

    payload, status = modules['eval_suite_case_exclusions'].PromptLibAPI().put(1, 1)

    assert status == 200
    assert calls == [[12, 10]]
    assert payload == {'case_ids': [10, 12]}


def test_put_accepts_an_empty_list_as_clear_all(api):
    modules, calls, _, request = api
    request.json = {'case_ids': []}

    payload, status = modules['eval_suite_case_exclusions'].PromptLibAPI().put(1, 1)

    assert (status, payload) == (200, {'case_ids': []})
    assert calls == [[]]


def test_a_util_error_keeps_its_own_status(api):
    modules, _, state, request = api
    request.json = {'case_ids': [99]}
    boom = _EvalLibraryError("case ids do not belong to this suite's dataset: [99]")
    boom.http_status = 400
    state['error'] = boom

    payload, status = modules['eval_suite_case_exclusions'].PromptLibAPI().put(1, 1)

    assert status == 400
    assert '99' in payload['error']


def test_an_unknown_suite_surfaces_as_404(api):
    modules, _, state, _ = api
    boom = _EvalLibraryError('Eval suite with id 404 not found')
    boom.http_status = 404
    state['error'] = boom

    payload, status = modules['eval_suite_case_exclusions'].PromptLibAPI().get(1, 404)

    assert status == 404
    assert 'not found' in payload['error']


# ---------------------------------------------------------------------------
# The computed read flags
# ---------------------------------------------------------------------------

def test_dataset_read_says_the_owner_may_edit(api):
    modules, _, _, request = api
    request.args = _Args(agent_id='7')

    payload, status = modules['eval_dataset'].PromptLibAPI().get(1, 5)

    assert status == 200
    assert payload['can_edit'] is True


def test_dataset_read_says_a_borrower_may_not_edit(api):
    modules, _, _, request = api
    request.args = _Args(agent_id='9')

    payload, _ = modules['eval_dataset'].PromptLibAPI().get(1, 5)

    assert payload['can_edit'] is False


def test_case_list_annotates_exclusions_for_the_named_suite(api):
    modules, _, _, request = api
    request.args = _Args(suite_id='1')

    payload, status = modules['eval_dataset_cases'].PromptLibAPI().get(1, 5)

    assert status == 200
    assert {c['id']: c['excluded'] for c in payload['cases']} == {
        10: False, 11: True, 12: False,
    }


def test_case_list_without_a_suite_id_marks_nothing_excluded(api):
    modules, _, _, request = api
    request.args = _Args()

    payload, _ = modules['eval_dataset_cases'].PromptLibAPI().get(1, 5)

    # The plain dataset view is suite-agnostic; a borrower's opt-out must not leak into it.
    assert all(c['excluded'] is False for c in payload['cases'])
