"""Integration tests for the bounded eval read endpoints (review #336, T2.1 / T2.4).

Three eval GETs used to return an unbounded payload: the single-run endpoint shipped the frozen
snapshot (every case's input/output/expected_output) on a poll target, and the results / dataset-case
reads returned the whole set. The fixes are all in the request layer — limit/offset parsing, an
``include=snapshot`` opt-in, and the ``{total,limit,offset}`` envelope — so they are pinned here
rather than in the util unit tests.

The handlers are loaded into a synthetic package so their relative imports resolve against fakes;
the point is the wire shape and the arguments handed to the utils, not the SQL underneath.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_read_pagination_test'


class _EvalLibraryError(Exception):
    http_status = 400


class _Args(dict):
    """Stand-in for ``request.args`` — mimics Flask's MultiDict.get(key, default, type=...)."""

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


class _Model:
    """Fake pydantic model: echoes whatever it was validated from."""

    def __init__(self, payload):
        self._payload = payload

    @classmethod
    def model_validate(cls, obj):
        return cls(dict(obj) if isinstance(obj, dict) else dict(obj.__dict__))

    def model_dump(self, mode=None, exclude=None):  # noqa: ARG002
        return {k: v for k, v in self._payload.items() if k not in (exclude or set())}


class _ResultsEnvelope:
    def __init__(self, **kwargs):
        self._payload = kwargs

    def model_dump(self, mode=None, exclude=None):  # noqa: ARG002
        out = {k: v for k, v in self._payload.items() if k not in (exclude or set())}
        out['run'] = out['run'].model_dump()
        out['results'] = [r.model_dump() for r in out['results']]
        out['human_scores'] = [h.model_dump() for h in out['human_scores']]
        return out


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
        'EvalRunDetailModel', 'EvalResultDetailModel', 'EvalHumanScoreDetailModel',
        'EvalDatasetDetailModel', 'EvalDatasetCaseDetailModel', 'EvalDatasetUpdateModel',
        'EvalDatasetCaseCreateModel',
    ):
        setattr(pd_eval, name, type(name, (_Model,), {}))
    pd_eval.EvalRunResultsModel = _ResultsEnvelope

    run_utils = types.ModuleType(f'{PKG}.utils.evaluation_run_utils')
    run_utils.get_run = lambda project_id, run_id, session=None: {
        'id': run_id, 'status': 'finished', 'progress': {'done': 2, 'total': 2},
        'headline_score': 7.5, 'error': None, 'snapshot': {'cases': [{'input': 'q'}]},
    }
    run_utils.delete_run = lambda project_id, run_id, session=None: None

    result_utils = types.ModuleType(f'{PKG}.utils.evaluation_result_utils')
    result_utils.DEFAULT_RESULT_LIMIT = 500
    result_utils.MAX_RESULT_LIMIT = 2000

    def _get_run_results(project_id, run_id, session=None, limit=None, offset=0):
        calls.append({'limit': limit, 'offset': offset})
        return {
            'run': {'id': run_id}, 'results': [{'id': 1}], 'human_scores': [],
            'headline_score': 7.5, 'total': 900, 'limit': limit, 'offset': offset,
        }
    result_utils.get_run_results = _get_run_results

    dataset_utils = types.ModuleType(f'{PKG}.utils.evaluation_dataset_utils')
    dataset_utils.DEFAULT_CASE_LIMIT = 200
    dataset_utils.MAX_CASE_LIMIT = 1000
    dataset_utils.get_dataset = lambda project_id, dataset_id, agent_id=None, session=None: {'id': dataset_id}
    dataset_utils.update_dataset = lambda *a, **k: {}
    dataset_utils.delete_dataset = lambda *a, **k: None
    dataset_utils.add_case = lambda *a, **k: {}

    def _list_cases(project_id, dataset_id, agent_id=None, session=None, limit=None, offset=0):
        calls.append({'limit': limit, 'offset': offset})
        window = min(limit or 200, 1000)
        return {
            'total': 500, 'limit': window, 'offset': offset,
            'cases': [{'id': i} for i in range(min(window, max(500 - offset, 0)))],
        }
    dataset_utils.list_cases = _list_cases

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
        f'{PKG}.utils.evaluation_run_utils': run_utils,
        f'{PKG}.utils.evaluation_result_utils': result_utils,
        f'{PKG}.utils.evaluation_dataset_utils': dataset_utils,
        f'{PKG}.utils.evaluation_library_utils': library_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    loaded = {}
    for endpoint in ('eval_run', 'eval_results', 'eval_dataset', 'eval_dataset_cases'):
        full = f'{PKG}.api.v2.{endpoint}'
        spec = importlib.util.spec_from_file_location(
            full, PLUGIN_ROOT / 'api' / 'v2' / f'{endpoint}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[full] = module
        spec.loader.exec_module(module)
        loaded[endpoint] = module
    return loaded


@pytest.fixture
def api():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools')}
    calls = []
    modules = _install_package(calls)
    yield modules, calls, _Request
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


# ---------------------------------------------------------------------------
# T2.1 — the polled run endpoint no longer ships the snapshot
# ---------------------------------------------------------------------------

def test_single_run_get_omits_the_snapshot_by_default(api):
    modules, _, request = api
    request.args = _Args()

    payload, status = modules['eval_run'].PromptLibAPI().get(1, 3)

    assert status == 200
    assert 'snapshot' not in payload
    # The progress feed the dialog actually reads must survive the narrowing.
    assert payload['status'] == 'finished'
    assert payload['progress'] == {'done': 2, 'total': 2}
    assert payload['headline_score'] == 7.5
    assert 'error' in payload


def test_single_run_get_returns_the_snapshot_when_asked(api):
    modules, _, request = api
    request.args = _Args(include='snapshot')

    payload, _ = modules['eval_run'].PromptLibAPI().get(1, 3)

    assert payload['snapshot'] == {'cases': [{'input': 'q'}]}


def test_include_accepts_a_comma_separated_list(api):
    modules, _, request = api
    request.args = _Args(include='foo, snapshot ')

    payload, _ = modules['eval_run'].PromptLibAPI().get(1, 3)

    assert 'snapshot' in payload


# ---------------------------------------------------------------------------
# T2.4 — paginated reads
# ---------------------------------------------------------------------------

def test_results_get_defaults_to_a_bounded_page(api):
    modules, calls, request = api
    request.args = _Args()

    payload, status = modules['eval_results'].PromptLibAPI().get(1, 3)

    assert status == 200
    assert calls == [{'limit': 500, 'offset': 0}]
    assert (payload['total'], payload['limit'], payload['offset']) == (900, 500, 0)


def test_results_get_forwards_limit_and_offset(api):
    modules, calls, request = api
    request.args = _Args(limit='50', offset='100')

    modules['eval_results'].PromptLibAPI().get(1, 3)

    assert calls == [{'limit': 50, 'offset': 100}]


def test_results_get_rejects_non_integer_paging(api):
    modules, calls, request = api
    request.args = _Args(limit='all')

    payload, status = modules['eval_results'].PromptLibAPI().get(1, 3)

    assert status == 400
    assert calls == []
    assert 'integers' in payload['error']


def test_dataset_get_reports_the_real_total_and_flags_truncation(api):
    modules, _, request = api
    request.args = _Args(limit='10')

    payload, status = modules['eval_dataset'].PromptLibAPI().get(1, 4)

    assert status == 200
    assert len(payload['cases']) == 10
    # `cases` is a window; `case_count` must stay the whole set or the UI miscounts.
    assert payload['case_count'] == 500
    assert payload['cases_truncated'] is True


def test_dataset_get_does_not_flag_truncation_on_the_last_page(api):
    modules, _, request = api
    request.args = _Args(limit='1000', offset='0')

    payload, _ = modules['eval_dataset'].PromptLibAPI().get(1, 4)

    assert len(payload['cases']) == 500
    assert payload['cases_truncated'] is False


def test_dataset_get_still_404s_for_a_missing_dataset(api):
    modules, _, request = api
    request.args = _Args()
    modules['eval_dataset'].get_dataset = lambda project_id, dataset_id, agent_id=None, session=None: None

    payload, status = modules['eval_dataset'].PromptLibAPI().get(1, 404)

    assert status == 404
    assert 'not found' in payload['error']


def test_cases_get_returns_a_paging_envelope(api):
    modules, calls, request = api
    request.args = _Args(limit='25', offset='50')

    payload, status = modules['eval_dataset_cases'].PromptLibAPI().get(1, 4)

    assert status == 200
    assert calls == [{'limit': 25, 'offset': 50}]
    assert payload['total'] == 500
    assert payload['limit'] == 25
    assert payload['offset'] == 50
    assert len(payload['cases']) == 25


def test_cases_get_surfaces_a_missing_dataset_as_its_own_status(api):
    modules, _, request = api
    request.args = _Args()

    def _boom(*a, **k):
        exc = _EvalLibraryError('Eval dataset with id 404 not found')
        exc.http_status = 404
        raise exc
    modules['eval_dataset_cases'].list_cases = _boom

    payload, status = modules['eval_dataset_cases'].PromptLibAPI().get(1, 404)

    assert status == 404
    assert 'not found' in payload['error']
