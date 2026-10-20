"""EL-7011 US-21 - the generate_schedule_draft endpoint answers a draft, or the error of the SAD contract.

The handler loads into a synthetic package (like test_6416_draft_llm_failure_surfacing.py), so its
relative imports resolve against fakes. The request and response models and the answer helpers of
draft_llm_utils are real. The checks of schedule_draft_utils have their own unit tests; here the
model call is mocked and the fake checks only record what the handler passes in.
"""
import importlib.util
import json
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'draftpkg_7011_test'
PROJECT = 2
ROSTER = [{'application_id': 42, 'name': 'Digest agent', 'agent_type': 'react', 'version_id': 420,
           'version_name': 'base'}]
MODEL_DRAFT = {'name': 'Daily digest', 'cron': '0 9 * * *', 'entity_type': 'agent'}


class _PoolSaturationError(Exception):
    retry_after = 5


class _Request:
    json = None  # set per test


class _Handler:
    """Stand-in for the app-injected ``self.module`` of APIModeHandler."""

    def __init__(self, result=None, exc=None):
        self.calls, self._result, self._exc = [], result, exc

    def predict_sio_llm(self, **kwargs):
        self.calls.append(kwargs)
        if self._exc:
            raise self._exc
        return self._result


def _answer(text):
    return {'result': {'chat_history': [{'role': 'assistant', 'content': text}]}}


def _load_real(name, relative_path):
    spec = importlib.util.spec_from_file_location(f'{PKG}.{name}', PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[f'{PKG}.{name}'] = module
    spec.loader.exec_module(module)
    return module


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


@pytest.fixture
def endpoint():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools', 'pylon', 'pylon.core', 'pylon.core.tools')}
    state = types.SimpleNamespace(
        service_prompt='Draft a schedule.', default_model={'model_name': 'gpt-5-mini'},
        permissions=[], seen={}, db_sessions=0,
    )
    for pkg_name, path in (('', None), ('.api', None), ('.api.v2', None), ('.models', None), ('.utils', None),
                           ('.models.pd', PLUGIN_ROOT / 'models' / 'pd')):
        _module(PKG + pkg_name).__path__ = [str(path)] if path else []

    def build(template, current, roster, timezone, locked_entity=None):
        state.seen.update(current=current, roster=roster, timezone=timezone, locked=locked_entity)
        return f'{template} current={json.dumps(current)}'

    def validate(project_id, raw, locked_entity):
        state.seen['raw'] = raw
        draft = schedule_models.ScheduleDraft(name=raw.get('name'), cron=raw.get('cron'))
        return draft, [{'field': 'timezone', 'reason': 'unknown'}]

    _module('flask', request=_Request)
    log = types.SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None,
                                exception=lambda *a, **k: None, info=lambda *a, **k: None)
    core_tools = _module('pylon.core.tools', log=log)
    _module('pylon.core', tools=core_tools)
    _module('pylon', core=sys.modules['pylon.core'])

    class _Rpc:
        def timeout(self, _seconds):
            return self

        def configurations_get_default_model(self, project_id, section='llm'):
            if isinstance(state.default_model, Exception):
                raise state.default_model
            return state.default_model and dict(state.default_model)

    def check_api(spec):
        state.permissions = spec['permissions']
        return lambda func: func

    _module('tools',
            api_tools=types.SimpleNamespace(APIModeHandler=object, APIBase=object, with_modes=lambda p: p,
                                            endpoint_metrics=lambda func: func),
            config=types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default'),
            auth=types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=check_api),
                                       current_user=lambda: {'id': 7}),
            register_openapi=lambda *a, **k: (lambda func: func),
            rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=_Rpc())),
            db=types.SimpleNamespace(with_project_schema_session=lambda project_id: state.__setattr__(
                'db_sessions', state.db_sessions + 1)))
    _module(f'{PKG}.utils.constants', PROMPT_LIB_MODE='prompt_lib')
    _module(f'{PKG}.utils.exceptions', PoolSaturationError=_PoolSaturationError)
    _module(f'{PKG}.utils.predict_utils', PredictPayloadError=type('PredictPayloadError', (Exception,), {}))
    _module(f'{PKG}.utils.service_prompt_utils', get_service_prompt=lambda key: state.service_prompt)
    _module(f'{PKG}.utils.utils', get_public_project_id=lambda: 1,
            extract_json_from_text=lambda text: text[text.find('{'):text.rfind('}') + 1] or text)
    schedule_models = _load_real('models.pd.generate_schedule_draft', 'models/pd/generate_schedule_draft.py')
    _module(f'{PKG}.utils.schedule_draft_utils', schedule_roster=lambda project_id: ROSTER,
            build_schedule_draft_prompt=build, validate_schedule_draft=validate)
    _load_real('utils.draft_llm_utils', 'utils/draft_llm_utils.py')
    state.module = _load_real('api.v2.generate_schedule_draft', 'api/v2/generate_schedule_draft.py')
    yield state
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _post(state, body, result=None, exc=None):
    state.module.request.json = body
    handler = state.module.PromptLibAPI()
    handler.module = _Handler(result=result, exc=exc)
    payload, status = handler.post(PROJECT)
    return payload, status, handler.module


def test_el_7011_r1_1_a_prompt_returns_the_draft_and_the_warnings(endpoint):
    payload, status, model = _post(endpoint, {'prompt': 'every day at 9'}, _answer(json.dumps(MODEL_DRAFT)))
    assert status == 200
    assert payload['draft']['name'] == 'Daily digest'
    assert payload['draft']['cron'] == '0 9 * * *'
    assert payload['warnings'] == [{'field': 'timezone', 'reason': 'unknown'}]
    assert model.calls[0]['data']['user_input'] == 'every day at 9'
    assert model.calls[0]['skip_expansion'] is True


def test_el_7011_r1_2_current_values_and_the_service_prompt_go_to_the_model(endpoint):
    current = {'name': 'Old name', 'cron': '*/30 * * * *'}
    _, status, model = _post(
        endpoint, {'prompt': 'make it hourly', 'current': current, 'timezone': 'Europe/Kyiv',
                   'locked_entity': {'entity_type': 'agent', 'application_id': 42}},
        _answer(json.dumps(MODEL_DRAFT)))
    assert status == 200
    assert endpoint.seen['current'] == current
    assert endpoint.seen['timezone'] == 'Europe/Kyiv'
    assert endpoint.seen['locked'] == {'entity_type': 'agent', 'application_id': 42}
    assert endpoint.seen['roster'] == ROSTER
    assert model.calls[0]['data']['instructions'].startswith('Draft a schedule.')
    assert 'Old name' in model.calls[0]['data']['instructions']


def test_el_7011_r1_3_the_draft_opens_no_write_session(endpoint):
    _post(endpoint, {'prompt': 'x'}, _answer(json.dumps(MODEL_DRAFT)))
    assert endpoint.db_sessions == 0


@pytest.mark.parametrize('body', [{}, {'prompt': '   '}, {'prompt': 'x', 'locked_entity': {'entity_type': 'tool'}},
                                  {'prompt': 'x', 'current': 'not a dict'}])
def test_el_7011_r1_4_a_bad_body_is_400_with_the_errors_and_no_model_call(endpoint, body):
    payload, status, model = _post(endpoint, body)
    assert status == 400
    assert isinstance(payload, list) and payload[0]['loc']
    assert model.calls == []


def test_el_7011_r1_5_a_missing_service_prompt_is_500_and_no_model_call(endpoint):
    endpoint.service_prompt = ''
    payload, status, model = _post(endpoint, {'prompt': 'x'})
    assert status == 500
    assert payload == {'error': "Service prompt 'generate_schedule_draft' is not configured"}
    assert model.calls == []


@pytest.mark.parametrize('result, exc', [
    ({'task_id': 'abc'}, None),
    (_answer('I cannot help with that.'), None),
    (_answer('[1, 2]'), None),
    ({'result': {'chat_history': []}}, None),
    (None, RuntimeError('boom')),
])
def test_el_7011_r1_6_a_model_failure_is_502_with_a_short_reason(endpoint, result, exc):
    payload, status, _ = _post(endpoint, {'prompt': 'x'}, result, exc)
    assert status == 502
    assert payload['error']


def test_el_7011_r1_6_no_default_model_is_400(endpoint):
    endpoint.default_model = None
    _, status, model = _post(endpoint, {'prompt': 'x'})
    assert status == 400
    assert model.calls == []


def test_el_7011_r1_6_a_busy_worker_pool_is_503_with_retry_after(endpoint):
    payload, status, _ = _post(endpoint, {'prompt': 'x'}, exc=_PoolSaturationError())
    assert status == 503
    assert payload['retry_after'] == 5


def test_el_7011_r2_5_the_endpoint_demands_the_schedules_create_permission(endpoint):
    assert endpoint.permissions == ['models.applications.schedules.create']
