"""Integration tests for the AI eval-dimension-generation endpoint.

Mirrors ``generate_application_draft.py``'s "Build with AI" contract: one LLM call producing a
JSON draft, validated and returned — nothing persisted. These tests pin the wire shape and the
same error-mapping (400/404/422/500/503) the agent-draft endpoint already has, since the new
endpoint deliberately copies that pipeline.

The handler is loaded into a synthetic package so its relative imports resolve against fakes;
the point is the request/response contract and error handling, not real LLM output.
"""
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_generate_dimensions_test'


class _PredictPayloadError(Exception):
    pass


class _PoolSaturationError(Exception):
    def __init__(self, pool='eval', retry_after=5):
        self.pool = pool
        self.retry_after = retry_after
        super().__init__(f"Pool '{pool}' saturated")


def _extract_json_from_text(text):
    match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if match:
        return match.group(1)
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        return text[start:end]
    return text


class _Request:
    json = {}


class _Handler:
    """Stand-in for the flask-app-injected ``self.module`` on APIModeHandler."""

    def __init__(self, predict_result=None, predict_exc=None):
        self.calls = []
        self._result = predict_result
        self._exc = predict_exc

    def predict_sio_llm(self, **kwargs):
        self.calls.append(kwargs)
        if self._exc:
            raise self._exc
        return self._result


def _thinking_result(text):
    return {'result': {'thinking_steps': [{'text': text}]}}


def _install_package():
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    api_pkg = types.ModuleType(f'{PKG}.api')
    api_pkg.__path__ = []
    v2_pkg = types.ModuleType(f'{PKG}.api.v2')
    v2_pkg.__path__ = [str(PLUGIN_ROOT / 'api' / 'v2')]
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    pd_pkg = types.ModuleType(f'{PKG}.models.pd')
    pd_pkg.__path__ = [str(PLUGIN_ROOT / 'models' / 'pd')]
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = []

    # ORM vocab stub — same lightweight sibling pattern as test_evaluation_dataset_models.py
    vocab = types.ModuleType(f'{PKG}.models.evaluation')

    class EvalTier:
        platform = 'platform'; project = 'project'; agent_adhoc = 'agent_adhoc'

    class EvalEngine:
        ai = 'ai'; human = 'human'; code = 'code'

    class EvalScaleType:
        binary = 'binary'; ordinal = 'ordinal'; continuous = 'continuous'

    class EvalPolarity:
        higher_better = 'higher_better'; lower_better = 'lower_better'

    class EvalCaseSource:
        manual = 'manual'; import_ = 'import'; conversation = 'conversation'

    class EvalRunTrigger:
        offline_batch = 'offline_batch'; on_demand = 'on_demand'

    vocab.EvalTier = EvalTier
    vocab.EvalEngine = EvalEngine
    vocab.EvalScaleType = EvalScaleType
    vocab.EvalPolarity = EvalPolarity
    vocab.EvalCaseSource = EvalCaseSource
    vocab.EvalRunTrigger = EvalRunTrigger

    predict_llm = types.ModuleType(f'{PKG}.models.pd.predict_llm')

    from pydantic import BaseModel as _BaseModel
    from typing import Optional as _Optional

    class LLMSettingsRequest(_BaseModel):
        model_name: _Optional[str] = None
        temperature: _Optional[float] = None
        max_tokens: _Optional[int] = None

    predict_llm.LLMSettingsRequest = LLMSettingsRequest

    class _ServicePromptTemplateError(Exception):
        pass

    generate_app_utils = types.ModuleType(f'{PKG}.utils.generate_application_utils')
    generate_app_utils.fetch_application_instructions = lambda *a, **k: {
        'application_name': 'Support Bot', 'version_id': 1, 'instructions': 'Answer support tickets politely.',
    }
    generate_app_utils.build_eval_dimensions_system_prompt = lambda template, **kw: template.format(
        application_name=kw.get('application_name', ''),
        instructions=kw.get('instructions', ''),
    )
    generate_app_utils.ServicePromptTemplateError = _ServicePromptTemplateError

    service_prompt_utils = types.ModuleType(f'{PKG}.utils.service_prompt_utils')
    service_prompt_utils.get_service_prompt = lambda key: 'generate dimensions for {application_name}: {instructions}'

    eval_library_utils = types.ModuleType(f'{PKG}.utils.evaluation_library_utils')
    eval_library_utils.list_dimensions = lambda *a, **k: []

    predict_utils = types.ModuleType(f'{PKG}.utils.predict_utils')
    predict_utils.PredictPayloadError = _PredictPayloadError

    exceptions = types.ModuleType(f'{PKG}.utils.exceptions')
    exceptions.PoolSaturationError = _PoolSaturationError

    utils_utils = types.ModuleType(f'{PKG}.utils.utils')
    utils_utils.extract_json_from_text = _extract_json_from_text

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    flask = types.ModuleType('flask')
    flask.request = _Request

    pylon_core_tools = types.ModuleType('pylon.core.tools')
    pylon_core_tools.log = types.SimpleNamespace(
        debug=lambda *a, **k: None, warning=lambda *a, **k: None,
        exception=lambda *a, **k: None, info=lambda *a, **k: None)
    pylon_core = types.ModuleType('pylon.core')
    pylon_core.tools = pylon_core_tools
    pylon = types.ModuleType('pylon')
    pylon.core = pylon_core

    default_model = {'model_name': 'gpt-5-mini'}

    class _RpcCaller:
        def timeout(self, _):
            return self

        def configurations_get_default_model(self, project_id, section='llm'):
            return dict(default_model)

    class _RpcMixin:
        rpc = _RpcCaller()

    rpc_tools = types.SimpleNamespace(RpcMixin=_RpcMixin)

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
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
        current_user=lambda: {'id': 7},
    )
    tools.register_openapi = lambda *a, **k: (lambda f: f)
    tools.rpc_tools = rpc_tools

    for name, mod in {
        PKG: pkg,
        f'{PKG}.api': api_pkg,
        f'{PKG}.api.v2': v2_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.evaluation': vocab,
        f'{PKG}.models.pd.predict_llm': predict_llm,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.generate_application_utils': generate_app_utils,
        f'{PKG}.utils.service_prompt_utils': service_prompt_utils,
        f'{PKG}.utils.evaluation_library_utils': eval_library_utils,
        f'{PKG}.utils.predict_utils': predict_utils,
        f'{PKG}.utils.exceptions': exceptions,
        f'{PKG}.utils.utils': utils_utils,
        f'{PKG}.utils.constants': constants,
        'flask': flask,
        'pylon': pylon,
        'pylon.core': pylon_core,
        'pylon.core.tools': pylon_core_tools,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.api.v2.generate_eval_dimensions'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'api' / 'v2' / 'generate_eval_dimensions.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module, generate_app_utils, service_prompt_utils, default_model


@pytest.fixture
def api():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools', 'pylon')}
    module, generate_app_utils, service_prompt_utils, default_model = _install_package()
    yield module, generate_app_utils, service_prompt_utils, default_model, _Request
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


_VALID_DRAFT = {
    'dimensions': [
        {
            'name': 'Politeness',
            'description': 'Response stays courteous and professional.',
            'evidence_scope': {'input': True, 'output': True},
            'weight': 1.0,
        },
    ]
}


def test_happy_path_returns_validated_draft(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_result=_thinking_result(json.dumps(_VALID_DRAFT)))

    payload, status = handler.post(1)

    assert status == 200
    assert payload['version_id'] == 1
    assert payload['dimensions'][0]['name'] == 'Politeness'
    assert payload['dimensions'][0]['tier'] == 'agent_adhoc'
    assert payload['dimensions'][0]['evidence_scope'] == {'input': True, 'output': True}
    assert handler.module.calls[0]['data']['project_id'] == 1


def test_missing_application_returns_404(api):
    module, *_rest = api
    module.fetch_application_instructions = lambda *a, **k: None
    module.request.json = {'application_id': 999}
    handler = module.PromptLibAPI()
    handler.module = _Handler()

    payload, status = handler.post(1)

    assert status == 404
    assert 'not found' in payload['error']


def test_unconfigured_service_prompt_returns_500(api):
    module, *_rest = api
    module.get_service_prompt = lambda key: ''
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler()

    payload, status = handler.post(1)

    assert status == 500
    assert 'not configured' in payload['error']


def test_no_default_model_configured_returns_400(api):
    module, _generate_app_utils, _service_prompt_utils, default_model, _Request = api
    default_model.clear()
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler()

    payload, status = handler.post(1)

    assert status == 400
    assert 'default LLM model' in payload['error']


def test_predict_payload_error_returns_400(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_exc=_PredictPayloadError('bad payload'))

    payload, status = handler.post(1)

    assert status == 400
    assert payload['error'] == 'bad payload'


def test_pool_saturation_returns_503(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_exc=_PoolSaturationError(pool='eval_runs', retry_after=9))

    payload, status = handler.post(1)

    assert status == 503
    assert payload['retry_after'] == 9


def test_empty_llm_response_returns_500(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_result=_thinking_result(''))

    payload, status = handler.post(1)

    assert status == 500
    assert 'empty response' in payload['error']


def test_truncated_json_returns_422_with_truncation_hint(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    truncated = json.dumps(_VALID_DRAFT)[:-5]  # cut off the closing braces
    handler.module = _Handler(predict_result=_thinking_result(truncated))

    payload, status = handler.post(1)

    assert status == 422
    assert 'truncated' in payload['error']


def test_schema_invalid_draft_returns_422(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    bad_draft = {'dimensions': [{'name': 'Politeness', 'weight': -1}]}  # weight must be >= 0
    handler.module = _Handler(predict_result=_thinking_result(json.dumps(bad_draft)))

    payload, status = handler.post(1)

    assert status == 422
    assert payload['error'] == 'Generated draft failed validation'


def test_duplicate_names_in_draft_returns_422(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    dupe_draft = {
        'dimensions': [
            {'name': 'Politeness', 'description': 'x', 'evidence_scope': {'output': True}, 'weight': 1.0},
            {'name': 'politeness', 'description': 'y', 'evidence_scope': {'output': True}, 'weight': 1.0},
        ]
    }
    handler.module = _Handler(predict_result=_thinking_result(json.dumps(dupe_draft)))

    payload, status = handler.post(1)

    assert status == 422
    assert payload['error'] == 'Generated draft failed validation'


def test_default_tier_is_agent_adhoc(api):
    module, *_rest = api
    module.request.json = {'application_id': 1}
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_result=_thinking_result(json.dumps(_VALID_DRAFT)))

    payload, status = handler.post(1)

    assert status == 200
    assert payload['dimensions'][0]['tier'] == 'agent_adhoc'
