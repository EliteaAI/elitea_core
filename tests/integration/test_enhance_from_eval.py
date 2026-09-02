"""Integration tests for the "Enhance with AI" analysis endpoint (ENH-4, §5).

Same contract as ``generate_eval_dimensions``: one LLM call, a validated JSON proposal returned,
nothing persisted. These tests pin the wire shape and the error mapping (400/404/409/422/500/503).

Two behaviours here are not shared with the draft endpoints and are the reason this file exists:

* the run's **pinned** version id and instructions hash are server-owned — whatever the model
  emits for them is overwritten, so an accepted patch can never be applied against text that was
  not under test;
* a run with no missed target returns 200 **without calling the LLM at all**.

The real pydantic contract is loaded (it is the parse boundary); ranking and prompt assembly are
stubbed, since both are covered by their own unit suites.
"""
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_enhance_from_eval_test'


class _PredictPayloadError(Exception):
    pass


class _PoolSaturationError(Exception):
    def __init__(self, pool='eval', retry_after=5):
        self.pool = pool
        self.retry_after = retry_after
        super().__init__(f"Pool '{pool}' saturated")


class _EvalRunNotFoundError(Exception):
    pass


class _EvalRunNotFinishedError(Exception):
    pass


class _EnhancePromptTemplateError(Exception):
    pass


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


RUN = {
    'run_id': 42,
    'application_id': 1,
    'version_id': 7,
    'status': 'finished',
    'headline_score': 61.0,
    # Ranking is stubbed, but grounding (ENH-5) reads these directly: they are the id namespace a
    # proposal's citations have to fall inside.
    'snapshot': {
        'dimensions': {'11': {'name': 'Groundedness'}, '12': {'name': 'Politeness'}},
        'cases': [{'id': 10}, {'id': 11}],
    },
    'results': [{'dimension_id': 11, 'dataset_case_id': 10}],
    'human_scores': [],
}

VERSION = {
    'application_id': 1,
    'application_name': 'Support Bot',
    'version_id': 7,
    'version_name': 'v3',
    'version_status': 'published',
    'instructions': 'Answer support tickets politely.',
    'instructions_sha256': 'b' * 64,
    'agent_context': {'model_name': 'gpt-5-mini', 'toolkit_names': ['jira']},
}

GAPS = [
    {'dimension_id': 11, 'dimension_name': 'Groundedness'},
    {'dimension_id': 12, 'dimension_name': 'Politeness'},
]

COVERAGE = {'total_cases': 10, 'gap_dimensions_total': 2, 'gap_dimensions_returned': 2}


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
    # Points at the real utils dir so ``enhancement_validation`` and its ``mcp_versioning`` sibling
    # load for real: grounding an item against the frozen instructions is worth exercising through
    # the endpoint. The stubs registered in sys.modules below still take precedence.
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]

    predict_llm = types.ModuleType(f'{PKG}.models.pd.predict_llm')

    from pydantic import BaseModel as _BaseModel
    from typing import Optional as _Optional

    class LLMSettingsRequest(_BaseModel):
        model_name: _Optional[str] = None
        temperature: _Optional[float] = None
        max_tokens: _Optional[int] = None

    predict_llm.LLMSettingsRequest = LLMSettingsRequest

    state = types.SimpleNamespace(run=dict(RUN), version=dict(VERSION), gaps=list(GAPS),
                                  build_kwargs=None)

    gap_selection = types.ModuleType(f'{PKG}.utils.enhancement_gap_selection')
    gap_selection.select_gaps = lambda *a, **k: {'gaps': list(state.gaps), 'coverage': dict(COVERAGE)}

    enhancement_prompt = types.ModuleType(f'{PKG}.utils.enhancement_prompt')
    enhancement_prompt.EnhancePromptTemplateError = _EnhancePromptTemplateError

    def _build_prompt(template, **kw):
        state.build_kwargs = kw
        return template.format(application_name=kw.get('application_name', ''),
                               instructions=kw.get('instructions', ''))

    enhancement_prompt.build_enhance_system_prompt = _build_prompt

    enhancement_utils = types.ModuleType(f'{PKG}.utils.enhancement_utils')
    enhancement_utils.EvalRunNotFinishedError = _EvalRunNotFinishedError
    enhancement_utils.fetch_run_for_enhancement = lambda *a, **k: dict(state.run)
    enhancement_utils.fetch_evaluated_version = lambda *a, **k: (
        dict(state.version) if state.version is not None else None
    )

    human_score_utils = types.ModuleType(f'{PKG}.utils.evaluation_human_score_utils')
    human_score_utils.EvalRunNotFoundError = _EvalRunNotFoundError

    service_prompt_utils = types.ModuleType(f'{PKG}.utils.service_prompt_utils')
    service_prompt_utils.get_service_prompt = lambda key: 'analyse {application_name}: {instructions}'

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
        f'{PKG}.models.pd.predict_llm': predict_llm,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.enhancement_gap_selection': gap_selection,
        f'{PKG}.utils.enhancement_prompt': enhancement_prompt,
        f'{PKG}.utils.enhancement_utils': enhancement_utils,
        f'{PKG}.utils.evaluation_human_score_utils': human_score_utils,
        f'{PKG}.utils.service_prompt_utils': service_prompt_utils,
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

    full = f'{PKG}.api.v2.enhance_from_eval'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'api' / 'v2' / 'enhance_from_eval.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module, state, default_model


@pytest.fixture
def api():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools', 'pylon')}
    module, state, default_model = _install_package()
    yield module, state, default_model
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


_VALID_PROPOSAL = {
    'diagnosis': 'Uncited claims cost points because nothing asks for citations.',
    'agent_fixes': [{
        'old_text': 'Answer support tickets politely.',
        'replacement': 'Answer support tickets politely and cite every source.',
        'rationale': 'Cases 10 and 11 lost points for uncited claims.',
        'cited_dimension_ids': [11],
        'cited_case_ids': [10, 11],
    }],
    'eval_fixes': [{
        'kind': 'dimension_target',
        'target_id': 11,
        'target_name': 'Groundedness',
        'current_value': '95',
        'proposed_value': '80',
        'rationale': 'A 95 target on a 0-100 judge scale is not reachable in practice.',
    }],
}


def _post(module, handler_module, body=None):
    module.request.json = body if body is not None else {'run_id': 42}
    handler = module.PromptLibAPI()
    handler.module = handler_module
    return handler, handler.post(1)


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

def test_happy_path_returns_validated_proposal(api):
    module, *_rest = api
    handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(_VALID_PROPOSAL))))

    assert status == 200
    assert payload['run_id'] == 42
    assert payload['version_id'] == 7
    assert payload['instructions_sha256'] == 'b' * 64
    assert payload['agent_fixes'][0]['old_text'] == 'Answer support tickets politely.'
    assert payload['eval_fixes'][0]['kind'] == 'dimension_target'
    assert payload['coverage']['gap_dimensions_total'] == 2
    assert handler.module.calls[0]['data']['project_id'] == 1


def test_an_ungrounded_item_is_dropped_and_counted_in_the_response(api):
    """An anchor that is not in the analysed instructions would fail at 409 in front of a user who
    had already accepted it, so it never reaches the payload — but the count does, or a prompt
    regression that yields only bad items looks like a clean run."""
    module, *_rest = api
    proposal = dict(_VALID_PROPOSAL, agent_fixes=[
        dict(_VALID_PROPOSAL['agent_fixes'][0]),
        {'old_text': 'Text that was never in the instructions.', 'replacement': 'x',
         'rationale': 'Invented anchor.'},
    ])

    _handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(proposal))))

    assert status == 200
    assert len(payload['agent_fixes']) == 1
    assert payload['coverage']['discarded_agent_fixes'] == 1
    assert payload['coverage']['discarded_eval_fixes'] == 0


def test_an_invented_citation_drops_the_item(api):
    module, *_rest = api
    proposal = dict(_VALID_PROPOSAL, eval_fixes=[
        dict(_VALID_PROPOSAL['eval_fixes'][0], target_id=999),
    ])

    _handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(proposal))))

    assert status == 200
    assert payload['eval_fixes'] == []
    assert payload['coverage']['discarded_eval_fixes'] == 1


def test_dropping_every_item_is_a_200_not_an_error(api):
    """The diagnosis is still worth showing, and a nonzero count with empty lists is exactly how an
    ungroundable proposal should surface rather than as a 5xx."""
    module, *_rest = api
    proposal = dict(
        _VALID_PROPOSAL,
        agent_fixes=[{'old_text': 'absent text', 'replacement': 'x', 'rationale': 'r'}],
        eval_fixes=[dict(_VALID_PROPOSAL['eval_fixes'][0], target_id=999)],
    )

    _handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(proposal))))

    assert status == 200
    assert payload['agent_fixes'] == [] and payload['eval_fixes'] == []
    assert payload['coverage']['discarded_agent_fixes'] == 1
    assert payload['coverage']['discarded_eval_fixes'] == 1
    assert payload['diagnosis']


def test_server_owns_the_pin_the_model_cannot_forge_it(api):
    """The pin is what makes an accepted patch safe to apply. A model that echoes a different
    version id or hash must not be able to redirect the edit at another version."""
    module, *_rest = api
    forged = dict(_VALID_PROPOSAL, run_id=999, version_id=999, instructions_sha256='c' * 64,
                  coverage={'total_cases': 1, 'gap_dimensions_total': 1,
                            'gap_dimensions_returned': 1})

    _handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(forged))))

    assert status == 200
    assert (payload['run_id'], payload['version_id']) == (42, 7)
    assert payload['instructions_sha256'] == 'b' * 64
    assert payload['coverage']['total_cases'] == 10


def test_requested_dimensions_narrow_the_brief(api):
    module, state, _default_model = api

    _handler, (_payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(_VALID_PROPOSAL))),
        body={'run_id': 42, 'dimension_ids': [12]})

    assert status == 200
    assert [gap['dimension_id'] for gap in state.build_kwargs['gaps']] == [12]


def test_clean_run_returns_a_diagnosis_without_calling_the_llm(api):
    """Asking a model to find fault in a run with no misses is how an ungrounded proposal gets
    generated, so the endpoint answers this case itself."""
    module, state, _default_model = api
    state.gaps = []
    handler_module = _Handler()

    handler, (payload, status) = _post(module, handler_module)

    assert status == 200
    assert payload['agent_fixes'] == [] and payload['eval_fixes'] == []
    assert 'nothing to diagnose' in payload['diagnosis']
    assert payload['version_id'] == 7
    assert handler.module.calls == []


def test_filtering_to_a_dimension_with_no_gap_skips_the_llm(api):
    module, _state, _default_model = api

    handler, (payload, status) = _post(
        module, _Handler(), body={'run_id': 42, 'dimension_ids': [999]})

    assert status == 200
    assert handler.module.calls == []
    assert payload['agent_fixes'] == []


# ---------------------------------------------------------------------------
# request / lookup failures
# ---------------------------------------------------------------------------

def test_missing_run_id_returns_400(api):
    module, *_rest = api
    _handler, (payload, status) = _post(module, _Handler(), body={})

    assert status == 400
    assert isinstance(payload, list)


def test_unknown_run_returns_404(api):
    module, *_rest = api

    def _missing(*a, **k):
        raise _EvalRunNotFoundError(999)

    module.fetch_run_for_enhancement = _missing
    _handler, (payload, status) = _post(module, _Handler(), body={'run_id': 999})

    assert status == 404
    assert 'not found' in payload['error']


def test_unfinished_run_returns_409(api):
    module, *_rest = api

    def _running(*a, **k):
        raise _EvalRunNotFinishedError('Run 42 is running; only a finished run can be analysed')

    module.fetch_run_for_enhancement = _running
    _handler, (payload, status) = _post(module, _Handler())

    assert status == 409
    assert 'only a finished run' in payload['error']


def test_deleted_pinned_version_returns_409(api):
    """The run's pinned version is the only text worth diagnosing; falling back to the agent's
    current draft would produce a confident diagnosis of text that was never under test."""
    module, state, _default_model = api
    state.version = None

    handler, (payload, status) = _post(module, _Handler())

    assert status == 409
    assert 'no longer exists' in payload['error']
    assert handler.module.calls == []


# ---------------------------------------------------------------------------
# configuration / LLM failures
# ---------------------------------------------------------------------------

def test_unconfigured_service_prompt_returns_500(api):
    module, *_rest = api
    module.get_service_prompt = lambda key: ''

    _handler, (payload, status) = _post(module, _Handler())

    assert status == 500
    assert 'not configured' in payload['error']


def test_malformed_template_returns_500(api):
    module, *_rest = api

    def _raise(*a, **k):
        raise _EnhancePromptTemplateError('boom')

    module.build_enhance_system_prompt = _raise
    _handler, (payload, status) = _post(module, _Handler())

    assert status == 500
    assert 'malformed' in payload['error']


def test_no_default_model_configured_returns_400(api):
    module, _state, default_model = api
    default_model.clear()

    _handler, (payload, status) = _post(module, _Handler())

    assert status == 400
    assert 'default LLM model' in payload['error']


def test_explicit_llm_settings_bypass_the_project_default(api):
    module, _state, default_model = api
    default_model.clear()

    handler, (_payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(_VALID_PROPOSAL))),
        body={'run_id': 42, 'llm_settings': {'model_name': 'gpt-5', 'max_tokens': 4096}})

    assert status == 200
    assert handler.module.calls[0]['data']['llm_settings']['model_name'] == 'gpt-5'


def test_default_temperature_is_low(api):
    """The proposal quotes existing instruction text back as patch anchors; a creative paraphrase
    of an anchor is a patch that cannot apply."""
    module, *_rest = api

    handler, (_payload, status) = _post(
        module, _Handler(predict_result=_thinking_result(json.dumps(_VALID_PROPOSAL))))

    assert status == 200
    settings = handler.module.calls[0]['data']['llm_settings']
    assert settings['temperature'] == 0.2
    assert settings['max_tokens'] == module._DEFAULT_MAX_TOKENS


def test_predict_payload_error_returns_400(api):
    module, *_rest = api
    _handler, (payload, status) = _post(module, _Handler(predict_exc=_PredictPayloadError('bad payload')))

    assert status == 400
    assert payload['error'] == 'bad payload'


def test_pool_saturation_returns_503(api):
    module, *_rest = api
    _handler, (payload, status) = _post(
        module, _Handler(predict_exc=_PoolSaturationError(pool='eval_runs', retry_after=9)))

    assert status == 503
    assert payload['retry_after'] == 9


def test_empty_llm_response_returns_500(api):
    module, *_rest = api
    _handler, (payload, status) = _post(module, _Handler(predict_result=_thinking_result('')))

    assert status == 500
    assert 'empty response' in payload['error']


# ---------------------------------------------------------------------------
# output parsing / validation
# ---------------------------------------------------------------------------

def test_truncated_json_returns_422_with_truncation_hint(api):
    module, *_rest = api
    truncated = json.dumps(_VALID_PROPOSAL)[:-5]
    _handler, (payload, status) = _post(module, _Handler(predict_result=_thinking_result(truncated)))

    assert status == 422
    assert 'truncated' in payload['error']
    assert 'max_tokens' in payload['error']


def test_unparseable_output_returns_422(api):
    module, *_rest = api
    _handler, (payload, status) = _post(
        module, _Handler(predict_result=_thinking_result('{not json at all}')))

    assert status == 422
    assert payload['error'] == 'LLM returned unparseable output'


def test_anchorless_agent_fix_returns_422(api):
    """``apply_instructions_patch`` refuses a null old_text unless replace_all is set, so such an
    item must never reach a one-click accept button."""
    module, *_rest = api
    bad = dict(_VALID_PROPOSAL,
               agent_fixes=[{'replacement': 'Always cite sources.', 'rationale': 'r'}])

    _handler, (payload, status) = _post(module, _Handler(predict_result=_thinking_result(json.dumps(bad))))

    assert status == 422
    assert payload['error'] == 'Generated proposal failed validation'


def test_unknown_eval_fix_kind_returns_422(api):
    module, *_rest = api
    bad = dict(_VALID_PROPOSAL,
               eval_fixes=[{'kind': 'rewrite_toolkits', 'target_id': 1,
                            'proposed_value': 'v', 'rationale': 'r'}])

    _handler, (payload, status) = _post(module, _Handler(predict_result=_thinking_result(json.dumps(bad))))

    assert status == 422
    assert 'details' in payload
