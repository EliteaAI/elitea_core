"""Wire contract for the draft-generating endpoints when the LLM call does not produce a draft.

Every worker-side failure used to collapse into "LLM returned an empty response" (#6416 Issue B,
#6415), and the skill generator reported truncated output as unparseable while running on half
the token budget the default-model branch gets (#6416 Issue A). The agent-draft generator shared
both defects, so it shares the fix and this contract.

The handlers are loaded into a synthetic package so their relative imports resolve against fakes,
following ``test_generate_eval_dimensions.py``; the point is the response contract, not real LLM
output. ``draft_llm_utils``, ``generate_skill_utils`` and ``generate_project_context_utils`` are
loaded for real — they are what is under test.
"""
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'draftpkg_6416_test'

DEFAULT_MODEL = 'gpt-5-mini'
PROJECT = 2
PUBLIC_PROJECT = 1

TRACEBACK = (
    'Traceback (most recent call last):\n'
    '  File "/data/methods/indexer_predict_agent.py", line 520, in predict\n'
    'litellm.exceptions.BadRequestError: LLM Provider NOT provided\n'
)

SKILL_DRAFT = {'name': 'github-review', 'description': 'Reviews PRs.', 'instructions': '# Review\nBe strict.'}
PROJECT_CONTEXT_DRAFT = {'project_background': '# Stack\nReact + FastAPI.', 'activation_description': 'stack questions'}
APPLICATION_DRAFT = {'name': 'PR Reviewer', 'description': 'Reviews PRs.', 'instructions': 'Review pull requests.'}


class _PredictPayloadError(Exception):
    pass


class _PoolSaturationError(Exception):
    def __init__(self, pool='agents', retry_after=5):
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

    @property
    def sent_llm_settings(self):
        return self.calls[0]['data']['llm_settings']

    @property
    def sent_instructions(self):
        return self.calls[0]['data']['instructions']


def _thinking_result(text):
    return {'result': {'thinking_steps': [{'text': text}]}}


def _load_real(name, relative_path):
    full = f'{PKG}.{name}'
    spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


def _install_package(rpc_state):
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

    # The real models.pd.skill drags the ORM vocabulary in; only the name rule is needed here.
    pd_skill = types.ModuleType(f'{PKG}.models.pd.skill')
    pd_skill.validate_skill_name = lambda value: value
    pd_skill.RESERVED_NAME_WORDS = ('claude', 'anthropic')

    orm_skill = types.ModuleType(f'{PKG}.models.skill')
    orm_skill.SkillVersion = type('SkillVersion', (), {})

    predict_utils = types.ModuleType(f'{PKG}.utils.predict_utils')
    predict_utils.PredictPayloadError = _PredictPayloadError

    exceptions = types.ModuleType(f'{PKG}.utils.exceptions')
    exceptions.PoolSaturationError = _PoolSaturationError

    utils_utils = types.ModuleType(f'{PKG}.utils.utils')
    utils_utils.extract_json_from_text = _extract_json_from_text
    utils_utils.get_public_project_id = lambda: PUBLIC_PROJECT

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'

    service_prompt_utils = types.ModuleType(f'{PKG}.utils.service_prompt_utils')
    service_prompt_utils.get_service_prompt = lambda key: 'Generate something useful.'

    generate_application_utils = types.ModuleType(f'{PKG}.utils.generate_application_utils')
    generate_application_utils.fetch_project_resources = lambda *a, **k: ([], [], [], [], [])
    generate_application_utils.build_system_prompt = lambda template, *a, **k: template
    generate_application_utils.fetch_application_for_edit = lambda *a, **k: None
    generate_application_utils.build_edit_system_prompt = lambda template, *a, **k: template

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

    class _RpcCaller:
        def timeout(self, _seconds):
            return self

        def configurations_get_default_model(self, project_id, section='llm'):
            return dict(rpc_state['default_model'])

        def configurations_get_available_models(self, project_id, section='llm', include_shared=True):
            return dict(rpc_state['available'])

        def configurations_get_configuration_model(self, project_id, model_name):
            return {'name': model_name} if (project_id, model_name) in rpc_state['resolvable'] else {}

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
    tools.db = types.SimpleNamespace(with_project_schema_session=lambda project_id: None)

    for name, mod in {
        PKG: pkg,
        f'{PKG}.api': api_pkg,
        f'{PKG}.api.v2': v2_pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.pd': pd_pkg,
        f'{PKG}.models.pd.skill': pd_skill,
        f'{PKG}.models.skill': orm_skill,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.predict_utils': predict_utils,
        f'{PKG}.utils.exceptions': exceptions,
        f'{PKG}.utils.utils': utils_utils,
        f'{PKG}.utils.constants': constants,
        f'{PKG}.utils.service_prompt_utils': service_prompt_utils,
        f'{PKG}.utils.generate_application_utils': generate_application_utils,
        'flask': flask,
        'pylon': pylon,
        'pylon.core': pylon_core,
        'pylon.core.tools': pylon_core_tools,
        'tools': tools,
    }.items():
        sys.modules[name] = mod

    _load_real('utils.draft_llm_utils', 'utils/draft_llm_utils.py')
    _load_real('utils.generate_skill_utils', 'utils/generate_skill_utils.py')
    _load_real('utils.generate_project_context_utils', 'utils/generate_project_context_utils.py')

    return (
        _load_real('api.v2.generate_skill_draft', 'api/v2/generate_skill_draft.py'),
        _load_real('api.v2.generate_project_context_draft', 'api/v2/generate_project_context_draft.py'),
        _load_real('api.v2.generate_application_draft', 'api/v2/generate_application_draft.py'),
    )


@pytest.fixture
def endpoints():
    saved = {name: sys.modules.get(name) for name in ('flask', 'tools', 'pylon')}
    rpc_state = {
        'default_model': {'model_name': DEFAULT_MODEL, 'model_project_id': PUBLIC_PROJECT},
        'available': {
            (PROJECT, DEFAULT_MODEL): {'supports_reasoning': False},
            (PUBLIC_PROJECT, 'claude-sonnet-5'): {'supports_reasoning': True, 'shared': True},
        },
        'resolvable': set(),
    }
    skill, project_context, application = _install_package(rpc_state)
    yield types.SimpleNamespace(
        skill=skill, project_context=project_context, application=application, rpc_state=rpc_state,
    )
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _call(module, body, predict_result=None, predict_exc=None):
    module.request.json = body
    handler = module.PromptLibAPI()
    handler.module = _Handler(predict_result=predict_result, predict_exc=predict_exc)
    payload, status = handler.post(PROJECT)
    return payload, status, handler.module


def _generators(endpoints):
    """Every draft generator that shares the failure-reporting helpers."""
    return [
        (endpoints.skill, SKILL_DRAFT),
        (endpoints.project_context, PROJECT_CONTEXT_DRAFT),
        (endpoints.application, APPLICATION_DRAFT),
    ]


def test_happy_path_still_returns_a_draft(endpoints):
    for module, draft in _generators(endpoints):
        payload, status, _ = _call(
            module, {'user_description': 'something'}, _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert all(payload[key] == value for key, value in draft.items())


def test_unknown_model_is_rejected_before_the_llm_call(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': 'nonexistent-model-xyz-999'}},
        )

        assert status == 400
        assert 'nonexistent-model-xyz-999' in payload['error']
        assert handler.calls == []


def test_public_shared_model_is_accepted(endpoints):
    for module, draft in _generators(endpoints):
        _payload, status, _ = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': 'claude-sonnet-5'}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200


def test_worker_failure_reports_the_real_reason(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module,
            {'user_description': 'x'},
            {'result': {
                'chat_history': [],
                'error': TRACEBACK,
                'human_readable': 'The selected model is not available for your team.',
            }},
        )

        assert status == 500
        assert 'not available for your team' in payload['error']
        assert payload['error'] != 'LLM returned an empty response'


def test_worker_failure_without_human_readable_uses_the_exception_line(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module, {'user_description': 'x'}, {'result': {'chat_history': [], 'error': TRACEBACK}},
        )

        assert status == 500
        assert 'LLM Provider NOT provided' in payload['error']


def test_maintenance_is_named_rather_than_masked(endpoints):
    """Not special-cased — it reaches the caller through the same path as any other refusal."""
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module,
            {'user_description': 'x'},
            {'error': 'maintenance_in_progress', 'message': 'The platform is currently in maintenance mode.'},
        )

        assert status == 500
        assert 'maintenance mode' in payload['error']


def test_timeout_is_a_504_so_a_retry_policy_can_tell_it_apart(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(module, {'user_description': 'x'}, {'task_id': 'abc'})

        assert status == 504
        assert 'timed out after 60s' in payload['error']


def test_genuinely_empty_completion_keeps_the_generic_message(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(module, {'user_description': 'x'}, {'result': {'thinking_steps': []}})

        assert status == 500
        assert payload['error'] == 'LLM returned an empty response'


def test_budget_exhausted_before_any_output_asks_for_a_bigger_budget(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module,
            {'user_description': 'x'},
            {'result': {'error': None, 'thinking_steps': [
                {'text': '', 'generation_info': {'finish_reason': 'length'}},
            ]}},
        )

        assert status == 500
        assert 'max_tokens' in payload['error']


def test_truncated_output_asks_for_a_bigger_budget(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module, {'user_description': 'x'}, _thinking_result('{"name": "half-a-draft'),
        )

        assert status == 422
        assert 'truncated' in payload['error']
        assert 'max_tokens' in payload['error']


def test_a_budget_cut_off_beats_the_brace_heuristic(endpoints):
    """Real truncated drafts end mid-prose with balanced braces; finish_reason still says length."""
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(
            module,
            {'user_description': 'x'},
            {'result': {'thinking_steps': [
                {'text': 'bullet points and short paragraphs to explain observations.',
                 'generation_info': {'finish_reason': 'length'}},
            ]}},
        )

        assert status == 422
        assert 'truncated' in payload['error']


def test_unparseable_output_carries_the_parse_error(endpoints):
    for module, _draft in _generators(endpoints):
        payload, status, _ = _call(module, {'user_description': 'x'}, _thinking_result('no json here'))

        assert status == 422
        assert payload['error'] == 'LLM returned unparseable output'
        assert payload['parse_error']


def test_caller_supplied_model_gets_the_full_token_budget(endpoints):
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': DEFAULT_MODEL}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['max_tokens'] == 4096


def test_default_model_branch_also_gets_the_full_token_budget(endpoints):
    """llm_settings without a model_name still carries the pydantic max_tokens default."""
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'temperature': 0}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['max_tokens'] == 4096
        assert handler.sent_llm_settings['temperature'] == 0


def test_a_model_project_id_without_a_model_name_is_dropped(endpoints):
    """It would otherwise overwrite the default model's own project and reach predict unchecked."""
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_project_id': 999}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['model_project_id'] == PUBLIC_PROJECT


def test_a_model_shared_from_public_carries_its_owning_project(endpoints):
    """#6416 Issue B: unstamped, generate_predict_payload resolves capabilities against the
    caller's project, misses, and defaults openai_compatible to False."""
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': 'claude-sonnet-5'}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['model_project_id'] == PUBLIC_PROJECT


def test_a_model_the_caller_owns_carries_the_callers_project(endpoints):
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': DEFAULT_MODEL}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['model_project_id'] == PROJECT


def test_an_explicit_model_project_id_is_not_overwritten(endpoints):
    for module, draft in _generators(endpoints):
        endpoints.rpc_state['resolvable'] = {(7, 'partner-model')}
        _payload, status, handler = _call(
            module,
            {'user_description': 'x',
             'llm_settings': {'model_name': 'partner-model', 'model_project_id': 7}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['model_project_id'] == 7


def test_an_unplaceable_model_is_sent_unstamped(endpoints):
    """The availability lookup failing must not invent a project id for the proxy to resolve."""
    for module, draft in _generators(endpoints):
        endpoints.rpc_state['available'] = None
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': 'externally-managed'}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert 'model_project_id' not in handler.sent_llm_settings


def test_explicit_null_max_tokens_falls_back_to_the_default(endpoints):
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': DEFAULT_MODEL, 'max_tokens': None}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['max_tokens'] == 4096


def test_explicit_max_tokens_is_respected(endpoints):
    for module, draft in _generators(endpoints):
        _payload, status, handler = _call(
            module,
            {'user_description': 'x', 'llm_settings': {'model_name': DEFAULT_MODEL, 'max_tokens': 512}},
            _thinking_result(json.dumps(draft)),
        )

        assert status == 200
        assert handler.sent_llm_settings['max_tokens'] == 512


def test_skill_prompt_carries_the_output_contract(endpoints):
    _payload, status, handler = _call(
        endpoints.skill, {'user_description': 'x'}, _thinking_result(json.dumps(SKILL_DRAFT)),
    )

    assert status == 200
    assert 'Required output contract' in handler.sent_instructions
    assert 'max 5000 characters' in handler.sent_instructions
    # validate_skill_name raises on these, so the authoritative contract has to repeat the rule
    assert "'claude' or 'anthropic'" in handler.sent_instructions
