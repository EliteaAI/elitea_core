"""EL-7325: HTTP contract of POST /generate_pipeline_node_draft.

The handler is loaded into a synthetic package so its relative imports resolve against fakes
(pattern of test_6416_draft_llm_failure_surfacing.py). ``utils/node_draft`` and ``draft_llm_utils``
are loaded for real; DB reads (version lookup, resources) are faked.

Covers AC-30 (unsupported type, 404), AC-30b (413), AC-31 (pipeline model only, unavailable model,
no default fallback), AC-32 (response), AC-33 (repair through chat history), AC-36 (settings),
AC-38 (project override wins), the prompt layout and resources only for resource types.
"""
import importlib
import importlib.util
import json
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'nodedraftpkg_7325_test'
PROJECT = 2
PUBLIC_PROJECT = 1
MODEL = 'gpt-5-mini'

PIPELINE_YAML = """
entry_point: fetch_issues
state:
  issues: {type: list}
nodes:
  - id: fetch_issues
    type: toolkit
    transition: summarize
  - id: summarize
    type: llm
    transition: END
"""

VALID_ANSWER = {
    "summary": "Summarizes issues.",
    "node": {"input": ["issues"], "output": ["summary"], "transition": "END",
             "input_mapping": {"task": {"type": "fstring", "value": "Summarize {issues}"}},
             "tool_names": {"github_elitea": ["get_issues"]}},
    "new_state": [{"name": "summary", "type": "str"}],
    "warnings": [],
}


class _PredictPayloadError(Exception):
    pass


class _PoolSaturationError(Exception):
    retry_after = 5


class _Request:
    json = {}
    content_length = 0


def _answer(obj):
    text = obj if isinstance(obj, str) else json.dumps(obj)
    return {'result': {'chat_history': [{'role': 'user', 'content': 'x'}, {'role': 'assistant', 'content': text}]}}


class _Module:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def predict_sio_llm(self, **kwargs):
        self.calls.append(kwargs)
        return self.results.pop(0)


def _load_real(name, relative_path):
    full = f'{PKG}.{name}'
    spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


def _pkg(name, path=None):
    mod = types.ModuleType(f'{PKG}{name}')
    mod.__path__ = [str(path)] if path else []
    sys.modules[f'{PKG}{name}'] = mod
    return mod


def _install(state):
    _pkg('')
    _pkg('.api')
    _pkg('.api.v2', PLUGIN_ROOT / 'api' / 'v2')
    _pkg('.models')
    _pkg('.models.pd', PLUGIN_ROOT / 'models' / 'pd')
    _pkg('.utils', PLUGIN_ROOT / 'utils')
    _pkg('.utils.node_draft', PLUGIN_ROOT / 'utils' / 'node_draft')

    def stub(name, **attrs):
        mod = types.ModuleType(f'{PKG}.{name}')
        mod.__dict__.update(attrs)
        sys.modules[f'{PKG}.{name}'] = mod
        return mod

    stub('utils.predict_utils', PredictPayloadError=_PredictPayloadError)
    stub('utils.exceptions', PoolSaturationError=_PoolSaturationError)
    stub('utils.constants', PROMPT_LIB_MODE='prompt_lib')
    stub('utils.utils', extract_json_from_text=lambda t: t[t.find('{'):t.rfind('}') + 1] if '{' in t else t,
         get_public_project_id=lambda: PUBLIC_PROJECT)
    stub('utils.service_prompt_utils', get_service_prompt=lambda key: state['public_prompts'].get(key, ''))
    stub('utils.pipeline_trigger', get_trigger_from_pipeline_settings=lambda s: (s or {}).get('trigger')
         or {'type': 'chat_message'})

    flask = types.ModuleType('flask')
    flask.request = _Request
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(**{k: (lambda *a, **kw: None)
                                               for k in ('debug', 'info', 'warning', 'exception', 'error')})
    pylon_core = types.ModuleType('pylon.core')
    pylon_core.tools = pylon_tools
    pylon = types.ModuleType('pylon')
    pylon.core = pylon_core

    class _Rpc:
        def timeout(self, _s):
            return self

        call = property(lambda self: self)

        def configurations_get_available_models(self, project_id, section='llm', include_shared=True):
            return {(PROJECT, MODEL): {'supports_reasoning': False}}

        def configurations_get_configuration_model(self, project_id, model_name):
            if model_name not in (MODEL, 'o-reasoner'):
                return {}
            return {'name': model_name, 'supports_reasoning': model_name == 'o-reasoner'}

        def configurations_get_default_model(self, project_id, section='llm'):
            state['default_model_asked'] = True
            return {'model_name': 'project-default'}

        def configurations_get_first_filtered_project(self, project_id, filter_fields):
            prompt = state['project_prompts'].get(filter_fields['elitea_title'])
            return {'data': {'prompt': prompt}} if prompt else None

    class _ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        with_modes = staticmethod(lambda p: p)
        endpoint_metrics = staticmethod(lambda f: f)

    tools = types.ModuleType('tools')
    tools.api_tools = _ApiTools()
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)),
                                       current_user=lambda: {'id': 7})
    tools.register_openapi = lambda *a, **k: (lambda f: f)
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=_Rpc()))
    tools.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    for name, mod in {'flask': flask, 'pylon': pylon, 'pylon.core': pylon_core, 'pylon.core.tools': pylon_tools,
                      'tools': tools}.items():
        sys.modules[name] = mod

    _load_real('utils.error_text', 'utils/error_text.py')
    _load_real('utils.draft_llm_utils', 'utils/draft_llm_utils.py')
    _load_real('models.pd.predict_llm', 'models/pd/predict_llm.py')
    saved_jinja = {n: sys.modules.pop(n) for n in list(sys.modules) if n == 'jinja2' or n.startswith('jinja2.')}
    importlib.import_module('jinja2')
    try:
        handler = _load_real('api.v2.generate_pipeline_node_draft', 'api/v2/generate_pipeline_node_draft.py')
    finally:
        sys.modules.update(saved_jinja)

    resources = sys.modules[f'{PKG}.utils.node_draft.resources']
    resources.load_candidates = lambda *a, **k: state['candidates']
    prompt_mod = sys.modules[f'{PKG}.utils.node_draft.prompt']
    prompt_mod.shipped_defaults = lambda: state['shipped']
    handler.shipped_defaults = prompt_mod.shipped_defaults
    handler.load_candidates = resources.load_candidates

    def load_version(project_id, application_id, version_id):
        if (application_id, version_id) not in state['versions']:
            raise handler._StopDraft({'error': 'Pipeline or version not found.'}, 404)
        return {'pipeline_settings': state['versions'][(application_id, version_id)]}

    handler._load_version = load_version
    return handler


@pytest.fixture
def api():
    saved = {n: sys.modules.get(n) for n in ('flask', 'tools', 'pylon', 'pylon.core', 'pylon.core.tools')}
    state = {
        'versions': {(5, 50): {}},
        'public_prompts': {},
        'project_prompts': {},
        'shipped': {'pipeline_node_build': 'CORE BUILD', 'pipeline_node_edit': 'CORE EDIT',
                    'pipeline_node_spec_llm': 'Node type: llm. SPEC', 'pipeline_node_spec_code': 'Node type: code. C'},
        'candidates': [{'kind': 'toolkit', 'entity_type': 'toolkit', 'id': 1, 'name': 'github_elitea',
                        'type': 'github', 'description': 'repo', 'attached': True,
                        'tools': [{'name': 'get_issues', 'description': '', 'params': {'repo': 'str*'}}]}],
    }
    handler = _install(state)
    yield types.SimpleNamespace(handler=handler, state=state)
    for name in [n for n in sys.modules if n.startswith(PKG)]:
        del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _body(**overrides):
    body = {'application_id': 5, 'version_id': 50, 'node_id': 'summarize', 'node_type': 'llm', 'mode': 'build',
            'requirement': 'Summarize the issues', 'current_node': {'id': 'summarize', 'type': 'llm'},
            'pipeline_yaml': PIPELINE_YAML, 'llm_settings': {'model_name': MODEL}}
    body.update(overrides)
    return body


def _post(api, body, results=(), content_length=None):
    api.handler.request.json = body
    api.handler.request.content_length = content_length if content_length is not None else len(json.dumps(body))
    handler = api.handler.PromptLibAPI()
    handler.module = _Module(results)
    payload, status = handler.post(PROJECT)
    return payload, status, handler.module


def test_success_returns_full_node(api):
    payload, status, module = _post(api, _body(), [_answer(VALID_ANSWER)])
    assert status == 200
    assert payload['node']['id'] == 'summarize' and payload['node']['type'] == 'llm'
    assert payload['attachments'] == []
    assert set(payload) == {'summary', 'node', 'changed_paths', 'attachments', 'new_state', 'warnings'}
    sent = module.calls[0]
    # resolve_model stamps the project that owns the model
    assert sent['data']['llm_settings'] == {'model_name': MODEL, 'model_project_id': PROJECT,
                                            'max_tokens': 2048, 'temperature': 0.2}
    assert sent['data']['instructions'] == 'CORE BUILD\n\n## Node type\nNode type: llm. SPEC'
    assert sent['data']['chat_history'] == []
    assert '"resources"' in sent['data']['user_input'] and 'github_elitea' in sent['data']['user_input']
    assert sent['skip_expansion'] is True and sent['return_chat_history'] is True


def test_unsupported_type_400(api):
    payload, status, module = _post(api, _body(node_type='loop'))
    assert status == 400 and payload['error'] == 'Node type loop is not supported by AI.'
    assert module.calls == []


def test_unknown_version_404(api):
    _, status, module = _post(api, _body(version_id=999))
    assert status == 404 and module.calls == []


def test_body_too_large_413(api):
    payload, status, module = _post(api, _body(), content_length=200 * 1024 + 1)
    assert status == 413 and payload['error'] == 'This pipeline is too large for AI drafting.'
    assert module.calls == []


def test_no_model_never_falls_back_to_default(api):
    """AC-31 / #6872."""
    payload, status, module = _post(api, _body(llm_settings=None))
    assert status == 400 and module.calls == [] and payload['code'] == 'no_model'
    assert 'default_model_asked' not in api.state


def test_unavailable_model_400(api):
    payload, status, module = _post(api, _body(llm_settings={'model_name': 'nope-model'}))
    assert status == 400 and 'nope-model' in payload['error'] and payload['code'] == 'model_unavailable'
    assert module.calls == []


def test_reasoning_model_settings(api):
    api.state['candidates'] = []
    body = _body(node_type='code', node_id='count', current_node={}, llm_settings={'model_name': 'o-reasoner',
                                                                                    'model_project_id': PROJECT})
    code_answer = {"summary": "s", "new_state": [], "warnings": [],
                   "node": {"code": {"type": "fixed", "value": "{'a': 1}"}, "output": ["a"]}}
    api.handler.resolve_model = lambda *a, **k: (None, None)
    _, status, module = _post(api, body, [_answer(code_answer)])
    assert status == 200
    settings = module.calls[0]['data']['llm_settings']
    assert settings['max_tokens'] == 8192 and settings['reasoning_effort'] == 'low' and 'temperature' not in settings


def test_repair_sent_as_chat_history(api):
    """AC-33: the second call carries the first answer and the errors."""
    bad = {"summary": "x", "node": {"input_mapping": {"task": {"type": "python", "value": "x"}}},
           "new_state": [], "warnings": []}
    _, status, module = _post(api, _body(), [_answer(bad), _answer(VALID_ANSWER)])
    assert status == 200 and len(module.calls) == 2
    second = module.calls[1]['data']
    assert [m['role'] for m in second['chat_history']] == ['user', 'assistant']
    assert second['user_input'].startswith('Your previous answer was invalid:')


def test_project_override_wins(api):
    """AC-38."""
    api.state['project_prompts'] = {'pipeline_node_build': 'PROJECT CORE'}
    api.state['public_prompts'] = {'pipeline_node_build': 'PUBLIC CORE', 'pipeline_node_spec_llm': 'PUBLIC SPEC'}
    _, _, module = _post(api, _body(), [_answer(VALID_ANSWER)])
    assert module.calls[0]['data']['instructions'] == 'PROJECT CORE\n\n## Node type\nPUBLIC SPEC'


def test_edit_mode_uses_edit_core_and_current(api):
    current = {'id': 'summarize', 'type': 'llm', 'transition': 'END', 'input': ['issues'], 'output': ['summary'],
               'input_mapping': {'task': {'type': 'fixed', 'value': 'Old task'}}}
    patch = {"summary": "s", "new_state": [], "warnings": [],
             "node": {"input_mapping": {"task": {"type": "fixed", "value": "New task"}}}}
    payload, status, module = _post(api, _body(mode='edit', current_node=current), [_answer(patch)])
    assert status == 200 and payload['changed_paths'] == ['input_mapping.task']
    assert module.calls[0]['data']['instructions'].startswith('CORE EDIT')
    assert '"Old task"' in module.calls[0]['data']['user_input']


def test_no_resources_for_code_nodes(api):
    body = _body(node_type='code', node_id='count', current_node={})
    code_answer = {"summary": "s", "new_state": [], "warnings": [],
                   "node": {"code": {"type": "fixed", "value": "{'a': 1}"}, "output": ["a"]}}
    _, status, module = _post(api, body, [_answer(code_answer)])
    assert status == 200 and '"resources"' not in module.calls[0]['data']['user_input']


def test_invalid_yaml_400(api):
    payload, status, module = _post(api, _body(pipeline_yaml='nodes: ['))
    assert status == 400 and module.calls == []


# ---- code review findings (2026-11-26) ---------------------------------------------------------

def _toolkit(i, attached=False):
    return {'kind': 'toolkit', 'entity_type': 'toolkit', 'id': 100 + i, 'name': f'tk_{i}', 'type': 'jira',
            'description': 'issues tracker', 'attached': attached,
            'tools': [{'name': 'create_issue', 'description': '', 'params': {'issue_json': 'str*'}}]}


def test_params_kept_for_toolkits_ranked_below_the_top_three(api):
    """Review #1: params are trimmed from the prompt for low-ranked toolkits, but not from validation."""
    api.state['candidates'] = [_toolkit(i) for i in range(6)]
    answer = {"summary": "s", "new_state": [], "warnings": [],
              "node": {"toolkit_name": "tk_5", "tool": "create_issue", "input": [], "output": [],
                       "transition": "END", "input_mapping": {"issue_json": {"type": "fixed", "value": "{}"}}}}
    body = _body(node_type='toolkit', node_id='make', current_node={},
                 requirement='Create an issue with tk_5')
    payload, status, _ = _post(api, body, [_answer(answer)])
    assert status == 200
    assert payload['node']['input_mapping'] == {'issue_json': {'type': 'fixed', 'value': '{}'}}
    assert not any('issue_json' in w for w in payload['warnings'])


@pytest.mark.parametrize('node', [
    {"toolkit_name": {"name": "x"}, "tool": "t", "input": [], "output": [], "transition": "END"},
    {"toolkit_name": "tk_0", "tool": ["t"], "input": [], "output": [], "transition": "END"},
])
def test_wrong_value_types_are_repaired_not_500(api, node):
    """Review #2: a model answer with a dict/list where a name belongs goes to the repair turn."""
    api.state['candidates'] = [_toolkit(0)]
    good = {"summary": "s", "new_state": [], "warnings": [],
            "node": {"toolkit_name": "tk_0", "tool": "create_issue", "input": [], "output": [], "transition": "END",
                     "input_mapping": {"issue_json": {"type": "fixed", "value": "{}"}}}}
    bad = {"summary": "s", "new_state": [], "warnings": [], "node": node}
    payload, status, module = _post(api, _body(node_type='toolkit', node_id='make', current_node={}),
                                    [_answer(bad), _answer(good)])
    assert status == 200 and len(module.calls) == 2


def test_new_state_with_a_non_string_name_is_skipped(api):
    """Review #2: `new_state[].name` that isn't a string is dropped with a warning, not a 500."""
    answer = {**VALID_ANSWER, "new_state": [{"name": ["a"], "type": "str"}, {"name": "summary", "type": "str"}]}
    payload, status, _ = _post(api, _body(), [_answer(answer)])
    assert status == 200 and {'name': 'summary', 'type': 'str'} in payload['new_state']


def test_router_condition_that_is_not_text_is_repaired(api):
    api.state['candidates'] = []
    bad = {"summary": "s", "new_state": [], "warnings": [],
           "node": {"condition": {"if": "x"}, "routes": ["END"], "input": []}}
    good = {"summary": "s", "new_state": [], "warnings": [],
            "node": {"condition": "{{ 'END' }}", "routes": ["END"], "input": []}}
    payload, status, module = _post(api, _body(node_type='router', node_id='r', current_node={}),
                                    [_answer(bad), _answer(good)])
    assert status == 200 and len(module.calls) == 2


@pytest.mark.parametrize('pipeline_yaml', [
    'state: [a, b]\nnodes: []\n',
    'nodes: 5\n',
    'nodes:\n  - id: 1\n    type: llm\n',
    'entry_point: 1\nnodes: []\n',
    'state:\n  x: {type: 5}\nnodes: []\n',
])
def test_odd_editor_yaml_is_a_400_not_500(api, pipeline_yaml):
    """Review #3: valid YAML with unexpected types never crashes the handler."""
    payload, status, module = _post(api, _body(pipeline_yaml=pipeline_yaml), [_answer(VALID_ANSWER)])
    assert status in (200, 400), payload
    if status == 400:
        assert module.calls == []


def test_edit_with_a_malformed_current_node_is_not_500(api):
    """Review #4: the merged node is shape-checked too; a bad `current_node.input` is repaired, not a crash."""
    current = {'id': 'summarize', 'type': 'llm', 'input': [{'a': 1}], 'transition': 'END',
               'input_mapping': {'task': {'type': 'fixed', 'value': 'x'}}}
    patch = {"summary": "s", "new_state": [], "warnings": [],
             "node": {"input_mapping": {"task": {"type": "fstring", "value": "Use {issues}"}}}}
    payload, status, _ = _post(api, _body(mode='edit', current_node=current), [_answer(patch), _answer(patch)])
    assert status in (200, 422), payload
