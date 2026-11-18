"""#6388: the Builder's reference, validate and instruction-patch tools use utils/pipeline_authoring."""
import functools
import importlib.util
import pathlib
import sys
import types

import pytest
import yaml

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7


def _package(name, path=None):
    module = types.ModuleType(name)
    module.__path__ = [str(path)] if path else []
    sys.modules[name] = module
    return module


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class Harness:
    def __init__(self):
        self.openapi = {}
        self.args = {}
        self.json = {}

    def check_api(self, descriptor):
        def decorator(func):
            return func
        return decorator

    def register_openapi(self, **meta):
        def decorator(func):
            self.openapi[func.__module__] = meta
            return func
        return decorator


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2'):
        _package(name)
    _package(f'{PACKAGE}.utils', PLUGIN_ROOT / 'utils')

    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(
        args=harness.args, get_json=lambda silent=False: harness.json,
    )
    sys.modules['flask'] = flask
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}), APIBase=type('APIBase', (), {}),
        endpoint_metrics=lambda func: func, with_modes=lambda params: params,
    )
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=harness.check_api))
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.register_openapi = harness.register_openapi
    sys.modules['tools'] = tools
    constants = types.ModuleType(f'{PACKAGE}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'
    sys.modules[constants.__name__] = constants

    harness.reference = _load('api/v2/builder_reference.py', f'{PACKAGE}.api.v2.builder_reference')
    harness.validate = _load('api/v2/validate_pipeline.py', f'{PACKAGE}.api.v2.validate_pipeline')
    return harness


def _get_reference(harness, **args):
    from werkzeug.datastructures import MultiDict
    harness.reference.request.args = MultiDict(args)  # Flask's request.args type
    return harness.reference.PromptLibAPI().get(project_id=PROJECT_ID)


def _validate(harness, pipeline_yaml, **extra):
    harness.json = {'pipeline_yaml': pipeline_yaml, **extra}
    return harness.validate.PromptLibAPI().post(project_id=PROJECT_ID)


# ---- reference ---------------------------------------------------------------------------------

def test_reference_defaults_to_the_overview(harness):
    body, status = _get_reference(harness)
    assert status == 200 and body['reference'].startswith('# Pipeline YAML: the shape')
    assert 'printer' not in body['node_types'] and 'llm' in body['node_types']


def test_reference_serves_the_requested_topics(harness):
    body, _ = _get_reference(harness, topics='node:toolkit,node:router')
    assert body['reference'].startswith('# toolkit') and '# router' in body['reference']
    assert '**Required:** `tool`' in body['reference']


def _mcp_executor_function(name, namespace):
    """One McpApiToolExecutor function, unchanged, without the module's database imports."""
    import ast
    source = (PLUGIN_ROOT / 'utils' / 'mcp_service.py').read_text()
    executor = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.ClassDef) and node.name == 'McpApiToolExecutor')
    func = next(node for node in executor.body if isinstance(node, ast.FunctionDef) and node.name == name)
    func.decorator_list = []
    exec(compile(ast.Module(body=[func], type_ignores=[]), f'<{name}>', 'exec'), namespace)  # pylint: disable=exec-used
    return namespace[name]


@pytest.mark.parametrize('topics', [['overview', 'node:llm'], 'overview,node:llm'])
def test_reference_serves_every_topic_sent_through_the_mcp_layer(harness, topics):
    """A list (as the tool description shows it) and a string both reach the endpoint whole."""
    from urllib.parse import parse_qsl, urlencode
    from werkzeug.datastructures import MultiDict

    parse = _mcp_executor_function('_parse_arguments', {
        'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        'sanitize_property_name': lambda name: name,
        '_is_blank': lambda v: v is None or (isinstance(v, str) and not v.strip()),
    })
    parameters = harness.openapi[f'{PACKAGE}.api.v2.builder_reference']['parameters']
    _, query, _ = parse({'topics': topics}, parameters)
    # _build_wsgi_environ's QUERY_STRING (urlencode doseq), read back as Flask builds request.args
    harness.reference.request.args = MultiDict(parse_qsl(urlencode(query, doseq=True)))
    body, _ = harness.reference.PromptLibAPI().get(project_id=PROJECT_ID)
    assert body['reference'].startswith('# Pipeline YAML: the shape') and '# llm' in body['reference']


def test_reference_no_longer_needs_the_service_prompt(harness):
    assert 'get_service_prompt' not in (PLUGIN_ROOT / 'api/v2/builder_reference.py').read_text()


def test_tool_descriptions_are_short_and_take_topics(harness):
    reference = harness.openapi[f'{PACKAGE}.api.v2.builder_reference']
    validate = harness.openapi[f'{PACKAGE}.api.v2.validate_pipeline']
    assert len(reference['mcp_description']) + len(validate['mcp_description']) < 1500
    assert [p['name'] for p in reference['parameters']] == ['topics']
    assert 'node:llm' in reference['mcp_description']


# ---- validate ---------------------------------------------------------------------------------

def test_validate_tells_the_model_to_set_tool_not_name(harness):
    body, status = _validate(harness, 'entry_point: a\nnodes:\n  - {id: a, type: toolkit, name: Jira, transition: END}')
    assert status == 200 and body['valid'] is False
    finding = body['errors'][0]
    assert 'reads `tool`, not `name`' in finding['suggestion']
    assert finding['code'] == finding['rule'] and finding['severity'] == 'error'


def test_validate_keeps_node_id_and_message_for_existing_readers(harness):
    body, _ = _validate(harness, 'entry_point: a\nnodes:\n  - {id: a, type: printer, transition: END,'
                                 ' input_mapping: {printer: {type: fixed, value: hi}}}')
    assert body['valid'] is True
    warning = body['warnings'][0]
    assert warning['node_id'] == 'a' and warning['message'] and warning['severity'] == 'warning'


def test_validate_uses_the_trigger(harness):
    spec = ('entry_point: a\nnodes:\n'
            '  - {id: a, type: llm, transition: h, input_mapping: {task: {type: fixed, value: x}}}\n'
            '  - {id: h, type: hitl, user_message: {type: fixed, value: "ok?"}, routes: {approve: END}}')
    assert _validate(harness, spec)[0]['valid']
    assert not _validate(harness, spec, trigger='schedule')[0]['valid']


def test_validate_rejects_a_bad_body(harness):
    harness.json = {}
    assert harness.validate.PromptLibAPI().post(project_id=PROJECT_ID)[1] == 400


# ---- instruction patch: refuse only what the edit breaks -------------------------------------

def _errors_the_patch_introduced():
    """The helper on its own: the endpoint's other imports need the database."""
    source = (PLUGIN_ROOT / 'api/v2/version_instruction_patch.py').read_text()
    start = source.index('def errors_the_patch_introduced')
    end = source.index('\nclass PromptLibAPI')
    package_dir = PLUGIN_ROOT / 'utils' / 'pipeline_authoring'
    spec = importlib.util.spec_from_file_location(
        'pipeline_authoring_patch', package_dir / '__init__.py', submodule_search_locations=[str(package_dir)])
    package = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = package
    spec.loader.exec_module(package)
    namespace = {'validate_pipeline': package.validate_pipeline}
    exec(source[start:end], namespace)  # pylint: disable=exec-used
    return namespace['errors_the_patch_introduced']


GOOD = {'entry_point': 'a', 'nodes': [
    {'id': 'a', 'type': 'llm', 'input_mapping': {'task': {'type': 'fixed', 'value': 'x'}}, 'transition': 'END'}]}


def _yaml(doc):
    return yaml.safe_dump(doc)


def test_patch_refuses_an_edit_that_breaks_the_pipeline():
    check = _errors_the_patch_introduced()
    broken = yaml.safe_load(_yaml(GOOD))
    broken['nodes'][0]['transition'] = 'ghost'
    assert 'R2' in [f['rule'] for f in check(_yaml(GOOD), _yaml(broken))]


def test_patch_allows_an_edit_to_a_pipeline_that_already_had_errors():
    check = _errors_the_patch_introduced()
    old = yaml.safe_load(_yaml(GOOD))
    old['nodes'].append({'id': 'b', 'type': 'toolkit', 'name': 'Jira', 'transition': 'END'})
    edited = yaml.safe_load(_yaml(old))
    edited['nodes'][0]['input_mapping']['task']['value'] = 'a better prompt'
    assert check(_yaml(old), _yaml(edited)) == []


@pytest.mark.parametrize('before', ['', 'nodes: ['])
def test_patch_refuses_a_text_that_is_still_not_a_pipeline(before):
    # An empty or broken original must not excuse a patched text that is still not a pipeline.
    check = _errors_the_patch_introduced()
    assert [f['rule'] for f in check(before, 'entry_point: [')] == ['R20']


def test_patch_refuses_a_new_error_next_to_an_old_one():
    check = _errors_the_patch_introduced()
    old = yaml.safe_load(_yaml(GOOD))
    old['nodes'].append({'id': 'b', 'type': 'toolkit', 'name': 'Jira', 'transition': 'END'})
    edited = yaml.safe_load(_yaml(old))
    edited['nodes'][0]['transition'] = 'ghost'
    rules = [f['rule'] for f in check(_yaml(old), _yaml(edited))]
    assert 'R2' in rules and 'R24' not in rules  # the old `name`-only node is not held against the edit
