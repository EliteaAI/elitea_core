"""Issue #7048 - a run_tool / toolkit-as-MCP call must hand the worker the project's default model.

The worker reads only the top-level ``llm_model``/``llm_settings``; ``llm.kwargs`` never reaches the
SDK. Without them the SDK fell back to a hardcoded ``gpt-4o-mini`` (and now has to look the default
up itself, which fails the call on a lookup blip).
"""
import ast
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

FUNCTIONS = ('default_tool_llm_settings', 'generate_test_tool_payload')


@pytest.fixture
def build(models_path):
    path = models_path.parent / 'utils/predict_utils.py'
    functions = [n for n in ast.parse(path.read_text()).body
                 if isinstance(n, ast.FunctionDef) and n.name in FUNCTIONS]
    assert len(functions) == len(FUNCTIONS)
    rpc = Mock()
    rpc.configurations_get_configuration_model.return_value = {'openai_compatible': True}
    default_model = {'model_name': 'project-default', 'model_project_id': 1}
    configurations = NS(get_default_model=lambda project_id: dict(default_model))
    namespace = {
        'VaultClient': lambda project: NS(get_all_secrets=lambda: {}),
        'get_predict_token_and_session': lambda *a: ('token', None),
        'get_predict_base_url': lambda *a: 'https://unit.invalid',
        'this': NS(for_module=lambda name: NS(module=configurations)),
        'rpc_tools': NS(RpcMixin=lambda: NS(rpc=NS(call=rpc))),
        'get_session_cookie_name': lambda: None,
        'get_toolkit_config': lambda *a: {'toolkit_name': 'github'},
        'serialize': lambda value: value,
        'log': NS(warning=lambda *a, **k: None),
    }
    exec(compile(ast.Module(functions, []), str(path), 'exec'), namespace)

    def run():
        return namespace['generate_test_tool_payload'](2, 3, 4, 'list_branches', {})
    return run, rpc, default_model


def test_the_worker_receives_the_project_default_model(build):
    run, rpc, _ = build

    payload = run()

    assert payload['llm_model'] == 'project-default'
    assert payload['llm_settings'] == {'model_name': 'project-default', 'model_project_id': 1,
                                       'max_tokens': 1024, 'openai_compatible': True}
    rpc.configurations_get_configuration_model.assert_called_once_with(1, 'project-default')


def test_a_failed_capability_lookup_still_sends_the_model(build):
    run, rpc, _ = build
    rpc.configurations_get_configuration_model.side_effect = TimeoutError()

    payload = run()

    assert payload['llm_model'] == 'project-default'
    assert 'openai_compatible' not in payload['llm_settings']


def test_a_project_without_models_sends_no_model(build):
    run, rpc, default_model = build
    default_model['model_name'] = None

    payload = run()

    assert payload['llm_model'] is None
    assert payload['llm_settings'] == {}
    rpc.configurations_get_configuration_model.assert_not_called()
