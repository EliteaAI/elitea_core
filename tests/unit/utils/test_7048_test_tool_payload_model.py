"""Issue #7048 - run_tool, toolkit-as-MCP and REST test-tool calls must use the project's default model.

The worker reads only the top-level ``llm_model``/``llm_settings``; ``llm.kwargs`` never reaches the
SDK. Without them the SDK fell back to a hardcoded ``gpt-4o-mini`` (and now has to look the default
up itself, which fails the call on a lookup blip).
"""
import ast
from types import SimpleNamespace as NS
from typing import Optional
from unittest.mock import Mock

import pytest

FUNCTIONS = ('default_tool_llm_settings', 'generate_test_tool_payload', 'resolve_test_tool_llm')


@pytest.fixture
def build(models_path):
    path = models_path.parent / 'utils/predict_utils.py'
    body = ast.parse(path.read_text()).body
    functions = [n for n in body if isinstance(n, ast.FunctionDef) and n.name in FUNCTIONS]
    constants = [n for n in body if isinstance(n, ast.Assign)
                 and any(getattr(t, 'id', None) == 'EXPLICIT_MODEL_TEST_LLM_SETTINGS' for t in n.targets)]
    assert len(functions) == len(FUNCTIONS) and len(constants) == 1
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
        'Optional': Optional,
    }
    exec(compile(ast.Module(constants + functions, []), str(path), 'exec'), namespace)

    def run():
        return namespace['generate_test_tool_payload'](2, 3, 4, 'list_branches', {})
    run.resolve = namespace['resolve_test_tool_llm']
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


class TestRestTestToolModel:
    def test_no_model_uses_the_project_default_with_the_old_low_temperature(self, build):
        run, _, _ = build

        llm_model, llm_settings = run.resolve(2, None, None)

        assert llm_model == 'project-default'
        assert llm_settings == {'model_name': 'project-default', 'model_project_id': 1,
                                'max_tokens': 1024, 'openai_compatible': True, 'temperature': 0.1}

    def test_a_reasoning_default_gets_no_custom_temperature(self, build):
        run, rpc, _ = build
        rpc.configurations_get_configuration_model.return_value = {'supports_reasoning': True}

        _, llm_settings = run.resolve(2, None, None)

        assert 'temperature' not in llm_settings

    def test_a_caller_reasoning_effort_gets_no_temperature_beside_it(self, build):
        run, _, _ = build

        _, llm_settings = run.resolve(2, None, {'reasoning_effort': 'low'})

        assert 'temperature' not in llm_settings
        assert llm_settings['reasoning_effort'] == 'low'

    def test_an_explicit_model_with_empty_settings_is_sent_as_is(self, build):
        run, _, _ = build

        assert run.resolve(2, 'o3', {}) == ('o3', {})

    def test_caller_settings_win_over_the_resolved_defaults(self, build):
        run, _, _ = build

        _, llm_settings = run.resolve(2, None, {'max_tokens': 4096, 'temperature': 0.3})

        assert llm_settings['max_tokens'] == 4096
        assert llm_settings['temperature'] == 0.3
        assert llm_settings['model_name'] == 'project-default'

    def test_an_explicit_model_keeps_the_old_default_settings(self, build):
        run, rpc, _ = build

        assert run.resolve(2, 'chosen', None) == ('chosen', {'max_tokens': 1024, 'temperature': 0.1})
        rpc.configurations_get_configuration_model.assert_not_called()

    def test_auto_selection_is_left_alone(self, build):
        run, rpc, _ = build
        settings = {'selection': {'mode': 'auto'}}

        assert run.resolve(2, None, settings) == (None, settings)
        rpc.configurations_get_configuration_model.assert_not_called()

    def test_a_project_without_models_passes_no_model(self, build):
        run, _, default_model = build
        default_model['model_name'] = None

        assert run.resolve(2, None, None) == (None, {})
