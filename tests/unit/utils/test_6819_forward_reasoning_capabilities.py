"""#6819: the predict payload forwards the model row's reasoning capability fields.

The SDK picks the Anthropic thinking mode from ``thinking_type``; without this forward the
admin's stored choice never leaves core. The fields ride only on reasoning models, and a
row that predates them (all null) produces exactly today's payload.
"""
import ast
import copy
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Union
from unittest.mock import Mock

import pytest
from fixtures.helpers import load_module_with_stubs

CAPABILITIES = {'thinking_type': 'always_on', 'supported_efforts': ['low', 'medium', 'high', 'xhigh', 'max'],
                'default_effort': 'high'}


class Chat(NS):
    def __getattr__(self, key):
        return None


@pytest.fixture
def builder(models_path):
    llm = load_module_with_stubs(models_path / 'pd/llm.py', 'forward_capabilities_llm')
    path = models_path.parent / 'utils/predict_utils.py'
    function = next(n for n in ast.parse(path.read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'generate_predict_payload')

    class Imports(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            return None

    function = Imports().visit(function)
    rpc = Mock()
    namespace = {'Union': Union, 'LLMChatRequest': Chat, 'ApplicationChatRequest': Chat,
                 '_normalize_llm_settings_family': llm._normalize_llm_settings_family,
                 'PredictPayloadError': ValueError, 'VaultClient': lambda project: NS(get_all_secrets=lambda: {}),
                 'rpc_tools': NS(RpcMixin=lambda: NS(rpc=NS(call=rpc))),
                 'get_predict_token_and_session': lambda *a: ('fixture-only', None),
                 'get_predict_base_url': lambda *a: 'https://unit.invalid',
                 'normalize_runtime_max_tokens': lambda value: None if value in (None, -1) else value,
                 'next_input_suggestion_config': lambda *a: {'enabled': False},
                 'serialize': lambda value: value,
                 'AgentTypes': NS(pipeline=NS(value='pipeline')), 'resolve_application_name': lambda p: 'fixture',
                 'resolve_runtime_skills': lambda version: [], 'consume_invoked_skills': lambda text, skills: (text, [])}
    exec(compile(ast.Module([function], []), str(path), 'exec'), namespace)

    def build(model_row, **settings):
        rpc.configurations_get_configuration_model.return_value = copy.deepcopy(model_row)
        request = Chat(project_id=7, chat_history=[], user_input='hello', thread_id='t', tools=[],
                       llm_settings=llm.LLMSettingsModel(model_name='fixed-model', max_tokens=8000, **settings))
        return build_payload(request, 2, skip_expansion=True)['llm']['kwargs']

    build_payload = namespace['generate_predict_payload']
    return build


def test_reasoning_model_row_forwards_the_three_capability_fields(builder):
    kwargs = builder({'supports_reasoning': True, 'max_output_tokens': 16000, **CAPABILITIES}, reasoning_effort='high')
    assert {key: kwargs[key] for key in CAPABILITIES} == CAPABILITIES
    assert kwargs['reasoning_effort'] == 'high' and 'temperature' not in kwargs


def test_row_without_the_fields_produces_todays_payload(builder):
    kwargs = builder({'supports_reasoning': True, 'max_output_tokens': 16000,
                      'thinking_type': None, 'supported_efforts': None, 'default_effort': None}, reasoning_effort='high')
    assert not set(CAPABILITIES) & set(kwargs)
    assert kwargs['reasoning_effort'] == 'high'


def test_non_reasoning_row_never_forwards_capabilities(builder):
    kwargs = builder({'supports_reasoning': False, 'max_output_tokens': 16000, **CAPABILITIES}, temperature=0.4)
    assert not set(CAPABILITIES) & set(kwargs)
    assert 'reasoning_effort' not in kwargs and kwargs['temperature'] == 0.4


def test_partial_row_forwards_only_the_set_fields(builder):
    kwargs = builder({'supports_reasoning': True, 'max_output_tokens': 16000, 'thinking_type': 'adaptive'},
                     reasoning_effort='medium')
    assert kwargs['thinking_type'] == 'adaptive'
    assert 'supported_efforts' not in kwargs and 'default_effort' not in kwargs
