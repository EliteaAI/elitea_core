"""#7383: a run with no socket (scheduled, webhook, blocking REST) has no one to answer a Builder card, so the predict
payload marks its Builder block non-interactive; a chat run keeps it as is.

The real ``generate_predict_payload`` runs with its collaborators replaced, the seam of ``test_7011_auto_approve_flag``.
"""
import ast
from types import SimpleNamespace as NS
from typing import Optional, Union
from unittest.mock import Mock

import pytest
from fixtures.cut_source import load_cut
from fixtures.helpers import load_module_with_stubs


class Chat(NS):
    def __getattr__(self, key):
        return None


@pytest.fixture
def build(models_path):
    llm = load_module_with_stubs(models_path / 'pd/llm.py', 'p7383_llm')
    path = models_path.parent / 'utils/predict_utils.py'
    function = next(n for n in ast.parse(path.read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'generate_predict_payload')
    rpc = Mock()
    rpc.configurations_get_configuration_model.return_value = {'supports_reasoning': False}
    namespace = {
        'Union': Union, 'Optional': Optional, 'LLMChatRequest': Chat, 'ApplicationChatRequest': type('App', (Chat,), {}),
        '_normalize_llm_settings_family': llm._normalize_llm_settings_family,
        'PredictPayloadError': Exception,
        'VaultClient': lambda project: NS(get_all_secrets=dict),
        'rpc_tools': NS(RpcMixin=lambda: NS(rpc=NS(call=rpc, timeout=lambda seconds: rpc))),
        'get_predict_token_and_session': lambda *a, **k: ('token', None),
        'get_system_token': lambda *a: 'token',
        'get_session_cookie_name': lambda: None,
        'get_predict_base_url': lambda *a: 'https://unit.invalid',
        'get_public_base_url': lambda: 'https://public.invalid',
        'normalize_runtime_max_tokens': lambda value: value,
        'next_input_suggestion_config': lambda *a: {'enabled': False},
        'serialize': lambda value: value,
        'resolve_application_name': lambda p: 'fixture',
        'resolve_runtime_skills': lambda version: [], 'consume_message_skills': lambda text, skills: (text, []), 'merge_invoked_skills': lambda message, resumed: [*message, *(resumed or [])],
        'merge_mention_candidates': lambda attached, chat: [*attached, *chat],
        'merge_disclosable_skills': lambda own, chat: [*own, *chat],
        'dedupe_internal_mcp_tools': lambda tools: None,
        'inject_stored_logins': lambda *a, **k: None,
        'skill_scratch_for_run': lambda project_id, conversation_uuid, user_id: {},
        'resolve_internal_mcp_tools': lambda *a: None,
        'strip_code_mode': lambda tools, project_id: tools,
        'log':NS(warning=lambda *a, **k: None, debug=lambda *a, **k: None, info=lambda *a, **k: None),
    }
    namespace = load_cut([function], namespace, 'utils/predict_utils.py')  # the real function cut out of utils/predict_utils.py
    return lambda builder, **kwargs: namespace['generate_predict_payload'](
        Chat(project_id=7, user_input='go', chat_history=[], thread_id='t', builder=builder,
             llm_settings=llm.LLMSettingsModel(model_name='m', max_tokens=500), tools=[]),
        2, skip_expansion=True, **kwargs)


def test_a_run_without_a_socket_gets_a_non_interactive_builder(build):
    assert build({'core_prompt': 'x'})['builder'] == {'core_prompt': 'x', 'interactive': False}


def test_a_chat_run_keeps_the_builder_block_as_is(build):
    assert build({'core_prompt': 'x'}, sid='sid-1')['builder'] == {'core_prompt': 'x'}


def test_a_run_without_the_builder_has_none(build):
    assert build(None)['builder'] is None
