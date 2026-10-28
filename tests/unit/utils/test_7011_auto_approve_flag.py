"""EL-7011 US-18 - the predict payload carries `auto_approve_sensitive_actions` only when asked.

R1.1: without the argument the payload has no flag (the run pauses for approval).
R3.1: with the argument the payload has `auto_approve_sensitive_actions = true`.

The builder runs from source with its collaborators replaced, the same seam
``test_7064_predict_expansion.py`` uses.

Run via:
    python tests/run_tests.py unit/utils/test_7011_auto_approve_flag.py -v
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
    llm = load_module_with_stubs(models_path / 'pd/llm.py', 'p7011_llm')
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
        'resolve_runtime_skills': lambda version: [], 'consume_message_skills': lambda text, skills: (text, []),
        'merge_mention_candidates': lambda attached, chat: [*attached, *chat],
        'merge_disclosable_skills': lambda own, chat: [*own, *chat],
        'dedupe_internal_mcp_tools': lambda tools: None,
        'inject_stored_logins': lambda *a, **k: None,
        'resolve_internal_mcp_tools': lambda *a: None,
        'log': NS(warning=lambda *a, **k: None, debug=lambda *a, **k: None, info=lambda *a, **k: None),
    }
    namespace = load_cut([function], namespace, 'utils/predict_utils.py')  # the real function cut out of utils/predict_utils.py
    return lambda **kwargs: namespace['generate_predict_payload'](
        Chat(project_id=7, user_input='go', chat_history=[], thread_id='t',
             llm_settings=llm.LLMSettingsModel(model_name='m', max_tokens=500), tools=[]),
        2, skip_expansion=True, **kwargs)


def test_el_7011_r1_1_no_flag_by_default(build):
    assert 'auto_approve_sensitive_actions' not in build()


def test_el_7011_r1_1_false_argument_sends_no_flag(build):
    assert 'auto_approve_sensitive_actions' not in build(auto_approve_sensitive_actions=False)


def test_el_7011_r3_1_true_argument_sets_the_flag(build):
    assert build(auto_approve_sensitive_actions=True)['auto_approve_sensitive_actions'] is True


def test_el_7011_r3_1_predict_sio_passes_the_argument(models_path):
    source = (models_path.parent / 'rpc/application.py').read_text()
    predict_sio = next(n for n in ast.walk(ast.parse(source))
                       if isinstance(n, ast.FunctionDef) and n.name == 'predict_sio')
    call = next(n for n in ast.walk(predict_sio)
                if isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'generate_predict_payload')

    assert 'auto_approve_sensitive_actions' in {k.arg for k in call.keywords}
    assert 'auto_approve_sensitive_actions' in {a.arg for a in predict_sio.args.args}


def test_a_list_of_pairs_reaches_the_payload_unchanged(build):
    pairs = [('Outlook', 'send_email'), ('GitHub', '*')]

    assert build(auto_approve_sensitive_actions=pairs)['auto_approve_sensitive_actions'] == pairs
