"""EL-7011 US-09 R2.1, R2.2 - a schedule owner without a PAT still gets a token, with no socket.

The pre-run check calls `get_predict_token_and_session(project_id, owner_id, sid=None)`.
`predict_utils.py` is loaded from source; only `tools.auth` and the sibling modules are faked.

Run via:
    python tests/run_tests.py integration/test_7011_owner_token_no_socket.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core.utils'
OWNER_ID = 42


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    return mod


@pytest.fixture
def predict_utils(monkeypatch):
    state = types.SimpleNamespace(pats=[], system_token='system-token-for-owner', ensured=[])

    def ensure_system_token(user_id):
        state.ensured.append(user_id)
        if isinstance(state.system_token, Exception):
            raise state.system_token
        return state.system_token

    auth = types.SimpleNamespace(
        list_tokens=lambda user_id: state.pats,
        encode_token=lambda token_id: f'pat-{token_id}',
        ensure_system_token=ensure_system_token,
        sio_users={},  # no socket handshake for a scheduled run
    )
    log = types.SimpleNamespace(warning=lambda *a, **k: None)
    # no request context: a scheduled run has no HTTP request either
    monkeypatch.setitem(sys.modules, 'flask', _module('flask', has_request_context=lambda: False))
    monkeypatch.setitem(sys.modules, 'pylon', _module('pylon'))
    monkeypatch.setitem(sys.modules, 'pylon.core', _module('pylon.core'))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', _module('pylon.core.tools', log=log))
    monkeypatch.setitem(sys.modules, 'tools', _module(
        'tools', VaultClient=object, db=None, serialize=None, this=None,
        auth=auth, rpc_tools=None, context=None))

    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
                 'plugins.elitea_core.models.enums', 'plugins.elitea_core.models.pd', PKG):
        pkg = _module(name)
        pkg.__path__ = []
        monkeypatch.setitem(sys.modules, name, pkg)
    stubs = {
        f'{PKG}.context_window': {'BALANCED': None, 'budget_for_model_config': None, 'is_platform_admin': None,
                                  'manual_context_tokens': None, 'manual_context_tokens_allowed': None,
                                  'strategy_for_predict': None},
        f'{PKG}.mcp_oauth': {'refresh_token': None},
        f'{PKG}.stored_oauth_tokens': {'inject_stored_oauth_tokens': None},
        f'{PKG}.llm_settings': {'normalize_runtime_max_tokens': None},
        f'{PKG}.next_input_suggestion_utils': {'next_input_suggestion_config': None},
        f'{PKG}.skill_mentions': {'merge_disclosable_skills': None, 'merge_mention_candidates': None},
        f'{PKG}.skill_utils': {'consume_message_skills': None, 'resolve_runtime_skills': None},
        f'{PKG}.subagent_prefetch': {'attach_subagent_prefetch': None, 'expand_version_for_runtime': None},
        f'{PKG}.application_tools': {'expand_toolkit_settings': None},
        f'{PKG}.internal_tools': {'resolve_internal_mcp_tools': None, 'dedupe_internal_mcp_tools': None},
        'plugins.elitea_core.models.elitea_tools': {'EliteATool': object},
        'plugins.elitea_core.models.enums.all': {'AgentTypes': object},
        'plugins.elitea_core.models.pd.chat': {'ApplicationChatRequest': object, 'LLMChatRequest': object},
        'plugins.elitea_core.models.pd.tool': {'ToolDetails': object},
    }
    for name, attrs in stubs.items():
        monkeypatch.setitem(sys.modules, name, _module(name, **attrs))

    name = f'{PKG}.predict_utils'
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / 'utils/predict_utils.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    module.state = state
    return module


def test_el_7011_r2_1_owner_without_pat_gets_system_token_with_no_socket(predict_utils):
    # TC-US09-07: no PAT, sid=None -> the owner's system token, no auth session
    token, session = predict_utils.get_predict_token_and_session(5, OWNER_ID, sid=None)

    assert (token, session) == ('system-token-for-owner', None)
    assert predict_utils.state.ensured == [OWNER_ID]


def test_el_7011_r2_1_owner_with_pat_keeps_the_pat(predict_utils):
    # TC-US09-08: a real PAT wins, the system token is never minted
    predict_utils.state.pats = [{'id': 9, 'expires': None}]

    token, session = predict_utils.get_predict_token_and_session(5, OWNER_ID, sid=None)

    assert (token, session) == ('pat-9', None)
    assert predict_utils.state.ensured == []


def test_el_7011_r2_2_no_token_resolves_raises_for_the_run_to_fail(predict_utils):
    # TC-US09-09: mint fails, no socket -> PredictPayloadError, never a system-user fallback
    predict_utils.state.system_token = RuntimeError('suspended')

    with pytest.raises(predict_utils.PredictPayloadError, match='User token not found'):
        predict_utils.get_predict_token_and_session(5, OWNER_ID, sid=None)
