"""#7445: an Auto chat's context window is the smallest among the models Auto can pick.

Run via:
    python tests/run_tests.py unit/utils/test_7445_auto_context_window.py -v
"""
import ast
import pathlib
import sys
import types
from typing import Optional

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

AUTO = {'selection': {'mode': 'auto'}, 'model_name': None}
AUTO_LIMITS = {'context_window': 128_000, 'max_output_tokens': 8_000, 'models': 2}


@pytest.fixture(scope='module')
def cw(utils_path):
    return load_utils_module(utils_path, 'context_window')


class _Rpc:
    def __init__(self, result=None, error=None):
        self.result, self.error, self.calls = result, error, []

    def timeout(self, _seconds):
        return self

    def configurations_get_auto_model_limits(self, project_id, user_id):
        self.calls.append((project_id, user_id))
        if self.error:
            raise self.error
        return self.result


@pytest.fixture
def rpc(cw, monkeypatch):
    rpc = _Rpc(result=AUTO_LIMITS)
    tools = types.ModuleType('tools')
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc))
    monkeypatch.setitem(sys.modules, 'tools', tools)
    monkeypatch.setattr(cw, '_value_cache', {})
    return rpc


def _function(relpath, name, namespace, strip_imports=False):
    source = (PLUGIN_ROOT / relpath).read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    if strip_imports:
        node.body = [n for n in node.body if not isinstance(n, ast.ImportFrom)]
    exec(compile(ast.Module([node], []), relpath, 'exec'), namespace)  # noqa: S102
    return namespace[name]


# --- resolve_context_budget ---------------------------------------------------------------

def test_auto_window_source_only_when_a_window_resolved(cw):
    budget = cw.resolve_context_budget('full', 272_000, window_source='auto')
    assert (budget['model_window'], budget['window_source']) == (272_000, 'auto')
    fallback = cw.resolve_context_budget('full', None, window_source='auto')
    assert (fallback['model_window'], fallback['window_source']) == (cw.DEFAULT_MODEL_WINDOW, 'default')
    assert cw.resolve_context_budget('full', 200_000)['window_source'] == 'model'


@pytest.mark.parametrize('window,balanced,full', [(128_000, 128_000, 128_000), (272_000, 272_000, 272_000),
                                                  (400_000, 272_000, 400_000)])
def test_auto_min_window_drives_balanced_and_full(cw, window, balanced, full):
    assert cw.resolve_context_budget('balanced', window, window_source='auto')['effective_window'] == balanced
    assert cw.resolve_context_budget('full', window, window_source='auto')['effective_window'] == full


# --- get_auto_model_limits ----------------------------------------------------------------

def test_auto_limits_read_once_per_actor_within_ttl(cw, rpc):
    assert cw.get_auto_model_limits(7, 42) == AUTO_LIMITS
    assert cw.get_auto_model_limits(7, 42) == AUTO_LIMITS
    assert rpc.calls == [(7, 42)]


def test_auto_limits_failure_is_empty_and_retried(cw, rpc):
    rpc.error = RuntimeError('rpc down')
    assert cw.get_auto_model_limits(7, 42) == {}
    assert cw.get_auto_model_limits(7, 42) == {}
    assert len(rpc.calls) == 2


@pytest.mark.parametrize('project_id,user_id', [(None, 42), (7, None), (0, 42)])
def test_auto_limits_need_project_and_actor(cw, rpc, project_id, user_id):
    assert cw.get_auto_model_limits(project_id, user_id) == {}
    assert rpc.calls == []


def test_empty_auto_inventory_keeps_default_window(cw, rpc):
    rpc.result = {}
    limits = cw.get_auto_model_limits(7, 42)
    assert limits == {}
    budget = cw.resolve_context_budget('full', limits.get('context_window'), window_source='auto')
    assert (budget['model_window'], budget['window_source']) == (cw.DEFAULT_MODEL_WINDOW, 'default')


# --- chat model pick ----------------------------------------------------------------------

def test_auto_user_participant_is_picked_without_model_name(cw):
    participants = [('application', {'llm_settings': {'model_name': 'agent-model'}}), ('user', {'llm_settings': AUTO})]
    assert cw.pick_chat_llm_settings(participants) == AUTO
    assert cw.is_auto_selection(AUTO) is True
    assert cw.is_auto_selection({'model_name': 'gpt'}) is False
    assert cw.pick_chat_llm_settings([('user', {'llm_settings': {'selection': None}})]) == {}


# --- status before the first response (context_analytics._conversation_model_budget) ----------

class _Session:
    def __init__(self, mappings):
        self.mappings = mappings

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def query(self, *args):
        return self

    def join(self, *args):
        return self

    def filter(self, *args):
        return self

    def all(self):
        return self.mappings


def _status_budget(cw, mappings, auto_limits=None, model_limits=None):
    calls = []
    column = types.SimpleNamespace(id=1, entity_name=2, entity_settings=3, participant_id=4, conversation_id=5)
    namespace = {
        'db': types.SimpleNamespace(get_session=lambda project_id: _Session(mappings)),
        'Participant': column, 'ParticipantMapping': column, 'log': types.SimpleNamespace(warning=print),
        'pick_chat_llm_settings': cw.pick_chat_llm_settings, 'is_auto_selection': cw.is_auto_selection,
        'resolve_context_budget': cw.resolve_context_budget,
        'get_auto_model_limits': lambda *a: calls.append(('auto',) + a) or (auto_limits or {}),
        'get_model_limits': lambda *a: calls.append(('model',) + a) or (model_limits or {}),
    }
    function = _function('utils/context_analytics.py', '_conversation_model_budget', namespace, strip_imports=True)
    return function, calls


def test_status_of_auto_chat_uses_auto_limits(cw):
    budget_of, calls = _status_budget(cw, [('user', {'llm_settings': {**AUTO, 'max_tokens': 50_000}})], AUTO_LIMITS)
    budget = budget_of(7, 11, 'full', 42)
    assert calls == [('auto', 7, 42)]
    assert (budget['model_window'], budget['window_source'], budget['effective_window']) == (128_000, 'auto', 128_000)
    assert (budget['reserved_output'], budget['reserved_output_source']) == (8_000, 'custom')


def test_status_of_concrete_chat_is_unchanged(cw):
    settings = {'model_name': 'gpt', 'model_project_id': 3}
    budget_of, calls = _status_budget(cw, [('user', {'llm_settings': settings})],
                                      model_limits={'context_window': 400_000, 'max_output_tokens': 32_000})
    budget = budget_of(7, 11, 'full', 42)
    assert calls == [('model', 3, 'gpt')]
    assert (budget['model_window'], budget['window_source']) == (400_000, 'model')


def test_status_of_auto_chat_with_empty_inventory_falls_back(cw):
    budget_of, _ = _status_budget(cw, [('user', {'llm_settings': AUTO})])
    budget = budget_of(7, 11, 'balanced', None)
    assert (budget['model_window'], budget['window_source']) == (cw.DEFAULT_MODEL_WINDOW, 'default')


# --- predict path (predict_utils.generate_predict_payload context block) ----------------------

def _predict_context_block(cw, *, is_auto, llm_settings, llm_model_configuration, auto_limits=AUTO_LIMITS):
    """Runs the generate_predict_payload statement that resolves the context budget, as written."""
    source = (PLUGIN_ROOT / 'utils' / 'predict_utils.py').read_text()
    fn = next(n for n in ast.parse(source).body
              if isinstance(n, ast.FunctionDef) and n.name == 'generate_predict_payload')
    block = next(n for n in fn.body if isinstance(n, ast.If) and 'context_settings' in ast.unparse(n.test))
    calls, stored = [], []
    namespace = {'Optional': Optional, 'BALANCED': 'balanced',
                 'budget_for_model_config': cw.budget_for_model_config,
                 'normalize_runtime_max_tokens': lambda v: None if v in (None, -1) else v,
                 'log': types.SimpleNamespace(warning=print)}
    namespace['apply_context_budget'] = _function('utils/predict_utils.py', 'apply_context_budget', namespace)
    context_settings = types.SimpleNamespace(context_window_mode='full', max_context_tokens=None)
    namespace.update({
        'parsed': types.SimpleNamespace(project_id=7, conversation_id='c-1', stream_id=None,
                                        context_settings=context_settings, llm_settings=llm_settings),
        'user_id': 42, 'is_auto': is_auto, 'llm_model_configuration': llm_model_configuration,
        'load_context_meta': lambda *a: {},
        'allowed_manual_context_tokens': lambda strategy: None,
        'get_auto_model_limits': lambda *a: calls.append(a) or auto_limits,
        'store_context_budget': lambda *a: stored.append(a[2]),
    })
    exec(compile(ast.Module([block], []), 'predict_utils', 'exec'), namespace)  # noqa: S102
    return stored[0], context_settings, calls


def test_predict_auto_budget_uses_auto_limits(cw):
    configuration = {}
    budget, settings, calls = _predict_context_block(
        cw, is_auto=True, llm_settings=types.SimpleNamespace(max_tokens=50_000),
        llm_model_configuration=configuration)
    assert calls == [(7, 42)]
    assert (budget['model_window'], budget['window_source'], budget['effective_window']) == (128_000, 'auto', 128_000)
    assert (budget['reserved_output'], budget['reserved_output_source']) == (8_000, 'custom')
    assert settings.effective_window == 128_000
    assert configuration == {}


def test_predict_auto_with_empty_inventory_falls_back(cw):
    budget, _, _ = _predict_context_block(cw, is_auto=True, llm_settings=types.SimpleNamespace(max_tokens=None),
                                          llm_model_configuration={}, auto_limits={})
    assert (budget['model_window'], budget['window_source']) == (cw.DEFAULT_MODEL_WINDOW, 'default')


def test_predict_concrete_model_budget_is_unchanged(cw):
    budget, _, calls = _predict_context_block(
        cw, is_auto=False, llm_settings=types.SimpleNamespace(max_tokens=None),
        llm_model_configuration={'context_window': 1_000_000, 'max_output_tokens': 64_000})
    assert calls == []
    assert (budget['model_window'], budget['window_source'], budget['effective_window']) == (1_000_000, 'model',
                                                                                             1_000_000)
