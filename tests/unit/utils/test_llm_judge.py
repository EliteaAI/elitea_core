"""Unit tests for run_llm_judge — the EVAL-H1 schema-agnostic judge primitive.

These characterize the invocation shape (tool-less, temp-pinned), the task-timeout stop_task
path, the predict-exception / predict-error / unparseable outcomes, and the parse-then-error
ordering that were previously inlined in run_ai_validation. predict_sio / stop_task are injected
via a stubbed ``this.module`` so no live model is needed.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture
def judge_env(utils_path, isolated_sys_modules):
    """Load llm_judge with stubbed pylon.core.tools.log + tools.this."""
    pct = types.ModuleType('pylon.core.tools')
    pct.log = types.SimpleNamespace(error=lambda *a, **k: None, info=lambda *a, **k: None,
                                    warning=lambda *a, **k: None)
    pcore = types.ModuleType('pylon.core')
    pcore.tools = pct
    pcore.__path__ = []
    pylon = types.ModuleType('pylon')
    pylon.core = pcore
    pylon.__path__ = []
    sys.modules['pylon'] = pylon
    sys.modules['pylon.core'] = pcore
    sys.modules['pylon.core.tools'] = pct

    tools = types.ModuleType('tools')
    this = types.SimpleNamespace()
    tools.this = this
    sys.modules['tools'] = tools

    mod = load_utils_module(utils_path, 'llm_judge')
    return mod, this


def _set_predict(this, result=None, raises=None):
    """Wire this.module.predict_sio (+ recording) and this.module.stop_task."""
    calls = {'predict': [], 'stopped': []}

    def predict_sio(**kwargs):
        calls['predict'].append(kwargs)
        if raises is not None:
            raise raises
        return result

    this.module = types.SimpleNamespace(
        predict_sio=predict_sio,
        stop_task=lambda tid: calls['stopped'].append(tid),
    )
    return calls


def _assistant(content):
    return {'result': {'chat_history': [{'role': 'assistant', 'content': content}]}}


# --- happy path + invocation shape -------------------------------------------

def test_ok_parses_json_data(judge_env):
    mod, this = judge_env
    _set_predict(this, result=_assistant('{"scores": [{"dimension_id": 1, "score": 4}]}'))
    out = mod.run_llm_judge(2, {'model_name': 'gpt-4o'}, 'sys', '{}', 30)
    assert out['status'] == 'ok'
    assert out['data'] == {'scores': [{'dimension_id': 1, 'score': 4}]}
    assert out['raw'] is not None


def test_invocation_is_toolless_and_temp_pinned(judge_env):
    mod, this = judge_env
    calls = _set_predict(this, result=_assistant('{"ok": true}'))
    mod.run_llm_judge(
        7, {'model_name': 'm', 'reasoning_effort': 'high', 'max_tokens': 10, 'temperature': 0.9},
        'system prompt', '{"input": "x"}', 45, stream_key='publish_validate_5',
    )
    sent = calls['predict'][0]['data']
    assert sent['tools'] == [] and sent['version_details']['tools'] == []
    assert sent['version_details']['agent_type'] == 'openai'
    ls = sent['llm_settings']
    assert ls['temperature'] == 0.1  # pinned
    assert 'reasoning_effort' not in ls and 'max_tokens' not in ls  # stripped
    assert sent['stream_id'].startswith('publish_validate_5_')
    assert sent['message_id'].startswith('publish_validate_5_')
    assert calls['predict'][0]['skip_expansion'] is True
    assert calls['predict'][0]['await_task_timeout'] == 45


def test_custom_temperature_respected(judge_env):
    mod, this = judge_env
    calls = _set_predict(this, result=_assistant('{"ok": 1}'))
    mod.run_llm_judge(1, {'model_name': 'm'}, 's', '{}', 10, temperature=0.0)
    assert calls['predict'][0]['data']['llm_settings']['temperature'] == 0.0


def test_fenced_json_extracted(judge_env):
    mod, this = judge_env
    _set_predict(this, result=_assistant('```json\n{"scores": []}\n```'))
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'ok' and out['data'] == {'scores': []}


def test_direct_result_without_wrapper(judge_env):
    mod, this = judge_env
    _set_predict(this, result={'chat_history': [{'type': 'ai', 'content': '{"a": 1}'}]})
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'ok' and out['data'] == {'a': 1}


# --- failure outcomes (never raises) -----------------------------------------

def test_timeout_calls_stop_task(judge_env):
    mod, this = judge_env
    calls = _set_predict(this, result={'task_id': 'abc123'})
    out = mod.run_llm_judge(1, {}, 's', '{}', 12)
    assert out['status'] == 'timeout' and out['data'] is None
    assert calls['stopped'] == ['abc123']
    assert '12s' in out['error']


def test_predict_exception_captured(judge_env):
    mod, this = judge_env
    _set_predict(this, raises=RuntimeError('boom'))
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'predict_exception' and out['raw'] is None
    assert 'boom' in out['error']


def test_predict_error_envelope(judge_env):
    mod, this = judge_env
    _set_predict(this, result={'result': {'error': 'model exploded'}})
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'predict_error' and out['error'] == 'model exploded'


def test_predict_error_truncated(judge_env):
    mod, this = judge_env
    _set_predict(this, result={'error': 'x' * 600})
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'predict_error'
    assert out['error'].endswith('…') and len(out['error']) == 501


def test_unparseable_when_text_not_json(judge_env):
    mod, this = judge_env
    _set_predict(this, result=_assistant('I could not comply.'))
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'unparseable' and out['data'] is None


def test_parse_wins_over_error_key(judge_env):
    # A valid assistant JSON response takes precedence even if an 'error' key is present.
    mod, this = judge_env
    result = {'error': 'ignored', 'result': {
        'chat_history': [{'role': 'assistant', 'content': '{"ok": true}'}]}}
    _set_predict(this, result=result)
    out = mod.run_llm_judge(1, {}, 's', '{}', 10)
    assert out['status'] == 'ok' and out['data'] == {'ok': True}
