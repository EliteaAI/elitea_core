"""Unit tests for evaluation_agent_runner.py — live agent execution (EVAL-H4, §14.2/§17.1/§8.1).

Pure builders + extraction + the injected-``predict`` dispatch. These lock the P1 agent_type scope
(pipelines deferred), the §17.1 variable overlay, the assistant-output extraction, and the
fail-closed outcome mapping (timeout / predict error / empty → a value, never a raise).

``run_agent``'s default ``predict`` is ``this.module.predict_sio``; every test injects a stub so the
module loads and runs without ``tools``/SDK present.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def runner(utils_path):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


# --- agent_type_supported: P1 scope (pipelines deferred) ---------------------

def test_openai_supported(runner):
    assert runner.agent_type_supported({'agent_type': 'openai'}) is True


def test_react_supported(runner):
    assert runner.agent_type_supported({'agent_type': 'react'}) is True


def test_pipeline_unsupported(runner):
    assert runner.agent_type_supported({'agent_type': 'pipeline'}) is False


def test_missing_agent_type_defaults_supported(runner):
    assert runner.agent_type_supported({}) is True


# --- agent_structure_snapshot: §19.4 evidence_scope.structure ----------------

def test_structure_snapshot_extracts_known_fields(runner):
    vd = {
        'agent_type': 'react',
        'instructions': 'be helpful',
        'llm_settings': {'model': 'gpt-4o'},
        'tools': [{'name': 'search'}],
        'skills': [{'name': 'summarize'}],
        'meta': {'owner': 'team-a'},
        'variables': [{'name': 'x', 'value': '1'}],  # not part of "structure"
    }
    snapshot = runner.agent_structure_snapshot(vd)
    assert snapshot == {
        'agent_type': 'react',
        'instructions': 'be helpful',
        'llm_settings': {'model': 'gpt-4o'},
        'tools': [{'name': 'search'}],
        'skills': [{'name': 'summarize'}],
        'meta': {'owner': 'team-a'},
    }


def test_structure_snapshot_handles_none(runner):
    assert runner.agent_structure_snapshot(None) == {
        'agent_type': None, 'instructions': None, 'llm_settings': None,
        'tools': None, 'skills': None, 'meta': None,
    }


# --- merge_case_variables: §17.1 overlay -------------------------------------

def test_case_value_overrides_matching_version_variable(runner):
    merged = runner.merge_case_variables(
        [{'name': 'city', 'value': 'default'}], {'city': 'Paris'})
    assert merged == [{'name': 'city', 'value': 'Paris'}]


def test_unmatched_case_key_appended(runner):
    merged = runner.merge_case_variables([{'name': 'a', 'value': 1}], {'b': 2})
    assert {'name': 'a', 'value': 1} in merged
    assert {'name': 'b', 'value': 2} in merged


def test_no_case_variables_keeps_version_defaults(runner):
    merged = runner.merge_case_variables([{'name': 'a', 'value': 1}], None)
    assert merged == [{'name': 'a', 'value': 1}]


def test_merge_does_not_mutate_version_variables(runner):
    version_vars = [{'name': 'a', 'value': 1}]
    runner.merge_case_variables(version_vars, {'a': 99})
    assert version_vars == [{'name': 'a', 'value': 1}]  # original untouched


# --- build_agent_predict_data: pure payload shape ----------------------------

def test_predict_data_uses_real_version_details(runner):
    vd = {'agent_type': 'react', 'instructions': 'be helpful',
          'llm_settings': {'model_name': 'm'}, 'tools': [{'name': 't'}]}
    data = runner.build_agent_predict_data(2, vd, 'hello')
    assert data['project_id'] == 2
    assert data['user_input'] == 'hello'
    assert data['version_details']['instructions'] == 'be helpful'
    assert data['tools'] == [{'name': 't'}]
    assert data['chat_history'] == []


def test_predict_data_blank_input_falls_back_to_continue(runner):
    data = runner.build_agent_predict_data(1, {'agent_type': 'openai'}, '   ')
    assert data['user_input'] == 'continue'


def test_predict_data_overlays_case_variables(runner):
    vd = {'agent_type': 'openai', 'variables': [{'name': 'x', 'value': 'old'}]}
    data = runner.build_agent_predict_data(1, vd, 'q', {'x': 'new'})
    assert data['version_details']['variables'] == [{'name': 'x', 'value': 'new'}]


def test_predict_data_deepcopies_version_details(runner):
    vd = {'agent_type': 'openai', 'variables': [{'name': 'x', 'value': 'old'}]}
    runner.build_agent_predict_data(1, vd, 'q', {'x': 'new'})
    assert vd['variables'] == [{'name': 'x', 'value': 'old'}]  # source untouched


# --- extract_agent_output ----------------------------------------------------

def _wrap(chat_history):
    return {'result': {'chat_history': chat_history}}


def test_extract_last_assistant_message(runner):
    out = runner.extract_agent_output(_wrap([
        {'role': 'user', 'content': 'q'},
        {'role': 'assistant', 'content': 'first'},
        {'type': 'ai', 'content': 'final answer'},
    ]))
    assert out == 'final answer'


def test_extract_skips_empty_assistant_and_returns_earlier(runner):
    out = runner.extract_agent_output(_wrap([
        {'role': 'assistant', 'content': 'real'},
        {'role': 'assistant', 'content': '   '},
    ]))
    assert out == 'real'


def test_extract_none_when_no_assistant(runner):
    assert runner.extract_agent_output(_wrap([{'role': 'user', 'content': 'q'}])) is None


def test_extract_none_on_empty_history(runner):
    assert runner.extract_agent_output(_wrap([])) is None


# --- run_agent: dispatch + fail-closed mapping (E4) ---------------------------

def test_run_agent_unsupported_short_circuits(runner):
    calls = []
    out = runner.run_agent(1, {'agent_type': 'pipeline'}, {'input': 'q'},
                           predict=lambda **kw: calls.append(kw))
    assert out['status'] == 'unsupported'
    assert out['output'] is None
    assert calls == []  # never dispatched


def test_run_agent_ok_returns_output(runner):
    def predict(**kwargs):
        return {'result': {'chat_history': [{'role': 'assistant', 'content': 'the answer'}]}}

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out == {'status': 'ok', 'output': 'the answer', 'error': None}


def test_run_agent_timeout_maps_closed(runner):
    def predict(**kwargs):
        return {'task_id': 'abc'}  # no 'result' -> timeout path

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict, timeout=5)
    assert out['status'] == 'timeout'
    assert '5s' in out['error']


def test_run_agent_predict_exception_maps_closed(runner):
    def predict(**kwargs):
        raise RuntimeError('boom')

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out['status'] == 'predict_exception'
    assert 'boom' in out['error']


def test_run_agent_error_envelope_maps_predict_error(runner):
    def predict(**kwargs):
        return {'result': {'chat_history': [], 'error': 'model refused'}}

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out['status'] == 'predict_error'
    assert 'model refused' in out['error']


def test_run_agent_empty_output_maps_empty(runner):
    def predict(**kwargs):
        return {'result': {'chat_history': [{'role': 'user', 'content': 'q'}]}}

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out['status'] == 'empty'
    assert out['output'] is None
