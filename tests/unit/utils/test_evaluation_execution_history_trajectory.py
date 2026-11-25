"""A case scored from a stored chat turn records the same trajectory a live run would.

``trajectory_from_trace_steps`` rebuilds it from ``message_trace_step`` rows; the rows below have
the columns ``trace_step_writer`` writes for the live envelope's entries, so the two must agree on
every tool step and on the order. LLM steps lose what the table does not keep (usage, planned tools).
"""

import pathlib
import sys
from datetime import datetime

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def execution(utils_path):
    return load_utils_module(utils_path, 'evaluation_execution')


T0, T1, T2, T3 = ('2026-11-20T10:00:00+00:00', '2026-11-20T10:00:02+00:00',
                  '2026-11-20T10:00:03+00:00', '2026-11-20T10:00:05+00:00')
MODEL = '1_anthropic.claude-haiku-4-5-20251001-v1:0'
META = {'toolkit_name': 'Files', 'toolkit_type': 'github', 'langgraph_node': 'tools'}


def _envelope(error=None):
    return {'result': {
        'thinking_steps': [
            {'text': 'Let me look.', 'message': {'response_metadata': {'model_name': MODEL}},
             'timestamp_start': T0, 'timestamp_finish': T1},
            {'text': 'Done: token sk-abcdefghijklmnopqrstuvwx', 'message': {'response_metadata': {'model_name': MODEL}},
             'timestamp_start': T3, 'timestamp_finish': T3},
        ],
        'tool_calls_dict': {'run-1': {
            'tool_name': 'read_file', 'tool_inputs': {'path': 'a.md', 'api_key': 'secret-value'},
            'tool_output': 'contents', 'finish_reason': 'error' if error else 'stop', 'error': error,
            'timestamp_start': T1, 'timestamp_finish': T2, 'metadata': dict(META),
        }},
    }}


def _rows(error=None):
    ts = datetime.fromisoformat
    # Reverse id order on purpose: the rebuild orders by started_at, not by how rows come back.
    return [
        {'kind': 'thinking_step', 'text': 'Done: token sk-abcdefghijklmnopqrstuvwx', 'model_name': MODEL,
         'started_at': ts(T3), 'finished_at': ts(T3), 'attrs': None},
        {'kind': 'tool_call', 'tool_name': 'read_file', 'tool_inputs': {'path': 'a.md', 'api_key': 'secret-value'},
         'tool_output': 'contents', 'finish_reason': 'error' if error else 'stop', 'is_error': bool(error),
         'started_at': ts(T1), 'finished_at': ts(T2), 'parent_agent_name': None, 'parent_agent_call_id': None,
         'attrs': {'metadata': dict(META)}},
        {'kind': 'thinking_step', 'text': 'Let me look.', 'model_name': MODEL,
         'started_at': ts(T0), 'finished_at': ts(T1), 'attrs': None},
        {'kind': 'unknown'},
    ]


def _comparable(step, *drop):
    skip = ('started_at', 'tokens', 'token_source', 'planned_tools', *drop)
    return {k: v for k, v in step.items() if k not in skip}


@pytest.mark.parametrize('error', [None, 'Boom'])
def test_matches_live_trajectory(execution, error):
    live = execution.build_trajectory(_envelope(error))
    stored = execution.trajectory_from_trace_steps(_rows(error))
    assert [s['kind'] for s in stored['steps']] == ['llm', 'tool', 'llm']
    assert stored['tool_sequence'] == live['tool_sequence'] == ['read_file']
    # The table has no error column: the writer reads a failed call's error back from its output.
    drop = ('error',) if error else ()
    assert [_comparable(s, *drop) for s in stored['steps']] == [_comparable(s, *drop) for s in live['steps']]
    assert stored['source'] == 'history'
    if error:
        assert stored['steps'][1]['error'] == 'contents' and stored['steps'][1]['status'] == 'error'


def test_masks_like_live(execution):
    stored = execution.trajectory_from_trace_steps(_rows())
    tool = stored['steps'][1]
    assert tool['tool_inputs']['api_key'] != 'secret-value'
    assert 'abcdefghijklmnop' not in stored['steps'][2]['text']


def test_tool_outputs_opt_out(execution):
    stored = execution.trajectory_from_trace_steps(_rows(), store_tool_outputs=False)
    assert stored['steps'][1]['tool_output'] is None
    assert stored['steps'][1]['output_omitted'] is True
    assert stored['tool_outputs_stored'] is False


def test_recorded_execution(execution):
    recorded = execution.recorded_execution(_rows(), latency_ms=5000, final_text='Done')
    assert recorded['trajectory_state'] == 'recorded'
    assert recorded['source'] == 'history' and recorded['status'] == 'ok'
    assert all('_identity' not in s for s in recorded['trajectory']['steps'])
    assert recorded['metrics']['tool_calls'] == 1
    assert recorded['metrics']['latency_ms'] == 5000


def test_no_rows_is_an_empty_recorded_trajectory(execution):
    recorded = execution.recorded_execution([])
    assert recorded['trajectory']['steps'] == []
    assert recorded['trajectory_state'] == 'recorded'
