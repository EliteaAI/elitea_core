"""#7015 P1: a pipeline can be batch-evaluated, and its trajectory shows the nodes that ran.

The indexer returns a pipeline's node spans in the envelope as ``pipeline_trace``. Each becomes a
``kind: 'node'`` step; the LLM and tool steps inside a node carry its name in ``node``; the
metrics gain ``node_count`` / ``node_path``. Agent envelopes (no ``pipeline_trace``) keep their
exact shape, and the existing trajectory checks score a pipeline as they do an agent.

The envelope below has the shape of a local run of LLM → tool → router → printer, with the text
replaced.

Run via:
    python tests/run_tests.py unit/utils/test_7015_eval_pipeline_trajectory.py -v
"""

import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def execution(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def runner(utils_path, execution):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def judge(utils_path, execution):
    return load_utils_module(utils_path, 'evaluation_ai_judge')


def _ts(second, ms=0):
    return f'2026-10-25T10:00:{second:02d}.{ms:03d}+00:00'


def _span(node, node_type, step, start, finish, *, status='ok', edge=None, **extra):
    span = {'node': node, 'node_type': node_type, 'step': step, 'run_id': f'run-{node}',
            'timestamp_start': start, 'timestamp_finish': finish, 'duration_ms': None,
            'status': status}
    if edge:
        span['edge'] = edge
    span.update(extra)
    return span


def _llm(node, start, finish, text):
    return {
        'text': text, 'generation_info': {'finish_reason': 'stop', 'model_name': 'haiku'},
        'message': {'type': 'ai', 'content': text, 'tool_calls': [],
                    'usage_metadata': {'input_tokens': 20, 'output_tokens': 5},
                    'response_metadata': {'tool_name': node}},
        'langgraph_node': node,
        'timestamp_start': start, 'timestamp_finish': finish, 'token_source': 'provider',
    }


def _envelope(spans=None, *, truncated=False):
    if spans is None:
        spans = [
            _span('Classify', 'llm', 1, _ts(0), _ts(2), edge={'type': 'transitional', 'next': 'Lookup'}),
            _span('Lookup', 'tool', 2, _ts(2, 10), _ts(3)),
            _span('Route', 'router', 3, _ts(3, 10), _ts(3, 20), edge={'type': 'conditional', 'next': 'Answer'}),
            _span('Answer', 'llm', 4, _ts(3, 30), _ts(5)),
            _span('Format', 'printer', 5, _ts(5, 10), _ts(5, 15)),
        ]
    return {'result': {
        'chat_history': [{'role': 'assistant', 'content': 'Refund approved.'}],
        'thinking_steps': [
            _llm('Classify', _ts(0), _ts(2), 'refund'),
            _llm('Answer', _ts(3, 30), _ts(5), 'Refund approved.'),
        ],
        'tool_calls_dict': {'t1': {
            'tool_name': 'get_order', 'tool_inputs': {'id': 7}, 'tool_output': 'paid',
            'finish_reason': 'stop', 'error': None,
            'timestamp_start': _ts(2, 10), 'timestamp_finish': _ts(3),
            'metadata': {'toolkit_name': 'Orders', 'toolkit_type': 'openapi', 'langgraph_node': 'Lookup'},
        }},
        'pipeline_trace': {'spans': spans, 'truncated': truncated},
    }}


# --- the gate ---------------------------------------------------------------------------

def test_pipelines_are_supported(runner):
    assert runner.agent_type_supported({'agent_type': 'pipeline'}) is True
    assert 'pipeline' not in runner.UNSUPPORTED_AGENT_TYPES


def test_a_pipeline_case_is_dispatched_and_records_nodes(runner):
    calls = []

    def predict(**kwargs):
        calls.append(kwargs)
        return _envelope()

    out = runner.run_agent(1, {'agent_type': 'pipeline', 'instructions': 'nodes: []'}, {'input': 'q'},
                           predict=predict)
    assert out['status'] == 'ok' and out['output'] == 'Refund approved.'
    assert len(calls) == 1
    execution = out['execution']
    assert execution['trajectory_state'] == 'recorded'
    assert execution['metrics']['node_path'] == ['Classify', 'Lookup', 'Route', 'Answer', 'Format']


def test_a_pipeline_hitl_pause_fails_the_case_like_an_agent(runner):
    envelope = _envelope([_span('Review', 'hitl', 1, _ts(0), _ts(1), status='interrupted',
                                hitl={'action': 'interrupt'})])
    envelope['result'].update(paused=True, hitl_interrupt={'node_name': 'Review', 'interaction_type': 'hitl'})
    out = runner.run_agent(1, {'agent_type': 'pipeline'}, {'input': 'q'}, predict=lambda **kw: envelope)

    assert out['status'] == 'guardrail_paused'
    trajectory = out['execution']['trajectory']
    assert trajectory['pause']['policy'] == 'fail_case'
    assert trajectory['pause']['node_name'] == 'Review'
    assert trajectory['steps'][0]['hitl'] == {'action': 'interrupt', 'next': None}


# --- build_trajectory ---------------------------------------------------------------------

def test_node_steps_interleave_with_the_steps_they_ran(execution):
    steps = execution.build_trajectory(_envelope())['steps']
    assert [(s['kind'], s.get('node')) for s in steps] == [
        ('node', 'Classify'), ('llm', 'Classify'),
        ('node', 'Lookup'), ('tool', 'Lookup'),
        ('node', 'Route'),
        ('node', 'Answer'), ('llm', 'Answer'),
        ('node', 'Format'),
    ]
    assert [s['i'] for s in steps] == list(range(8))


def test_node_step_fields(execution):
    classify, *_ = execution.build_trajectory(_envelope())['steps']
    assert classify == {
        'kind': 'node', 'node': 'Classify', 'node_type': 'llm', 'graph_step': 1, 'status': 'ok',
        'edge': {'type': 'transitional', 'next': 'Lookup'}, 'hitl': None, 'error': None,
        'started_at': _ts(0), 'duration_ms': 2000, 'i': 0,
    }


def test_node_sequence_and_tool_sequence(execution):
    trajectory = execution.build_trajectory(_envelope())
    assert trajectory['node_sequence'] == ['Classify', 'Lookup', 'Route', 'Answer', 'Format']
    assert trajectory['tool_sequence'] == ['get_order']
    assert trajectory['truncated'] is False


def test_a_capped_trace_marks_the_trajectory_truncated(execution):
    assert execution.build_trajectory(_envelope(truncated=True))['truncated'] is True


def test_a_node_error_is_masked_and_capped(execution):
    span = _span('Lookup', 'tool', 1, _ts(0), _ts(1), status='error',
                 error='api_key=sk-abcdefghijklmnopqrstuvwxyz0123456789 failed')
    step = execution.build_trajectory(_envelope([span]))['steps'][0]
    assert step['status'] == 'error'
    assert 'sk-abcdefghijklmnopqrstuvwxyz0123456789' not in step['error']


def test_agent_envelopes_are_unchanged(execution):
    """No ``pipeline_trace``: no node steps, no ``node_sequence``, no node metrics."""
    envelope = _envelope()
    del envelope['result']['pipeline_trace']
    for step in envelope['result']['thinking_steps']:
        del step['langgraph_node']
    del envelope['result']['tool_calls_dict']['t1']['metadata']['langgraph_node']

    out = execution.extract_execution(envelope, status='ok')
    trajectory = out['trajectory']
    assert 'node_sequence' not in trajectory
    assert {s['kind'] for s in trajectory['steps']} == {'llm', 'tool'}
    assert all(s['node'] is None for s in trajectory['steps'])
    assert 'node_count' not in out['metrics'] and 'node_path' not in out['metrics']


# --- metrics -----------------------------------------------------------------------------

def test_metrics_count_nodes_and_keep_tool_and_llm_counters(execution):
    metrics = execution.extract_execution(_envelope(), status='ok', latency_ms=5200)['metrics']
    assert metrics['node_count'] == 5
    assert metrics['node_path'] == ['Classify', 'Lookup', 'Route', 'Answer', 'Format']
    assert (metrics['llm_calls'], metrics['tool_calls'], metrics['tool_errors']) == (2, 1, 0)


def test_final_answer_is_the_last_llm_node_even_when_a_printer_follows(execution):
    trajectory = execution.build_trajectory(_envelope())
    assert trajectory['steps'][-1]['node_type'] == 'printer'
    assert execution.final_answer_ms(trajectory) == 1970  # Answer's LLM call, 3.030 → 5.000


def test_a_looping_pipeline_lists_every_visit(execution):
    spans = [_span('Ask', 'llm', step, _ts(step), _ts(step, 500)) for step in (1, 2, 3)]
    metrics = execution.extract_execution(_envelope(spans), status='ok')['metrics']
    assert metrics['node_path'] == ['Ask', 'Ask', 'Ask'] and metrics['node_count'] == 3


# --- the judge's view --------------------------------------------------------------------

def test_the_judge_sees_nodes_and_the_edge_taken(execution, judge):
    text = judge.render_trajectory(execution.build_trajectory(_envelope()))
    assert '1. NODE Classify (llm) [ok] → Lookup' in text
    assert 'NODE Format (printer) [ok]' in text
    assert 'TOOL get_order [ok]' in text
