"""#6809 P1 item 1: an offline-batch run records what the agent did on each case.

``evaluation_execution`` turns the ``predict_sio`` envelope into the stored trajectory and counters;
``run_agent`` attaches it to every outcome; ``split_case_executions`` turns resolved cases into
``eval_case_execution`` rows and keeps the trajectory off the run snapshot.

The envelope below has the shape of a recorded local run (parent agent → sub-agent tool → the
sub-agent's own LLM step → the parent's final answer), with the text replaced.
"""

import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def execution(utils_path):
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def runner(utils_path, execution):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def orchestration(utils_path, execution):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


CALL_ID = 'toolu_child_1'
MODEL = '1_anthropic.claude-haiku-4-5-20251001-v1:0'


def _llm(start, finish, *, text='', calls=(), usage=(10, 2), finish_reason='stop', parent=None):
    step = {
        'type': 'ChatGeneration',
        'text': text,
        'generation_info': {'finish_reason': finish_reason, 'model_name': MODEL},
        'message': {
            'type': 'ai', 'content': text,
            'tool_calls': [{'name': name, 'args': {}, 'id': f'id-{name}'} for name in calls],
            'usage_metadata': ({'input_tokens': usage[0], 'output_tokens': usage[1]}
                               if usage is not None else None),
            'response_metadata': {},
        },
        'timestamp_start': start, 'timestamp_finish': finish,
        'token_source': 'provider' if usage is not None else 'estimate',
    }
    if parent:
        step.update(parent_agent_name=parent, parent_agent_call_id=CALL_ID)
    return step


def _tool(name, start, finish, *, inputs=None, output='done', finish_reason='stop', error=None,
          metadata=None):
    return {
        'tool_name': name, 'tool_inputs': inputs if inputs is not None else {'q': 1},
        'tool_output': output, 'finish_reason': finish_reason, 'error': error,
        'timestamp_start': start, 'timestamp_finish': finish,
        'metadata': metadata if metadata is not None else {'toolkit_name': 'Files', 'toolkit_type': 'github'},
    }


def _envelope():
    return {'result': {
        'chat_history': [{'role': 'assistant', 'content': 'Owls eat small animals.'}],
        'thinking_steps': [
            _llm('2026-10-06T06:27:41.773+00:00', '2026-10-06T06:27:44.431+00:00',
                 calls=['ChildWriter'], usage=(1431, 77), finish_reason='tool_calls'),
            _llm('2026-10-06T06:27:44.462+00:00', '2026-10-06T06:28:27.176+00:00',
                 text='A long essay.', usage=(53, 4376), parent='Child Writer'),
            _llm('2026-10-06T06:28:27.199+00:00', '2026-10-06T06:28:28.952+00:00',
                 text='Owls eat small animals.', usage=(5891, 48)),
        ],
        'tool_calls_dict': {
            'run-1': _tool('ChildWriter', '2026-10-06T06:27:44.437+00:00', '2026-10-06T06:28:27.186+00:00',
                           inputs={'task': 'What do owls eat?'}, output='x' * 46_000,
                           metadata={'toolkit_name': 'Child Writer', 'toolkit_type': 'application',
                                     'parent_agent_call_id': CALL_ID}),
        },
        'token_source': 'provider',
        'error': None,
    }}


# --- build_trajectory ---------------------------------------------------------------------

def test_steps_follow_time_order_across_both_sources(execution):
    trajectory = execution.build_trajectory(_envelope())

    assert [(s['kind'], s.get('tool_name')) for s in trajectory['steps']] == [
        ('llm', None), ('tool', 'ChildWriter'), ('llm', None), ('llm', None),
    ]
    assert [s['i'] for s in trajectory['steps']] == [0, 1, 2, 3]
    assert trajectory['tool_sequence'] == ['ChildWriter']
    assert trajectory['source'] == 'predict_result'
    assert trajectory['truncated'] is False


def test_llm_step_fields(execution):
    first, _, child, _ = execution.build_trajectory(_envelope())['steps']

    assert first['model'] == MODEL
    assert first['tokens'] == {'in': 1431, 'out': 77}
    assert first['token_source'] == 'provider'
    assert first['planned_tools'] == ['ChildWriter']
    assert first['finish_reason'] == 'tool_calls'
    assert first['duration_ms'] == 2658
    assert first['parent_agent'] is None
    assert (child['parent_agent'], child['parent_call_id']) == ('Child Writer', CALL_ID)


def test_sub_agent_call_is_top_level_and_names_its_call(execution):
    """The orchestrator's sub-agent call carries its own call id; its children point at it."""
    tool = execution.build_trajectory(_envelope())['steps'][1]

    assert tool['call_id'] == CALL_ID
    assert (tool['parent_agent'], tool['parent_call_id']) == (None, None)
    assert (tool['toolkit'], tool['toolkit_type']) == ('Child Writer', 'application')
    assert tool['status'] == 'ok' and tool['is_error'] is False
    assert tool['tool_inputs'] == {'task': 'What do owls eat?'}


def test_child_tool_keeps_its_parent(execution):
    envelope = {'result': {'tool_calls_dict': {'r': _tool(
        'read_file', None, None,
        metadata={'toolkit_name': 'Files', 'parent_agent_name': 'Child', 'parent_agent_call_id': CALL_ID})}}}
    step = execution.build_trajectory(envelope)['steps'][0]
    assert (step['parent_agent'], step['parent_call_id'], step['call_id']) == ('Child', CALL_ID, None)


def test_long_tool_output_is_capped(execution):
    tool = execution.build_trajectory(_envelope())['steps'][1]
    assert len(tool['tool_output']) < 46_000
    assert tool['tool_output'].endswith('[truncated]')


def test_missing_usage_is_none_not_zero(execution):
    envelope = {'result': {'thinking_steps': [_llm(None, None, usage=None)]}}
    step = execution.build_trajectory(envelope)['steps'][0]
    assert step['tokens'] is None
    assert step['token_source'] == 'estimate'


def test_tool_status_vocabulary(execution):
    assert execution.tool_step_status({'finish_reason': 'stop'}) == 'ok'
    assert execution.tool_step_status({'finish_reason': 'error'}) == 'error'
    assert execution.tool_step_status({'finish_reason': 'stop', 'error': 'boom'}) == 'error'
    assert execution.tool_step_status({'finish_reason': 'action_required'}) == 'action_required'


def test_untimed_steps_keep_insertion_order_after_timed_ones(execution):
    envelope = {'result': {
        'thinking_steps': [_llm(None, None, text='late')],
        'tool_calls_dict': {'a': _tool('t1', '2026-10-06T06:00:01+00:00', None),
                            'b': _tool('t2', None, None)},
    }}
    steps = execution.build_trajectory(envelope)['steps']
    assert [s.get('tool_name') or s['text'] for s in steps] == ['t1', 'late', 't2']


def test_oversized_trajectory_drops_payloads_then_steps(execution):
    tools = {str(i): _tool('t', f'2026-10-06T06:00:{i % 60:02d}+00:00', None,
                           inputs={'n': i}, output='y' * 3_900)
             for i in range(200)}
    trajectory = execution.build_trajectory({'result': {'tool_calls_dict': tools}})

    import json
    assert len(json.dumps(trajectory)) <= execution.MAX_TRAJECTORY_BYTES
    assert trajectory['truncated'] is True
    assert trajectory['steps'][-1].get('output_omitted') is True
    assert trajectory['steps'][0]['tool_output'] is not None  # earliest payloads survive
    assert len(trajectory['steps']) == 200  # outputs went first, every step still counted


def test_no_envelope_is_none(execution):
    assert execution.build_trajectory({'result': {'chat_history': []}}) is None
    assert execution.build_trajectory(None) is None
    assert execution.build_trajectory('oops') is None


# --- metrics --------------------------------------------------------------------------------

def test_metrics_from_recorded_run(execution):
    metrics = execution.extract_execution(_envelope(), status='ok', latency_ms=47_000)['metrics']
    assert metrics == {
        'llm_calls': 3, 'tool_calls': 1, 'distinct_tools': 1, 'tool_errors': 0,
        'retries': 0, 'redundant_calls': 0, 'step_limit_hit': False,
        'guardrail_events': 0, 'latency_ms': 47_000,
    }


def test_redundant_retries_and_guardrails(execution):
    envelope = {'result': {'tool_calls_dict': {
        '1': _tool('search', '2026-10-06T06:00:01+00:00', None, inputs={'q': 'a'}),
        '2': _tool('search', '2026-10-06T06:00:02+00:00', None, inputs={'q': 'a'}),   # redundant
        '3': _tool('fetch', '2026-10-06T06:00:03+00:00', None, finish_reason='error', error='503'),
        '4': _tool('fetch', '2026-10-06T06:00:04+00:00', None, inputs={'q': 2}),      # retry
        '5': _tool('mcp', '2026-10-06T06:00:05+00:00', None, finish_reason='action_required'),
    }}}
    metrics = execution.extract_execution(envelope, status='ok')['metrics']
    assert metrics['redundant_calls'] == 1
    assert metrics['tool_errors'] == 1
    assert metrics['retries'] == 1
    assert metrics['guardrail_events'] == 1
    assert metrics['distinct_tools'] == 3


def test_step_limit_hit_from_error(execution):
    envelope = {'result': {'thinking_steps': [], 'error': 'Recursion limit of 25 reached'}}
    out = execution.extract_execution(envelope, status='predict_error',
                                      error='Recursion limit of 25 reached')
    assert out['metrics']['step_limit_hit'] is True


# --- trajectory_state (design §13) ---------------------------------------------------------

@pytest.mark.parametrize('status,envelope,expected', [
    ('ok', {'result': {'thinking_steps': [], 'tool_calls_dict': {}}}, ('recorded', None)),
    ('empty', {'result': {'thinking_steps': []}}, ('recorded', None)),
    ('predict_error', {'result': {'tool_calls_dict': {}, 'error': 'x'}}, ('recorded', None)),
    ('ok', {'result': {'chat_history': []}}, ('not_recorded', 'no_envelope')),
    ('timeout', {'task_id': 't'}, ('not_recorded', 'timeout')),
    ('predict_exception', None, ('not_recorded', 'no_envelope')),
    ('unsupported', None, ('not_applicable', 'unsupported')),
])
def test_trajectory_state(execution, status, envelope, expected):
    out = execution.extract_execution(envelope, status=status)
    assert (out['trajectory_state'], out['trajectory_state_reason']) == expected
    assert (out['trajectory'] is not None) == (expected[0] == 'recorded')


def test_zero_step_run_is_recorded_not_missing(execution):
    out = execution.extract_execution({'result': {'thinking_steps': [], 'tool_calls_dict': {}}}, status='ok')
    assert out['trajectory']['steps'] == []
    assert out['metrics']['tool_calls'] == 0


# --- run_agent attaches execution to every outcome ------------------------------------------

def test_run_agent_ok_carries_trajectory_and_latency(runner):
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: _envelope())
    assert out['status'] == 'ok'
    assert out['output'] == 'Owls eat small animals.'
    assert out['execution']['trajectory_state'] == 'recorded'
    assert out['execution']['trajectory']['tool_sequence'] == ['ChildWriter']
    assert isinstance(out['execution']['metrics']['latency_ms'], int)


def test_run_agent_timeout_is_not_recorded(runner):
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: {'task_id': 't'})
    assert out['status'] == 'timeout'
    assert (out['execution']['trajectory_state'], out['execution']['trajectory_state_reason']) == \
        ('not_recorded', 'timeout')


def test_run_agent_exception_is_not_recorded(runner):
    def predict(**_kw):
        raise RuntimeError('rpc down')

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out['status'] == 'predict_exception'
    assert out['execution']['trajectory_state_reason'] == 'no_envelope'


def test_run_agent_unsupported_is_not_applicable(runner):
    out = runner.run_agent(1, {'agent_type': 'pipeline'}, {'input': 'q'}, predict=lambda **kw: None)
    assert out['execution']['trajectory_state'] == 'not_applicable'


def test_run_agent_error_envelope_keeps_its_steps(runner):
    """A failed case still shows what the agent did before it failed (G1)."""
    envelope = _envelope()
    envelope['result']['chat_history'] = []
    envelope['result']['error'] = 'LLM call failed'
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: envelope)
    assert out['status'] == 'predict_error'
    assert out['execution']['trajectory_state'] == 'recorded'
    assert out['execution']['metrics']['llm_calls'] == 3


# --- orchestration: rows off the snapshot ----------------------------------------------------

def test_run_one_case_stashes_execution(orchestration):
    snapshot = {'bindings': [], 'cases': []}
    runner = lambda case: {'status': 'ok', 'output': 'a', 'structure': None,  # noqa: E731
                           'execution': {'trajectory_state': 'recorded', 'trajectory_state_reason': None,
                                         'trajectory': {'steps': []}, 'metrics': {'llm_calls': 1}}}
    case, _ = orchestration.run_one_case({'id': 7, 'input': 'q'}, snapshot, agent_runner=runner)
    assert case['_execution']['status'] == 'ok'
    assert case['_execution']['trajectory_state'] == 'recorded'


def test_split_case_executions(orchestration):
    cases = [
        {'id': 7, 'input': 'q', 'output': 'a',
         '_execution': {'status': 'ok', 'trajectory_state': 'recorded', 'trajectory_state_reason': None,
                        'trajectory': {'steps': []}, 'metrics': {'llm_calls': 1}}},
        {'id': 8, 'input': 'q2'},  # never reached the agent
        {'id': 9, 'input': 'q3', '_agent_error': 'agent timed out after 120s',
         '_execution': {'status': 'timeout', 'trajectory_state': 'not_recorded',
                        'trajectory_state_reason': 'timeout', 'trajectory': None, 'metrics': {}}},
    ]
    rows, stripped = orchestration.split_case_executions(cases)

    assert [(r['dataset_case_id'], r['case_index'], r['status'], r['trajectory_state']) for r in rows] == [
        (7, 0, 'ok', 'recorded'), (9, 2, 'timeout', 'not_recorded'),
    ]
    assert rows[1]['trajectory_state_reason'] == 'timeout'
    assert all('_execution' not in c for c in stripped)
    assert [c['id'] for c in stripped] == [7, 8, 9]
    assert stripped[2]['_agent_error'] == 'agent timed out after 120s'
