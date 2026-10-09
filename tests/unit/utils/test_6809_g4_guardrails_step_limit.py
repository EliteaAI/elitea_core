"""#6809 P1 item 2 + G4: guardrail pauses, parked runs, blocked tools and the suite step limit.

A batch run has nobody to answer a HITL prompt, so a paused or parked run fails the case with its
own status instead of passing as ``empty``/``ok``. A sensitive tool the guard refused is a step
status, not a pause. A suite's ``steps_limit`` is frozen into the snapshot and reaches the indexer
as ``version_details['meta']['step_limit']`` (design §4.5).

The envelopes follow the indexer builders (``build_success_result`` / ``build_parked_result``) and
the SDK sensitive-tool guard payloads, with the text replaced.
"""
import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def execution(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')  # sibling for the runner's lazy import
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def runner(utils_path, execution):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def orchestration(utils_path, execution):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


def _tool(name, start, *, inputs=None, output='done', finish_reason='stop'):
    return {'tool_name': name, 'tool_inputs': inputs if inputs is not None else {'q': 1},
            'tool_output': output, 'finish_reason': finish_reason, 'error': None,
            'timestamp_start': start, 'timestamp_finish': None, 'metadata': {}}


BLOCKED_OUTPUT = json.dumps({'type': 'sensitive_tool_blocked', 'tool_name': 'delete_branch',
                             'toolkit_name': 'Repo', 'message': 'The user declined this action.'},
                            separators=(',', ':'))

SENSITIVE_INTERRUPT = {
    'type': 'hitl', 'interrupt_id': 'hitl_1', 'guardrail_type': 'sensitive_tool',
    'node_name': 'sensitive_tool_guard', 'message': 'Approve delete?',
    'tool_name': 'delete_branch', 'toolkit_name': 'Repo', 'toolkit_type': 'github',
    'tool_args': {'branch': 'main'},
}


def _paused_envelope(interrupt=SENSITIVE_INTERRUPT, text='I will delete the branch now.'):
    """``build_success_result`` with ``hitl_interrupt`` set; text before the pause is kept."""
    return {'result': {
        'error': None,
        'chat_history': [{'role': 'assistant', 'content': text}],
        'thinking_steps': [],
        'tool_calls_dict': {},
        'hitl_interrupt': interrupt,
        'hitl_interrupts': [interrupt],
        'paused': True,
        'pause_type': 'hitl',
    }}


def _parked_envelope():
    return {'result': {'error': None, 'parallel_parked': True, 'dispatch_epoch': 1,
                       'parallel_dispatch': [{'tool_call_id': 'c1'}], 'thinking_steps': [],
                       'tool_calls_dict': {}}}


# --- step status: blocked ---------------------------------------------------------------------

@pytest.mark.parametrize('output', [BLOCKED_OUTPUT, json.loads(BLOCKED_OUTPUT)])
def test_blocked_sensitive_tool_is_a_step_status(execution, output):
    assert execution.tool_step_status({'finish_reason': 'stop', 'tool_output': output}) == 'blocked'


def test_text_that_mentions_the_marker_is_not_blocked(execution):
    entry = {'finish_reason': 'stop', 'tool_output': 'docs about sensitive_tool_blocked results'}
    assert execution.tool_step_status(entry) == 'ok'


def test_blocked_step_is_counted_not_an_error(execution):
    envelope = {'result': {'tool_calls_dict': {
        'a': _tool('delete_branch', '2026-10-06T06:00:01+00:00', output=BLOCKED_OUTPUT),
        'b': _tool('list_branches', '2026-10-06T06:00:02+00:00'),
    }}}
    out = execution.extract_execution(envelope, status='ok')
    step = out['trajectory']['steps'][0]
    assert (step['status'], step['is_error']) == ('blocked', False)
    assert out['metrics']['guardrail_events'] == 1
    assert out['metrics']['tool_errors'] == 0
    assert 'pause' not in out['trajectory']


# --- HITL pause -------------------------------------------------------------------------------

def test_pause_details_keep_identities_not_arguments(execution):
    pause = execution.pause_details(_paused_envelope())
    assert pause == {'pause_type': 'hitl', 'interaction_type': None, 'guardrail_type': 'sensitive_tool',
                     'node_name': 'sensitive_tool_guard', 'tool_name': 'delete_branch',
                     'toolkit_name': 'Repo', 'interrupts': 1}


def test_parallel_pause_reports_its_first_pending_call(execution):
    aggregate = {'type': 'hitl', 'guardrail_type': 'parallel_sensitive_tools', 'message': 'Review',
                 'pending': [SENSITIVE_INTERRUPT, {**SENSITIVE_INTERRUPT, 'tool_name': 'push'}]}
    pause = execution.pause_details(_paused_envelope(aggregate))
    assert (pause['guardrail_type'], pause['tool_name'], pause['interrupts']) == \
        ('parallel_sensitive_tools', 'delete_branch', 2)


def test_no_pause_is_none(execution):
    assert execution.pause_details({'result': {'chat_history': [], 'error': None}}) is None
    assert execution.pause_details(None) is None


def test_paused_run_records_pause_and_counts_it(execution):
    out = execution.extract_execution(_paused_envelope(), status='guardrail_paused')
    assert out['trajectory_state'] == 'recorded'
    assert out['trajectory']['pause']['tool_name'] == 'delete_branch'
    assert out['metrics']['guardrail_events'] == 1


ASK_USER_TRACEBACK = ('Traceback (most recent call last):\n  ...\n'
                      "langgraph.errors.GraphInterrupt: (Interrupt(value={'type': 'hitl', "
                      "'guardrail_type': 'clarifying_question', 'tool_name': 'ask_user'}),)")

ASK_USER_INTERRUPT = {'type': 'hitl', 'interrupt_id': 'hitl_2', 'guardrail_type': 'clarifying_question',
                      'node_name': 'ask_user', 'tool_name': 'ask_user', 'message': 'Which language?'}


def _ask_user_envelope():
    """Live shape (run 156): the interrupted ``ask_user`` call is an errored entry with the traceback."""
    envelope = _paused_envelope(ASK_USER_INTERRUPT, text='')
    entry = _tool('ask_user', '2026-10-07T11:57:00+00:00', output=None, finish_reason='error')
    entry['error'] = ASK_USER_TRACEBACK
    envelope['result']['tool_calls_dict'] = {'q': entry}
    return envelope


def test_interrupted_call_is_paused_not_a_tool_error(execution):
    out = execution.extract_execution(_ask_user_envelope(), status='guardrail_paused')
    step = out['trajectory']['steps'][0]
    assert (step['status'], step['is_error'], step['error']) == ('paused', False, None)
    assert out['metrics']['tool_errors'] == 0
    assert out['metrics']['retries'] == 0
    # The pause is counted once, on the trajectory, not again for its call.
    assert out['metrics']['guardrail_events'] == 1


def test_interrupt_marker_is_paused_without_a_recorded_pause(execution):
    assert execution.tool_step_status({'finish_reason': 'error', 'error': ASK_USER_TRACEBACK}) == 'paused'


def test_paused_tool_error_without_marker_is_relabelled(execution):
    envelope = _paused_envelope()
    failed = _tool('list_branches', '2026-10-06T06:00:01+00:00', finish_reason='error')
    failed['error'] = 'boom'
    guarded = _tool('delete_branch', '2026-10-06T06:00:02+00:00', output=None, finish_reason='error')
    guarded['error'] = 'Waiting for approval'
    envelope['result']['tool_calls_dict'] = {'a': failed, 'b': guarded}
    out = execution.extract_execution(envelope, status='guardrail_paused')
    assert [s['status'] for s in out['trajectory']['steps']] == ['error', 'paused']
    assert out['metrics']['tool_errors'] == 1


def test_unpaused_run_keeps_its_tool_errors(execution):
    entry = _tool('delete_branch', '2026-10-06T06:00:01+00:00', finish_reason='error')
    entry['error'] = 'boom'
    out = execution.extract_execution({'result': {'tool_calls_dict': {'a': entry}}}, status='ok')
    assert out['trajectory']['steps'][0]['status'] == 'error'
    assert out['metrics']['tool_errors'] == 1


def test_run_agent_pause_fails_the_case_even_with_text(runner):
    """Text before the pause used to pass as ``ok``; nobody answers the prompt in a batch run."""
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                           predict=lambda **kw: _paused_envelope())
    assert out['status'] == 'guardrail_paused'
    assert out['output'] is None
    assert 'delete_branch' in out['error']
    assert out['execution']['trajectory']['pause']['guardrail_type'] == 'sensitive_tool'


def test_run_agent_pause_without_text_is_not_empty(runner):
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                           predict=lambda **kw: _paused_envelope(text=''))
    assert out['status'] == 'guardrail_paused'


# --- parked -----------------------------------------------------------------------------------

def test_run_agent_parked_is_its_own_status(runner):
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                           predict=lambda **kw: _parked_envelope())
    assert out['status'] == 'parked'
    assert out['output'] is None
    assert out['execution']['trajectory_state'] == 'recorded'


def test_failed_statuses_become_agent_errors(orchestration):
    """Default policy (§4.5): the case fails, so every machine binding scores an error row."""
    snapshot = {'bindings': [{'engine': 'code', 'dimension_id': 1, 'evidence_scope': {}}], 'cases': []}
    for status in ('guardrail_paused', 'parked'):
        runner = lambda case, st=status: {'status': st, 'output': None, 'error': f'{st} reason',  # noqa: E731
                                          'structure': None, 'execution': {'trajectory_state': 'recorded'}}
        case, rows = orchestration.run_one_case({'id': 1, 'input': 'q'}, snapshot, agent_runner=runner)
        assert case['_agent_error'] == f'{status} reason'
        assert case['_execution']['status'] == status
        assert [r['status'] for r in rows] == ['error']


# --- step limit -------------------------------------------------------------------------------

def test_step_limit_hit_from_sdk_final_warning(execution):
    envelope = {'result': {
        'chat_history': [{'role': 'assistant',
                          'content': 'Maximum tool execution iterations (3) reached. Stopping tool execution.'}],
        'thinking_steps': [], 'tool_calls_dict': {}, 'error': None}}
    assert execution.extract_execution(envelope, status='ok')['metrics']['step_limit_hit'] is True


def test_ordinary_answer_is_not_a_step_limit_hit(execution):
    envelope = {'result': {'chat_history': [{'role': 'assistant', 'content': 'Owls eat mice.'}],
                           'thinking_steps': [], 'tool_calls_dict': {}}}
    assert execution.extract_execution(envelope, status='ok')['metrics']['step_limit_hit'] is False


def test_predict_data_carries_the_suite_limit(runner):
    vd = {'agent_type': 'openai', 'meta': {'internal_tools': ['x'], 'step_limit': 25}}
    data = runner.build_agent_predict_data(1, vd, 'q', step_limit=3)
    assert data['version_details']['meta'] == {'internal_tools': ['x'], 'step_limit': 3}
    assert vd['meta']['step_limit'] == 25  # the frozen version details are not mutated


def test_predict_data_without_limit_keeps_the_agents_own(runner):
    vd = {'agent_type': 'openai', 'meta': None}
    assert runner.build_agent_predict_data(1, vd, 'q')['version_details']['meta'] is None
    vd = {'agent_type': 'openai', 'meta': {'step_limit': 40}}
    assert runner.build_agent_predict_data(1, vd, 'q')['version_details']['meta']['step_limit'] == 40


def test_suite_limit_of_3_reaches_the_indexer_as_3(runner):
    """The design's acceptance test: ``indexer_agent`` reads ``meta.get('step_limit', 25)``."""
    seen = {}

    def predict(**kwargs):
        seen['meta'] = kwargs['data']['version_details']['meta']
        return {'result': {'chat_history': [{'role': 'assistant', 'content': 'a'}]}}

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict, step_limit=3)
    assert seen['meta'].get('step_limit', 25) == 3


def test_snapshot_freezes_the_suite_limit(orchestration):
    common = dict(dimensions=[], bindings=[], cases=[], application_id=1, application_version_id=2)
    assert orchestration.build_run_snapshot(
        suite={'id': 1, 'name': 's', 'steps_limit': 3}, **common)['suite']['steps_limit'] == 3
    assert orchestration.build_run_snapshot(suite={'id': 1, 'name': 's'}, **common)['suite']['steps_limit'] is None


# --- redundant calls (design §5.2) -----------------------------------------------------------

def test_repeat_after_failure_is_a_retry_not_redundant(execution):
    envelope = {'result': {'tool_calls_dict': {
        '1': _tool('fetch', '2026-10-06T06:00:01+00:00', finish_reason='error'),
        '2': _tool('fetch', '2026-10-06T06:00:02+00:00'),
        '3': _tool('fetch', '2026-10-06T06:00:03+00:00'),
    }}}
    metrics = execution.extract_execution(envelope, status='ok')['metrics']
    assert (metrics['retries'], metrics['redundant_calls']) == (1, 1)


# --- graph-level limit: the SDK's soft-boundary reply (seen live, suite limit 1) ---------------

GRAPH_LIMIT_REPLY = ('Tool step limit 1 reached for this run. You can continue by sending another '
                     'message or refining your request.')


def test_step_limit_hit_from_graph_recursion_reply(execution):
    envelope = {'result': {'chat_history': [{'role': 'assistant', 'content': GRAPH_LIMIT_REPLY}],
                           'thinking_steps': [], 'tool_calls_dict': {}, 'error': None}}
    assert execution.extract_execution(envelope, status='ok')['metrics']['step_limit_hit'] is True


def test_reply_that_discusses_step_limits_is_not_a_hit(execution):
    text = 'If the tool step limit 5 reached for this run, raise it in the suite settings.'
    envelope = {'result': {'chat_history': [{'role': 'assistant', 'content': text}],
                           'thinking_steps': [], 'tool_calls_dict': {}}}
    assert execution.extract_execution(envelope, status='ok')['metrics']['step_limit_hit'] is False
