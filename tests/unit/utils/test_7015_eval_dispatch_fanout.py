"""#7015 P2: a batch case waits for a dispatched sub-agent fan-out instead of failing as ``parked``.

The parent parks, its children run as their own tasks, and ``parallel_dispatch`` re-invokes the
parent as a reconcile task whose id it records under the parked result's thread id and epoch. The
runner polls that pointer, joins the reconcile, and merges the parent's, the children's and the
reconcile's evidence into one envelope. The platform I/O is a stub here.

Run via:
    python tests/run_tests.py unit/utils/test_7015_eval_dispatch_fanout.py -v
"""

import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def dispatch(utils_path):
    return load_utils_module(utils_path, 'evaluation_dispatch')


@pytest.fixture(scope='module')
def execution(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def runner(utils_path, execution, dispatch):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


def _ts(second):
    return f'2026-10-25T10:00:{second:02d}+00:00'


def _llm(second, text, *, agent=None, tokens=(10, 2)):
    step = {'text': text, 'generation_info': {'finish_reason': 'stop', 'model_name': 'haiku'},
            'message': {'type': 'ai', 'content': text, 'tool_calls': [],
                        'usage_metadata': {'input_tokens': tokens[0], 'output_tokens': tokens[1]}},
            'timestamp_start': _ts(second), 'timestamp_finish': _ts(second), 'token_source': 'provider'}
    if agent:
        step['parent_agent_name'] = agent
    return step


def _tool(second, name, *, agent=None):
    metadata = {'toolkit_name': 'Files'}
    if agent:
        metadata['parent_agent_name'] = agent
    return {'tool_name': name, 'tool_inputs': {}, 'tool_output': 'ok', 'finish_reason': 'stop',
            'error': None, 'timestamp_start': _ts(second), 'timestamp_finish': _ts(second),
            'metadata': metadata}


def _parked(epoch='e1', *, steps=None, tokens=(10, 2)):
    return {'result': {
        'error': None, 'parallel_parked': True, 'dispatch_epoch': epoch, 'thread_id': 'T',
        'parallel_dispatch': [{'tool_call_id': 'c1'}], 'reconcile_payload': {'llm': 'x'},
        'parent_stream_id': 's', 'parent_message_id': 'm',
        'thinking_steps': steps if steps is not None else [_llm(0, 'fan out')],
        'tool_calls_dict': {}, 'chat_history_tokens_input': tokens[0],
        'llm_response_tokens_output': tokens[1], 'token_source': 'provider'}}


def _child(agent, second):
    return {'result': {'thinking_steps': [_llm(second, f'{agent} done', agent=agent)],
                       'tool_calls_dict': {f'{agent}-t': _tool(second, 'read_file', agent=agent)},
                       'chat_history_tokens_input': 10, 'llm_response_tokens_output': 2,
                       'token_source': 'provider'}}


def _final(text='Both orders refunded.'):
    return {'result': {'error': None, 'chat_history': [{'role': 'assistant', 'content': text}],
                       'thinking_steps': [_llm(9, text)], 'tool_calls_dict': {},
                       'chat_history_tokens_input': 30, 'llm_response_tokens_output': 5,
                       'token_source': 'provider'}}


class FakeIO:
    """Platform stub: the pointer appears after ``appear_after`` polls, the clock moves on sleep."""

    def __init__(self, dispatch, *, pointers, joins, results=None, appear_after=1, live=None):
        self.now = 0.0
        self.pointers, self.joins = dict(pointers), list(joins)
        self.results = results or {}
        self.appear_after = appear_after
        self.live = live or {}
        self.polls = 0
        self.stopped, self.stopped_epochs, self.joined = [], [], []
        self.io = dispatch.DispatchIO(
            pointer=self.pointer, epoch_children=lambda t, e: self.live.get((t, e), {}),
            join=self.join, task_result=self.results.get,
            stop_task=self.stopped.append, stop_epoch=lambda t, e: self.stopped_epochs.append((t, e)),
            sleep=self.sleep, clock=lambda: self.now)

    def pointer(self, thread_id, epoch):
        self.polls += 1
        if self.polls < self.appear_after:
            return None
        return self.pointers.get((thread_id, epoch))

    def join(self, task_id, timeout):
        self.joined.append((task_id, timeout))
        return self.joins.pop(0)

    def sleep(self, seconds):
        self.now += seconds


# --- merge_envelopes -----------------------------------------------------------------------

def test_merge_keeps_every_step_and_sums_tokens(dispatch):
    merged = dispatch.merge_envelopes([_parked(), _child('Billing', 3), _child('Refunds', 4), _final()])
    inner = merged['result']
    assert [s['text'] for s in inner['thinking_steps']] == [
        'fan out', 'Billing done', 'Refunds done', 'Both orders refunded.']
    assert set(inner['tool_calls_dict']) == {'Billing-t', 'Refunds-t'}
    assert (inner['chat_history_tokens_input'], inner['llm_response_tokens_output']) == (60, 11)
    assert inner['trajectory_source'] == 'predict_result+dispatch'
    assert inner['chat_history'][-1]['content'] == 'Both orders refunded.'


def test_merge_drops_the_park_markers(dispatch):
    inner = dispatch.merge_envelopes([_parked()])['result']
    for key in ('parallel_parked', 'dispatch_epoch', 'reconcile_payload', 'parallel_dispatch'):
        assert key not in inner


def test_merge_token_source(dispatch):
    estimated = _child('Billing', 3)
    estimated['result']['token_source'] = 'estimated'
    assert dispatch.merge_envelopes([_parked(), estimated])['result']['token_source'] == 'mixed'
    blind = _child('Billing', 3)
    blind['result']['token_source'] = 'none'
    assert dispatch.merge_envelopes([_parked(), blind])['result']['token_source'] == 'provider'


def test_merge_of_nothing_is_none(dispatch):
    assert dispatch.merge_envelopes([None, 'x']) is None


# --- await_dispatch -------------------------------------------------------------------------

def _pointer(task_id='R1', children=None):
    return {'task_id': task_id, 'epoch': 'e1',
            'children': children if children is not None else {'cB': 'B1', 'cR': 'R'}}


def test_waits_for_the_pointer_then_joins_the_reconcile(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[_final()], appear_after=3,
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    out = dispatch.await_dispatch(_parked(), deadline=60, io=fake.io)
    assert out['state'] == dispatch.RECONCILED
    assert (out['waves'], out['children']) == (1, 2)
    assert fake.polls == 3 and fake.now == 2.0
    assert fake.joined == [('R1', 58.0)]  # the join gets only what is left of the case's limit
    assert len(out['result']['result']['thinking_steps']) == 4
    assert fake.stopped == [] and fake.stopped_epochs == []


def test_a_second_wave_is_followed(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer('R1'), ('T', 'e2'): _pointer('R2', {})},
                  joins=[_parked('e2', steps=[_llm(6, 'fan out again')]), _final()],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    out = dispatch.await_dispatch(_parked(), deadline=60, io=fake.io)
    assert out['state'] == dispatch.RECONCILED and out['waves'] == 2
    texts = [s['text'] for s in out['result']['result']['thinking_steps']]
    assert texts == ['fan out', 'Billing done', 'Refunds done', 'fan out again', 'Both orders refunded.']


def test_runaway_waves_are_capped(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer('R1', {})}, joins=[_parked()] * 5)
    out = dispatch.await_dispatch(_parked(), deadline=60, io=fake.io, max_waves=3)
    assert out['state'] == dispatch.TOO_MANY_WAVES and out['waves'] == 3
    assert fake.stopped_epochs == [('T', 'e1')]  # the wave past the cap does not run on


def test_no_pointer_by_the_deadline_stops_the_epoch_and_keeps_finished_children(dispatch):
    fake = FakeIO(dispatch, pointers={}, joins=[], live={('T', 'e1'): {'cB': 'B1', 'cR': 'R'}},
                  results={'B1': _child('Billing', 3)})  # R is still running: no result yet
    out = dispatch.await_dispatch(_parked(), deadline=5, io=fake.io)
    assert out['state'] == dispatch.TIMED_OUT
    assert fake.now == 5.0
    assert fake.stopped_epochs == [('T', 'e1')]
    texts = [s['text'] for s in out['result']['result']['thinking_steps']]
    assert texts == ['fan out', 'Billing done']


def test_a_reconcile_that_runs_past_the_deadline_is_stopped(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[...],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    out = dispatch.await_dispatch(_parked(), deadline=30, io=fake.io)
    assert out['state'] == dispatch.TIMED_OUT
    assert fake.stopped == ['R1']
    assert len(out['result']['result']['thinking_steps']) == 3  # the children's work is kept


def test_a_reconcile_whose_join_raises_is_a_reconcile_failure_with_the_evidence(dispatch):
    # join_task raises for a failed or cancelled reconcile task: await_dispatch must not
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})

    def broken(task_id, timeout):
        raise RuntimeError('reconcile task cancelled')

    fake.io.join = broken
    out = dispatch.await_dispatch(_parked(), deadline=30, io=fake.io)
    assert out['state'] == dispatch.RECONCILE_FAILED
    assert fake.stopped == ['R1']
    assert len(out['result']['result']['thinking_steps']) == 3


def test_a_join_that_returns_nothing_is_a_reconcile_failure(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[None],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    assert dispatch.await_dispatch(_parked(), deadline=30, io=fake.io)['state'] == dispatch.RECONCILE_FAILED
    assert fake.stopped == ['R1']


def test_a_cancelled_run_stops_the_epoch_without_waiting_out_the_limit(dispatch):
    fake = FakeIO(dispatch, pointers={}, joins=[], live={('T', 'e1'): {'cB': 'B1'}},
                  results={'B1': _child('Billing', 3)})
    polls = []
    out = dispatch.await_dispatch(_parked(), deadline=600, io=fake.io,
                                  should_cancel=lambda: polls.append(1) or len(polls) >= 3)
    assert out['state'] == dispatch.CANCELLED
    assert fake.now == 2.0 and fake.stopped_epochs == [('T', 'e1')]
    assert [s['text'] for s in out['result']['result']['thinking_steps']] == ['fan out', 'Billing done']


def test_a_failing_cancel_check_is_not_a_cancel(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[_final()], appear_after=3,
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})

    def broken():
        raise RuntimeError('db down')

    out = dispatch.await_dispatch(_parked(), deadline=60, io=fake.io, should_cancel=broken)
    assert out['state'] == dispatch.RECONCILED


def test_a_parked_result_without_an_epoch_is_not_waited_for(dispatch):
    parked = _parked()
    del parked['result']['dispatch_epoch']
    fake = FakeIO(dispatch, pointers={}, joins=[])
    assert dispatch.await_dispatch(parked, deadline=30, io=fake.io)['state'] == dispatch.NOT_RECONCILED
    assert fake.polls == 0


def test_a_pointer_read_error_is_retried(dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer(children={})}, joins=[_final()])
    calls = []
    real = fake.io.pointer

    def flaky(thread_id, epoch):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError('redis')
        return real(thread_id, epoch)

    fake.io.pointer = flaky
    assert dispatch.await_dispatch(_parked(), deadline=30, io=fake.io)['state'] == dispatch.RECONCILED


# --- pipeline_trace ------------------------------------------------------------------------

def _span(node, status='ok'):
    return {'node': node, 'status': status}


def test_merged_traces_join_each_waves_spans(dispatch):
    parked = _parked()
    parked['result']['pipeline_trace'] = {'spans': [_span('Start'), _span('Fan', 'running')],
                                          'truncated': False}
    final = _final()
    final['result']['pipeline_trace'] = {'spans': [_span('Fan'), _span('Answer')], 'truncated': True}
    merged = dispatch.merge_envelopes([parked, _child('Billing', 3), final])['result']
    # the parked wave's open span is the reconcile's first one, finished: listed once
    assert merged['pipeline_trace'] == {'spans': [_span('Start'), _span('Fan'), _span('Answer')],
                                        'truncated': True}


def test_a_running_span_of_another_node_is_kept(dispatch):
    parked = _parked()
    parked['result']['pipeline_trace'] = {'spans': [_span('Fan', 'running')]}
    final = _final()
    final['result']['pipeline_trace'] = {'spans': [_span('Answer')]}
    merged = dispatch.merge_envelopes([parked, final])['result']
    assert merged['pipeline_trace'] == {'spans': [_span('Fan', 'running'), _span('Answer')],
                                        'truncated': False}


def test_envelopes_without_a_trace_merge_without_one(dispatch):
    assert 'pipeline_trace' not in dispatch.merge_envelopes([_parked(), _final()])['result']


# --- run_agent ------------------------------------------------------------------------------

def test_run_agent_scores_the_reconciled_answer(runner, dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[_final()],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'refund both'},
                           predict=lambda **kw: _parked(), dispatch_io=fake.io, timeout=60)
    assert out['status'] == 'ok' and out['output'] == 'Both orders refunded.'
    assert out['dispatch_state'] == 'reconciled'
    execution = out['execution']
    assert execution['trajectory']['source'] == 'predict_result+dispatch'
    assert execution['metrics']['llm_calls'] == 4 and execution['metrics']['tool_calls'] == 2
    assert (execution['metrics']['dispatch_waves'], execution['metrics']['dispatch_children']) == (1, 2)
    agents = {s.get('parent_agent') for s in execution['trajectory']['steps']}
    assert {'Billing', 'Refunds'} <= agents
    assert out['usage']['usage_state'] == 'recorded'
    assert out['usage']['input_tokens'] == 60


def test_run_agent_dispatch_timeout_keeps_partial_evidence(runner, dispatch):
    fake = FakeIO(dispatch, pointers={}, joins=[],
                  live={('T', 'e1'): {'cB': 'B1'}}, results={'B1': _child('Billing', 3)})
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'refund both'},
                           predict=lambda **kw: _parked(), dispatch_io=fake.io, timeout=0)
    assert out['status'] == 'timeout' and out['output'] is None
    assert 'did not finish within 0s' in out['error']
    assert out['execution']['trajectory_state'] == 'recorded'
    assert out['execution']['metrics']['dispatch_state'] == 'timeout'
    assert out['usage']['usage_state'] == 'recorded'
    assert fake.stopped_epochs == [('T', 'e1')]
    assert fake.polls == 1  # out of time on the first poll: no extra waiting


def test_run_agent_turns_a_failing_reconcile_join_into_a_predict_error(runner, dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer()}, joins=[],
                  results={'B1': _child('Billing', 3), 'R': _child('Refunds', 4)})
    fake.io.join = lambda task_id, timeout: (_ for _ in ()).throw(RuntimeError('task failed'))
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'refund both'},
                           predict=lambda **kw: _parked(), dispatch_io=fake.io, timeout=60)
    assert out['status'] == 'predict_error' and 'dispatch_reconcile_failed' in out['error']
    assert out['execution']['metrics']['dispatch_state'] == 'dispatch_reconcile_failed'
    assert out['execution']['trajectory_state'] == 'recorded'


def test_run_agent_passes_the_runs_cancel_to_the_wait(runner, dispatch):
    fake = FakeIO(dispatch, pointers={}, joins=[], live={('T', 'e1'): {}})
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: _parked(),
                           dispatch_io=fake.io, timeout=600, should_cancel=lambda: True)
    assert out['status'] == 'cancelled' and 'dispatch_cancelled' in out['error']
    assert fake.stopped_epochs == [('T', 'e1')]


def test_run_agent_maps_a_capped_fan_out_to_parked(runner, dispatch):
    fake = FakeIO(dispatch, pointers={('T', 'e1'): _pointer('R1', {})}, joins=[_parked()] * 20)
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: _parked(),
                           dispatch_io=fake.io, timeout=600)
    assert out['status'] == 'parked' and 'dispatch_too_many_waves' in out['error']
    assert out['execution']['trajectory_state'] == 'recorded'


def test_run_agent_survives_a_raising_wait(runner, dispatch, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError('bug')

    monkeypatch.setattr(dispatch, 'await_dispatch', boom)
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: _parked(),
                           dispatch_io=FakeIO(dispatch, pointers={}, joins=[]).io, timeout=60)
    assert out['status'] == 'parked' and 'bug' in out['error']
    assert out['execution']['metrics']['dispatch_state'] == 'dispatch_error'


def test_run_agent_without_dispatch_io_still_reports_parked(runner):
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: _parked())
    assert out['status'] == 'parked' and 'dispatch_state' not in out


def test_plain_runs_carry_no_dispatch_metrics(runner, dispatch):
    fake = FakeIO(dispatch, pointers={}, joins=[])
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'},
                           predict=lambda **kw: _final('hi'), dispatch_io=fake.io)
    assert out['status'] == 'ok' and 'dispatch_state' not in out
    assert 'dispatch_state' not in out['execution']['metrics'] and fake.polls == 0

