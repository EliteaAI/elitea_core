"""#6716 P1 Slice B: settling a run's usage rows against the usage ledger (design §4.3).

Each case's agent and judge calls carry ``eval_<role>_<platform_run_id>_<case_index>_<uid>``
as their stream id. The predict path stores that as ``usage_event.conversation_id``, so the
run's ledger rows can be split back per case and role. The ledger's figures replace the runtime
ones where it has calls. A row it has nothing for keeps its runtime figure, unsettled.

Run via:
    python tests/run_tests.py unit/utils/test_6716_eval_settlement.py -v
"""
import pathlib
import sys
from decimal import Decimal

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

PRID = '6f1c2a90-3b4d-4e5f-8a7b-0123456789ab'


@pytest.fixture(scope='module')
def usage(utils_path):
    return load_utils_module(utils_path, 'evaluation_usage')


@pytest.fixture(scope='module')
def runner(utils_path, usage):
    load_utils_module(utils_path, 'evaluation_execution')
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def judge(utils_path, usage):
    return load_utils_module(utils_path, 'evaluation_ai_judge')


@pytest.fixture(scope='module')
def orch(utils_path, usage, judge):
    load_utils_module(utils_path, 'evaluation_scoring')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


# --- stream ids ----------------------------------------------------------------------------------

def test_stream_key_names_run_case_and_role(usage):
    assert usage.case_stream_key('agent', PRID, 3) == f'eval_agent_{PRID}_3'
    assert usage.case_stream_key('judge', PRID, 0) == f'eval_judge_{PRID}_0'


def test_stream_key_without_run_or_index_is_the_plain_key(usage):
    assert usage.case_stream_key('agent', None, 3) == 'eval_agent'
    assert usage.case_stream_key('judge', PRID, None) == 'eval_judge'


@pytest.mark.parametrize('conversation_id, expected', [
    (f'eval_agent_{PRID}_3_a1b2c3d4e5f6', (3, 'agent')),
    (f'eval_judge_{PRID}_12_a1b2c3d4e5f6', (12, 'judge')),
    (f'eval_agent_{PRID}_3', (3, 'agent')),
    ('eval_agent_a1b2c3d4e5f6', None),                    # a call made before Slice B
    (f'eval_agent_{PRID[:-1]}0_3_a1b2c3d4e5f6', None),     # another run
    (f'eval_agent_{PRID}_x_a1b2c3d4e5f6', None),
    ('chat_123', None), (None, None), (42, None),
])
def test_parse_case_stream(usage, conversation_id, expected):
    assert usage.parse_case_stream(conversation_id, PRID) == expected


def test_run_agent_stream_id_names_the_case(runner):
    seen = {}

    def predict(**kwargs):
        seen.update(kwargs['data'])
        return {'result': {'chat_history': [{'role': 'assistant', 'content': 'a'}]}}

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict,
                     platform_run_id=PRID, case_index=4)
    assert seen['stream_id'].startswith(f'eval_agent_{PRID}_4_')
    assert len(seen['stream_id']) == len(f'eval_agent_{PRID}_4_') + 12  # the uid keeps threads apart
    assert seen['message_id'] == seen['stream_id']


def test_run_agent_without_index_keeps_the_old_stream_id(runner):
    seen = {}

    def predict(**kwargs):
        seen.update(kwargs['data'])
        return {'result': {'chat_history': [{'role': 'assistant', 'content': 'a'}]}}

    runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert seen['stream_id'].startswith('eval_agent_') and PRID not in seen['stream_id']


def test_judge_stream_key_names_the_case(judge):
    seen = {}

    def fake_judge(*args, **kwargs):
        seen.update(kwargs)
        return {'status': 'ok', 'data': {}, 'raw': None}

    judge.evaluate_case(1, {}, {'input': 'q', 'output': 'a'},
                        [{'id': 5, 'name': 'tone', 'description': 'Tone', 'scale_type': 'continuous',
                          'scale_min': 0, 'scale_max': 10}],
                        judge=fake_judge, platform_run_id=PRID, case_index=2)
    assert seen['stream_key'] == f'eval_judge_{PRID}_2'


# --- orchestration hands each case its index ----------------------------------------------------

def _snapshot(orch, n, engine='ai'):
    return orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S'},
        dimensions=[{'id': 5, 'name': 'tone', 'scale_type': 'continuous', 'scale_min': 0, 'scale_max': 10}],
        bindings=[{'engine': engine, 'dimension_id': 5, 'weight': 1.0}],
        cases=[{'id': 100 + i, 'input': f'q{i}', 'output': None} for i in range(n)],
        application_id=10, application_version_id=99,
    )


@pytest.mark.parametrize('concurrency', [1, 3])
def test_marked_runner_and_scorer_get_the_case_index(orch, concurrency):
    agent_seen, judge_seen = [], []

    def agent(case, case_index=None):
        agent_seen.append((case['id'], case_index))
        return {'status': 'ok', 'output': 'a', 'error': None, 'structure': None}
    agent.accepts_case_index = True

    def scorer(evidence, dims, case_index=None):
        judge_seen.append(case_index)
        return [{'dimension_id': d['dimension_id'], 'native_score': 5, 'rationale': 'r',
                 'status': 'scored', 'error': None} for d in dims]
    scorer.accepts_case_index = True

    orch.orchestrate_run(_snapshot(orch, 4), agent_runner=agent, ai_scorer=scorer,
                         case_concurrency=concurrency)
    assert sorted(agent_seen) == [(100, 0), (101, 1), (102, 2), (103, 3)]
    assert sorted(judge_seen) == [0, 1, 2, 3]


def test_unmarked_runner_is_called_as_before(orch):
    seen = []

    def agent(case):
        seen.append(case['id'])
        return {'status': 'ok', 'output': 'a', 'error': None, 'structure': None}

    orch.orchestrate_run(_snapshot(orch, 2, engine='code'), agent_runner=agent,
                         code_scorer=lambda b, e: {'score': 1.0, 'passed': True})
    assert seen == [100, 101]


# --- settle_usage_rows ---------------------------------------------------------------------------

def _row(usage, index, role, *, tokens=(100, 10), cost='0.01', state='recorded', status='ok'):
    return {'dataset_case_id': 100 + index, 'case_index': index, 'role': role,
            'input_tokens': tokens[0], 'output_tokens': tokens[1], 'cache_read_tokens': 0,
            'cache_creation_tokens': 0, 'reasoning_tokens': 0,
            'cost': Decimal(cost) if cost is not None else None, 'model_name': 'gpt-4o',
            'usage_state': state, 'usage_state_reason': None if state == 'recorded' else 'no_envelope',
            'token_source': 'provider', 'cost_source': usage.COST_RUNTIME, 'case_status': status,
            'settled': False}


def _ledger(role, index, *, calls=1, inp=120, out=12, nano=15_000_000, unpriced=0, unparsed=0,
            uid='a1b2c3d4e5f6'):
    return {'conversation_id': f'eval_{role}_{PRID}_{index}_{uid}', 'llm_calls': calls,
            'input_tokens': inp, 'output_tokens': out, 'cache_read_tokens': 5,
            'cache_creation_tokens': 0, 'reasoning_tokens': 0, 'cost_nano_usd': nano,
            'unpriced_calls': unpriced, 'unparsed_calls': unparsed, 'model_name': 'gpt-4o-2024'}


def test_ledger_figures_replace_the_runtime_ones(usage):
    rows, settlement = usage.settle_usage_rows(
        [_row(usage, 0, 'agent'), _row(usage, 0, 'judge')],
        [_ledger('agent', 0), _ledger('judge', 0, inp=40, out=4, nano=1_000)], PRID)
    agent, judge_row = rows
    assert (agent['input_tokens'], agent['output_tokens'], agent['cache_read_tokens']) == (120, 12, 5)
    assert agent['cost'] == Decimal('0.015')
    assert (agent['cost_source'], agent['token_source'], agent['settled']) == ('usage_event', 'usage_event', True)
    assert agent['model_name'] == 'gpt-4o'  # the runtime's most-used model is kept
    assert judge_row['cost'] == Decimal('0.000001')
    assert settlement == {'state': 'settled', 'settled_rows': 2, 'expected_rows': 2,
                          'ledger_calls': 2, 'unparsed_rows': 0, 'unmatched_ledger_rows': 0}


def test_several_calls_of_one_case_are_summed(usage):
    """Each judge batch is its own call with its own uid; the case's row is their sum."""
    rows, _ = usage.settle_usage_rows(
        [_row(usage, 1, 'judge')],
        [_ledger('judge', 1, uid='aaaaaaaaaaaa'), _ledger('judge', 1, uid='bbbbbbbbbbbb', inp=30)], PRID)
    assert rows[0]['input_tokens'] == 150
    assert rows[0]['cost'] == Decimal('0.03')


def test_an_unpriced_call_leaves_the_cost_unknown(usage):
    rows, _ = usage.settle_usage_rows([_row(usage, 0, 'agent')], [_ledger('agent', 0, unpriced=1)], PRID)
    assert (rows[0]['cost'], rows[0]['cost_source'], rows[0]['settled']) == (None, 'unpriced', True)


def test_an_unparsed_call_keeps_the_runtime_figures(usage):
    # The proxy could not read a response, so the ledger's 0/0 for it is not a reading (#6809 Gap 2).
    original = [_row(usage, 0, 'judge'), _row(usage, 1, 'judge')]
    rows, settlement = usage.settle_usage_rows(
        original, [_ledger('judge', 0, inp=0, out=0, nano=0, unparsed=1), _ledger('judge', 1)], PRID)
    assert rows[0] == original[0]
    assert rows[1]['settled'] is True
    assert (settlement['state'], settlement['settled_rows'], settlement['expected_rows'],
            settlement['unparsed_rows'], settlement['unmatched_ledger_rows']) == ('partial', 1, 2, 1, 0)


def test_one_unparsed_call_among_several_keeps_the_runtime_figures(usage):
    """The parsed calls' sum would understate the case, so the runtime figure stands."""
    original = [_row(usage, 0, 'agent')]
    rows, settlement = usage.settle_usage_rows(
        original, [_ledger('agent', 0, uid='aaaaaaaaaaaa'),
                   _ledger('agent', 0, uid='bbbbbbbbbbbb', inp=0, out=0, nano=0, unparsed=1)], PRID)
    assert rows == original
    assert (settlement['state'], settlement['unparsed_rows']) == ('unavailable', 1)


def test_breakdown_without_unparsed_counts_still_settles(usage):
    """An older usage plugin sends no unparsed_calls; settlement behaves as before."""
    entry = _ledger('agent', 0)
    del entry['unparsed_calls']
    rows, _ = usage.settle_usage_rows([_row(usage, 0, 'agent')], [entry], PRID)
    assert rows[0]['settled'] is True


def test_ledger_settles_a_row_the_runtime_did_not_record(usage):
    row = _row(usage, 0, 'agent', tokens=(0, 0), cost=None, state='not_recorded')
    rows, _ = usage.settle_usage_rows([row], [_ledger('agent', 0)], PRID)
    assert (rows[0]['usage_state'], rows[0]['usage_state_reason'], rows[0]['input_tokens']) == \
        ('recorded', 'ledger', 120)


def test_rows_the_ledger_lacks_keep_their_runtime_figures(usage):
    original = [_row(usage, 0, 'agent'), _row(usage, 1, 'agent')]
    rows, settlement = usage.settle_usage_rows(original, [_ledger('agent', 1)], PRID)
    assert rows[0] == original[0]
    assert rows[1]['settled'] is True
    assert (settlement['state'], settlement['settled_rows'], settlement['expected_rows']) == ('partial', 1, 2)


def test_empty_ledger_is_unavailable(usage):
    original = [_row(usage, 0, 'agent')]
    rows, settlement = usage.settle_usage_rows(original, [], PRID)
    assert rows == original
    assert settlement['state'] == 'unavailable'


def test_refused_and_skipped_rows_are_not_expected(usage):
    rows = [_row(usage, 0, 'agent'),
            _row(usage, 1, 'agent', tokens=(0, 0), cost=None, state='not_recorded', status='budget_blocked'),
            _row(usage, 2, 'agent', tokens=(0, 0), cost=None, state='not_applicable', status='unsupported')]
    _, settlement = usage.settle_usage_rows(rows, [_ledger('agent', 0)], PRID)
    assert (settlement['state'], settlement['expected_rows']) == ('settled', 1)


def test_other_runs_and_zero_call_entries_are_ignored(usage):
    other = {**_ledger('agent', 0), 'conversation_id': 'eval_agent_a1b2c3d4e5f6'}
    zero = {**_ledger('agent', 0), 'llm_calls': 0}
    rows, settlement = usage.settle_usage_rows([_row(usage, 0, 'agent')], [other, zero], PRID)
    assert rows[0]['settled'] is False and settlement['ledger_calls'] == 0


def test_ledger_rows_with_no_usage_row_are_counted(usage):
    _, settlement = usage.settle_usage_rows([_row(usage, 0, 'agent')],
                                            [_ledger('agent', 0), _ledger('agent', 7)], PRID)
    assert (settlement['settled_rows'], settlement['unmatched_ledger_rows']) == (1, 1)


def test_settled_rows_feed_the_rollup_and_verdict(orch, usage):
    rows, _ = usage.settle_usage_rows([_row(usage, 0, 'agent')], [_ledger('agent', 0, inp=900, out=100)], PRID)
    meta = orch.usage_meta(rows, {'per_case': {'tokens': 500}})
    assert meta['agent_usage']['totals']['input_tokens'] == 900
    assert meta['budget_verdict']['per_case']['tokens']['breached_cases'] == 1


# --- poll_ledger_breakdown -----------------------------------------------------------------------

class _Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds

    def __call__(self):
        return self.now


def _reads(*counts):
    queue = [[{'llm_calls': c}] if c else [] for c in counts]

    def fetch():
        return queue.pop(0) if len(queue) > 1 else queue[0]
    return fetch


def test_poll_stops_once_the_ledger_stops_growing(orch):
    clock = _Clock()
    out = orch.poll_ledger_breakdown(_reads(1, 3, 3, 9), sleep=clock.sleep, clock=clock)
    assert out == [{'llm_calls': 3}]
    assert clock.waits == [7, 3, 3]


def test_poll_returns_quickly_when_nothing_reached_the_ledger(orch):
    clock = _Clock()
    assert orch.poll_ledger_breakdown(_reads(0, 0), sleep=clock.sleep, clock=clock) == []
    assert clock.waits == [7, 3]


def test_poll_gives_up_at_the_cap_with_the_latest_read(orch):
    clock = _Clock()
    counter = iter(range(1, 1000))
    out = orch.poll_ledger_breakdown(lambda: [{'llm_calls': next(counter)}], sleep=clock.sleep,
                                     clock=clock, max_wait=20)
    assert clock.now <= 20 + 3
    assert out[0]['llm_calls'] == len(clock.waits)


def test_poll_lets_a_fetch_error_through(orch):
    def fetch():
        raise RuntimeError('rpc down')
    with pytest.raises(RuntimeError):
        orch.poll_ledger_breakdown(fetch, sleep=lambda s: None, clock=lambda: 0.0)
