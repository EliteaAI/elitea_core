"""#6716 P1 Slice A: per-case token/cost capture, the consumption budget and early stops.

A figure that was not measured stays missing: no envelope or no callback is ``not_recorded``, an
unpriced model is ``cost=None``, and a limit checked against a missing figure is ``unknown``, never
``pass``. The envelopes follow the indexer's ``build_execution_evidence`` (thinking steps carrying
LangChain ``usage_metadata``, plus the callback's envelope totals and ``token_source``).

Run via:
    python tests/run_tests.py unit/utils/test_6716_eval_usage.py -v
"""
import pathlib
import sys
from decimal import Decimal

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


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


def _step(model, inp, out, *, cache_read=0, cache_creation=0, reasoning=0):
    usage_metadata = {'input_tokens': inp, 'output_tokens': out, 'total_tokens': inp + out}
    if cache_read or cache_creation:
        usage_metadata['input_token_details'] = {'cache_read': cache_read, 'cache_creation': cache_creation}
    if reasoning:
        usage_metadata['output_token_details'] = {'reasoning': reasoning}
    return {'message': {'type': 'ai', 'usage_metadata': usage_metadata,
                        'response_metadata': {'model_name': model}},
            'generation_info': {'model_name': model}, 'token_source': 'provider'}


def _estimated_step(model):
    """A call the provider sent no usage for: the callback counted it, the step has nothing."""
    return {'message': {'type': 'ai', 'response_metadata': {'model_name': model}},
            'generation_info': {'model_name': model}, 'token_source': 'estimate'}


def _envelope(steps, inp, out, token_source='provider'):
    return {'result': {'chat_history': [{'role': 'assistant', 'content': 'answer'}],
                       'thinking_steps': steps, 'tool_calls_dict': {},
                       'chat_history_tokens_input': inp, 'llm_response_tokens_output': out,
                       'token_source': token_source}}


# --- envelope parsing ---------------------------------------------------------------------------

def test_provider_steps_split_by_model(usage):
    env = _envelope([_step('gpt-4o', 100, 10, cache_read=40), _step('gpt-4o', 200, 20),
                     _step('haiku', 50, 5, reasoning=3)], 350, 35)
    u = usage.envelope_usage(env)
    assert (u['usage_state'], u['token_source'], u['model_name']) == ('recorded', 'provider', 'gpt-4o')
    assert (u['input_tokens'], u['output_tokens'], u['cache_read_tokens'], u['reasoning_tokens']) == \
        (350, 35, 40, 3)
    assert u['models']['gpt-4o']['input_tokens'] == 300
    assert u['models']['haiku']['output_tokens'] == 5


def test_estimated_calls_reach_the_total_through_the_remainder(usage):
    """Estimated steps carry no usage_metadata; the callback's envelope totals still count them."""
    env = _envelope([_step('gpt-4o', 100, 10), _estimated_step('gpt-4o')], 180, 25, token_source='mixed')
    u = usage.envelope_usage(env)
    assert (u['input_tokens'], u['output_tokens'], u['token_source']) == (180, 25, 'mixed')
    assert u['models']['gpt-4o'] == {**u['models']['gpt-4o'], 'input_tokens': 180, 'output_tokens': 25}


def test_remainder_without_any_model_cannot_be_priced(usage):
    env = _envelope([], 90, 9, token_source='estimate')
    u = usage.envelope_usage(env)
    assert u['usage_state'] == 'recorded'
    assert u['models'] == {None: {**u['models'][None], 'input_tokens': 90, 'output_tokens': 9}}
    assert usage.price_usage(u, lambda **kw: {'cost': 1.0})['cost_source'] == 'unpriced'


@pytest.mark.parametrize('result, status, reason', [
    (_envelope([], 0, 0, token_source='none'), None, 'no_callback'),
    ({'result': {'chat_history': [{'role': 'assistant', 'content': 'a'}]}}, None, 'no_envelope'),
    (None, None, 'no_envelope'),
    ({'task_id': 't1'}, 'timeout', 'timeout'),
    (None, 'predict_exception', 'no_envelope'),
    (None, 'budget_blocked', 'budget_blocked'),
])
def test_missing_usage_is_not_recorded_never_zero(usage, result, status, reason):
    u = usage.envelope_usage(result, status=status)
    assert (u['usage_state'], u['usage_state_reason']) == ('not_recorded', reason)
    assert usage.price_usage(u, lambda **kw: {'cost': 1.0}) == {'cost': None, 'cost_source': 'pending'}
    assert usage.run_tokens(u) == 0


def test_capture_does_not_silently_regress_to_zero(usage):
    """Guard: if step parsing broke, a provider envelope must not read as a recorded zero."""
    u = usage.envelope_usage(_envelope([_step('gpt-4o', 1200, 80)], 1200, 80))
    assert u['usage_state'] == 'recorded'
    assert usage.run_tokens(u) == 1280
    assert u['models']['gpt-4o']['input_tokens'] == 1200


# --- merge (judge groups of one case) ---------------------------------------------------------------

def test_merge_sums_recorded_calls_and_flags_partial(usage):
    a = usage.envelope_usage(_envelope([_step('haiku', 100, 10)], 100, 10))
    b = usage.envelope_usage(_envelope([_step('haiku', 50, 5)], 50, 5, token_source='estimate'))
    missing = usage.envelope_usage({'task_id': 't'}, status='timeout')
    merged = usage.merge_usage([a, b, missing])
    assert (merged['input_tokens'], merged['output_tokens']) == (150, 15)
    assert (merged['usage_state_reason'], merged['token_source']) == ('partial', 'mixed')
    assert merged['models']['haiku']['input_tokens'] == 150
    assert usage.merge_usage([a, b])['usage_state_reason'] is None


def test_merge_of_nothing_is_none_and_of_misses_is_not_recorded(usage):
    assert usage.merge_usage([]) is None
    missing = usage.envelope_usage(None)
    assert usage.merge_usage([missing])['usage_state'] == 'not_recorded'


# --- pricing ------------------------------------------------------------------------------------

def test_pricing_bills_cache_tokens_once(usage):
    seen = []

    def pricer(**kw):
        seen.append(kw)
        return {'cost': 0.25, 'cost_source': 'catalog'}

    u = usage.envelope_usage(_envelope([_step('gpt-4o', 1000, 50, cache_read=600, cache_creation=100)], 1000, 50))
    priced = usage.price_usage(u, pricer)
    assert priced == {'cost': Decimal('0.25'), 'cost_source': 'runtime:costs-catalog'}
    assert seen == [{'model_name': 'gpt-4o', 'input_tokens': 300, 'output_tokens': 50,
                     'cache_read_input_tokens': 600, 'cache_creation_input_tokens': 100}]


def test_one_unpriced_model_makes_the_whole_case_unpriced(usage):
    u = usage.envelope_usage(_envelope([_step('gpt-4o', 10, 1), _step('mystery', 10, 1)], 20, 2))
    pricer = lambda model_name, **kw: {'cost': None if model_name == 'mystery' else 0.1}  # noqa: E731
    assert usage.price_usage(u, pricer) == {'cost': None, 'cost_source': 'unpriced'}


def test_a_raising_pricer_leaves_the_cost_pending(usage):
    def pricer(**kw):
        raise TimeoutError('rpc')
    u = usage.envelope_usage(_envelope([_step('gpt-4o', 10, 1)], 10, 1))
    assert usage.price_usage(u, pricer) == {'cost': None, 'cost_source': 'pending'}
    assert usage.price_usage(u, None)['cost_source'] == 'pending'


# --- rollup ---------------------------------------------------------------------------------------

def _row(usage, *, tokens=(100, 10), cost=None, state='recorded', status='ok', cost_source=None):
    u = {'usage_state': state, 'usage_state_reason': None, 'token_source': 'provider',
         'model_name': 'gpt-4o', 'input_tokens': tokens[0], 'output_tokens': tokens[1]}
    priced = {'cost': None if cost is None else Decimal(str(cost)),
              'cost_source': cost_source or ('runtime:costs-catalog' if cost is not None else 'unpriced')}
    return usage.usage_row(dataset_case_id=1, case_index=0, role='agent', usage=u, priced=priced,
                           case_status=status)


def test_rollup_averages_recorded_cases_and_counts_the_rest(usage):
    rows = [_row(usage, tokens=(100, 10), cost=0.5), _row(usage, tokens=(300, 30), cost=1.5),
            _row(usage, tokens=(50, 5)),
            _row(usage, state='not_recorded', tokens=(0, 0)),
            _row(usage, state='not_recorded', tokens=(0, 0), status='budget_blocked'),
            _row(usage, state='not_applicable', tokens=(0, 0), status='unsupported')]
    r = usage.usage_rollup(rows)
    assert (r['cases'], r['recorded_cases']) == (6, 3)
    assert r['totals']['input_tokens'] == 450
    assert r['averages']['input_tokens'] == 150
    assert (r['cost'], r['average_cost'], r['unpriced_cases']) == (2.0, 1.0, 1)
    assert r['excluded_cases'] == {'count': 3, 'budget_blocked': 1, 'not_applicable': 1, 'not_recorded': 1}
    assert r['cost_source'] == 'mixed'


def test_rollup_with_nothing_priced_has_no_cost(usage):
    r = usage.usage_rollup([_row(usage)])
    assert (r['cost'], r['average_cost']) == (None, None)


# --- budget verdict ------------------------------------------------------------------------------

def test_no_limits_no_verdict(usage):
    assert usage.budget_verdict([_row(usage)], None) is None
    assert usage.budget_verdict([_row(usage)], {'per_case': {'tokens': None}, 'per_run': None}) is None


def test_per_run_tokens_pass_and_breach(usage):
    rows = [_row(usage, tokens=(100, 10)), _row(usage, tokens=(100, 10))]
    assert usage.budget_verdict(rows, {'per_run': {'tokens': 220}})['verdict'] == 'pass'
    v = usage.budget_verdict(rows, {'per_run': {'tokens': 219}})
    assert v['verdict'] == 'breached'
    assert v['per_run']['tokens'] == {'limit': 219, 'value': 220, 'verdict': 'breached'}


def test_a_missing_figure_is_unknown_not_pass(usage):
    rows = [_row(usage, tokens=(10, 1), cost=0.1), _row(usage, tokens=(10, 1))]  # second unpriced
    v = usage.budget_verdict(rows, {'per_run': {'cost': 1.0}})
    assert v['verdict'] == 'unknown'
    # ...but a known part already over the limit is a breach whatever the rest costs.
    assert usage.budget_verdict(rows, {'per_run': {'cost': 0.05}})['verdict'] == 'breached'


def test_per_case_counts_breached_and_unknown_cases(usage):
    rows = [_row(usage, tokens=(100, 10)), _row(usage, tokens=(10, 1)),
            _row(usage, state='not_recorded', tokens=(0, 0))]
    v = usage.budget_verdict(rows, {'per_case': {'tokens': 50}})
    assert v['per_case']['tokens'] == {'limit': 50, 'cases': 3, 'breached_cases': 1,
                                       'unknown_cases': 1, 'verdict': 'breached'}


def test_blocked_and_not_applicable_cases_are_left_out(usage):
    rows = [_row(usage, tokens=(10, 1)),
            _row(usage, state='not_recorded', tokens=(0, 0), status='budget_blocked'),
            _row(usage, state='not_applicable', tokens=(0, 0), status='unsupported')]
    v = usage.budget_verdict(rows, {'per_case': {'tokens': 50}})
    assert v['per_case']['tokens']['cases'] == 1
    assert v['verdict'] == 'pass'


def test_cost_checks_say_where_the_cost_came_from(usage):
    rows = [_row(usage, cost=0.1)]
    v = usage.budget_verdict(rows, {'per_case': {'cost': 1}, 'per_run': {'tokens': 1000}})
    assert v['per_case']['cost']['cost_source'] == 'runtime:costs-catalog'
    assert 'cost_source' not in v['per_run']['tokens']


# --- run_agent: the budget gate ------------------------------------------------------------------

class _BudgetDoorClosedError(Exception):
    type = 'budget_exceeded'

    def __init__(self, scope):
        super().__init__(f'{scope} budget exhausted')
        self.scope = scope


def test_gate_refusal_is_budget_blocked_with_its_scope(runner):
    def predict(**kw):
        raise _BudgetDoorClosedError('member')

    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert (out['status'], out['budget_scope']) == ('budget_blocked', 'member')
    assert out['execution']['metrics']['budget_scope'] == 'member'
    assert (out['usage']['usage_state'], out['usage']['usage_state_reason']) == ('not_recorded', 'budget_blocked')


def test_other_exceptions_stay_predict_exception(runner):
    def predict(**kw):
        raise RuntimeError('boom')
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=predict)
    assert out['status'] == 'predict_exception'
    assert 'budget_scope' not in out


def test_run_agent_carries_envelope_usage(runner):
    env = _envelope([_step('gpt-4o', 100, 10)], 100, 10)
    out = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: env)
    assert out['status'] == 'ok'
    assert (out['usage']['usage_state'], out['usage']['input_tokens']) == ('recorded', 100)


# --- AI judge usage sink ---------------------------------------------------------------------------

DIMS = [{'id': 5, 'name': 'tone', 'description': 'Tone', 'scale_type': 'continuous',
         'scale_min': 0, 'scale_max': 10}]


def test_judge_usage_reaches_the_sink_even_when_unparseable(judge):
    env = _envelope([_step('haiku', 400, 40)], 400, 40)
    stub = lambda *a, **kw: {'status': 'unparseable', 'data': None, 'error': 'x', 'raw': env}  # noqa: E731
    sink = []
    judge.evaluate_case(1, {'model_name': 'haiku'}, {'input': 'q', 'output': 'a'}, DIMS,
                        judge=stub, usage_sink=sink.append)
    assert [(u['usage_state'], u['input_tokens']) for u in sink] == [('recorded', 400)]


def test_judge_timeout_is_not_recorded(judge):
    stub = lambda *a, **kw: {'status': 'timeout', 'data': None, 'error': 't', 'raw': {'task_id': 't'}}  # noqa: E731
    sink = []
    judge.evaluate_case(1, {}, {'input': 'q', 'output': 'a'}, DIMS, judge=stub, usage_sink=sink.append)
    assert (sink[0]['usage_state'], sink[0]['usage_state_reason']) == ('not_recorded', 'timeout')


def test_judge_without_sink_is_unchanged(judge):
    stub = lambda *a, **kw: {'status': 'timeout', 'data': None, 'error': 't', 'raw': None}  # noqa: E731
    rows = judge.evaluate_case(1, {}, {'input': 'q', 'output': 'a'}, DIMS, judge=stub)
    assert [r['status'] for r in rows] == ['error']


# --- orchestration: stops and usage rows ---------------------------------------------------------

def _snapshot(orch, n, *, budget=None, engine='code'):
    return orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S', 'consumption_budget': budget},
        dimensions=[{'id': 5, 'name': 'tone', 'scale_type': 'continuous', 'scale_min': 0, 'scale_max': 10}],
        bindings=[{'engine': engine, 'dimension_id': 5, 'weight': 1.0}],
        cases=[{'id': 100 + i, 'input': f'q{i}', 'output': None} for i in range(n)],
        application_id=10, application_version_id=99,
    )


def _agent(usage, tokens=(100, 10), status='ok', scope=None):
    def run(case):
        if status == 'budget_blocked':
            u = usage.envelope_usage(None, status='budget_blocked')
        else:
            u = usage.envelope_usage(_envelope([_step('gpt-4o', *tokens)], *tokens))
        out = {'status': status, 'output': 'a' if status == 'ok' else None, 'error': None,
               'structure': None, 'usage': u, 'execution': {'trajectory_state': 'recorded', 'metrics': {}}}
        if scope:
            out['budget_scope'] = scope
        return out
    return run


_CODE = lambda binding, evidence: {'score': 1.0, 'passed': True}  # noqa: E731


def test_token_budget_stops_the_run(orch, usage):
    snap = _snapshot(orch, 5, budget={'per_run': {'tokens': 250}})
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE)
    # 110 + 110 = 220 < 250 → third case runs, 330 ≥ 250 → stop.
    assert (out['stop_reason'], out['stop_scope'], out['cancelled']) == ('budget_exhausted', None, True)
    assert out['progress'] == {'done': 3, 'total': 5}


def test_token_budget_reached_on_the_last_case_is_not_a_stop(orch, usage):
    """Nothing was skipped, so the run is complete; the breach stays in the verdict."""
    snap = _snapshot(orch, 2, budget={'per_run': {'tokens': 200}})
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE)
    # 110 < 200 → second case runs, 220 ≥ 200 on the last case.
    assert (out['stop_reason'], out['cancelled']) == (None, False)
    assert out['progress'] == {'done': 2, 'total': 2}
    rows, _ = orch.split_case_usage(out['cases'])
    meta = orch.usage_meta(rows, snap['suite']['consumption_budget'])
    assert meta['budget_verdict']['per_run']['tokens']['verdict'] == 'breached'


def test_closed_gate_on_the_last_case_still_stops(orch, usage):
    snap = _snapshot(orch, 1)
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage, status='budget_blocked', scope='member'),
                               code_scorer=_CODE)
    assert (out['stop_reason'], out['stop_scope']) == ('gate_closed', 'member')


def test_closed_gate_stops_the_run_with_its_scope(orch, usage):
    snap = _snapshot(orch, 4)
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage, status='budget_blocked', scope='project'),
                               code_scorer=_CODE)
    assert (out['stop_reason'], out['stop_scope']) == ('gate_closed', 'project')
    assert out['progress']['done'] == 1


def test_per_case_breach_does_not_stop_the_run(orch, usage):
    """Only per_run.tokens stops early; a per-case breach is reported in the verdict."""
    snap = _snapshot(orch, 3, budget={'per_case': {'tokens': 50}})
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE)
    assert out['stop_reason'] is None
    rows, cases = orch.split_case_usage(out['cases'])
    meta = orch.usage_meta(rows, snap['suite']['consumption_budget'])
    assert meta['budget_verdict']['per_case']['tokens']['breached_cases'] == 3
    assert all('_usage' not in c for c in cases)


def test_token_budget_stops_the_concurrent_path_too(orch, usage):
    snap = _snapshot(orch, 10, budget={'per_run': {'tokens': 200}})
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE, case_concurrency=2)
    assert out['stop_reason'] == 'budget_exhausted'
    assert out['progress']['done'] < 10


def test_split_case_usage_prices_each_role(orch, usage):
    snap = _snapshot(orch, 2)
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE)
    rows, _ = orch.split_case_usage(out['cases'], lambda **kw: {'cost': 0.01})
    assert [(r['case_index'], r['role'], r['dataset_case_id']) for r in rows] == \
        [(0, 'agent', 100), (1, 'agent', 101)]
    assert all(r['cost'] == Decimal('0.01') and r['cost_source'] == 'runtime:costs-catalog' for r in rows)
    meta = orch.usage_meta(rows, None)
    assert set(meta) == {'agent_usage'}
    assert meta['agent_usage']['totals']['input_tokens'] == 200


def test_cases_never_run_have_no_usage_row(orch, usage):
    snap = _snapshot(orch, 4, budget={'per_run': {'tokens': 100}})
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), code_scorer=_CODE)
    rows, _ = orch.split_case_usage(out['cases'])
    assert [r['case_index'] for r in rows] == [0]


def test_judge_usage_is_collected_per_case(orch, usage):
    calls = []

    def scorer(evidence, dims, usage_sink=None):
        calls.append(usage_sink)
        usage_sink(usage.envelope_usage(_envelope([_step('haiku', 30, 3)], 30, 3)))
        return [{'dimension_id': d['dimension_id'], 'native_score': 5, 'rationale': 'r',
                 'status': 'scored', 'error': None} for d in dims]
    scorer.accepts_usage_sink = True

    snap = _snapshot(orch, 2, engine='ai')
    out = orch.orchestrate_run(snap, agent_runner=_agent(usage), ai_scorer=scorer)
    rows, _ = orch.split_case_usage(out['cases'])
    judge_rows = [r for r in rows if r['role'] == 'judge']
    assert [(r['case_index'], r['input_tokens']) for r in judge_rows] == [(0, 30), (1, 30)]
    assert all(c is not None for c in calls)


def test_a_scorer_without_the_marker_gets_no_sink(orch, usage):
    seen = []

    def scorer(evidence, dims):
        seen.append(True)
        return []

    out = orch.orchestrate_run(_snapshot(orch, 1, engine='ai'), agent_runner=_agent(usage), ai_scorer=scorer)
    rows, _ = orch.split_case_usage(out['cases'])
    assert seen and [r['role'] for r in rows] == ['agent']


def test_snapshot_freezes_the_consumption_budget(orch):
    budget = {'per_run': {'tokens': 1000}}
    assert _snapshot(orch, 1, budget=budget)['suite']['consumption_budget'] == budget
    assert _snapshot(orch, 1)['suite']['consumption_budget'] is None
