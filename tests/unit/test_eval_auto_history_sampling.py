"""Unit tests for ``utils/eval_auto_history.py``: which stored turns history mode scores, the Not
Evaluated reasons it reads from them, and the sample. Both modules are stdlib-only and loaded by path."""
import importlib.util
import pathlib
from datetime import datetime, timedelta

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name):
    spec = importlib.util.spec_from_file_location(f'{name}_under_test', PLUGIN_ROOT / 'utils' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def history():
    return _load('eval_auto_history')


@pytest.fixture(scope='module')
def rules():
    return _load('eval_auto_rules')


@pytest.fixture
def settings(rules):
    return rules.effective_config({})


BASE = datetime(2026, 11, 20, 12, 0)


def turn(id_, conversation=1, user=10, *, text='How do I reset my password?', output='Open Settings.',
         agent_metas=None, user_meta=None, streaming=False, minutes=0):
    return {
        'id': id_, 'conversation_id': conversation, 'user_id': user, 'is_private': False, 'version_id': 5,
        'created_at': BASE + timedelta(minutes=minutes or id_), 'input': text, 'output': output,
        'group_ids': [id_ + 1000], 'user_meta': user_meta or {},
        'agent_metas': [{}] if agent_metas is None else agent_metas, 'streaming': streaming,
    }


def test_reason_codes_match_rules(history, rules):
    for code in ('HITL_REQUIRED', 'OAUTH_REQUIRED', 'AGENT_BROKEN', 'NO_TRAFFIC'):
        assert getattr(history, code) == getattr(rules, code)
        assert code in rules.REASON_CODES
    assert history.STRATEGY_STRATIFIED == rules.HISTORY_STRATEGY_STRATIFIED
    assert history.STRATEGY_RECENT == rules.HISTORY_STRATEGY_RECENT


# --- conversations and turns ------------------------------------------------

@pytest.mark.parametrize('conversation, excluded', [
    ({'source': 'elitea', 'meta': {}}, False),
    ({'source': 'elitea', 'meta': None}, False),
    ({'source': 'evaluation', 'meta': {}}, True),
    ({'source': 'toolkit', 'meta': {}}, True),
    ({'source': 'elitea', 'meta': {'evaluation_run_id': 7}}, True),
])
def test_evaluation_conversations_are_never_sampled(history, conversation, excluded):
    assert history.is_evaluation_conversation(conversation) is excluded


def test_plain_turn_is_ok(history):
    assert history.turn_outcome(turn(1)) == history.OK


@pytest.mark.parametrize('kwargs', [
    {'text': '[Scheduled execution triggered]'},
    {'text': '   '},
    {'text': None},
    {'streaming': True},
])
def test_not_a_use_of_the_agent_is_skipped(history, kwargs):
    assert history.turn_outcome(turn(1, **kwargs)) == history.SKIP


@pytest.mark.parametrize('kwargs', [
    {'agent_metas': []},
    {'agent_metas': [{'is_error': True}]},
    {'output': ''},
    {'output': None},
])
def test_failed_or_empty_answer_is_error(history, kwargs):
    assert history.turn_outcome(turn(1, **kwargs)) == history.ERROR


def test_only_a_guardrail_block_excludes(history):
    block = {'input_guardrail': {'action': 'block', 'rules': ['pii']}}
    redact = {'input_guardrail': {'action': 'redact', 'rules': ['pii']}}
    assert history.turn_outcome(turn(1, user_meta=block)) == history.GUARDRAIL
    assert history.turn_outcome(turn(1, agent_metas=[block])) == history.GUARDRAIL
    assert history.turn_outcome(turn(1, user_meta=redact)) == history.OK


def test_hitl_pending_versus_resolved(history):
    single = {'hitl_interrupt': {'type': 'hitl', 'interrupt_id': 'i1'}}
    assert history.turn_outcome(turn(1, agent_metas=[single])) == history.HITL_OPEN
    resolved = {**single, 'resolved_hitl_interrupt_ids': ['i1']}
    assert history.turn_outcome(turn(1, agent_metas=[resolved])) == history.OK
    # The identity falls back to thread:tool_call when there is no interrupt id.
    plural = {'hitl_interrupts': [{'thread_id': 't', 'tool_call_id': 'c'}, {'interrupt_id': 'i2'}],
              'resolved_hitl_interrupt_ids': ['t:c']}
    assert len(history.pending_hitl(plural)) == 1
    assert history.pending_hitl({**plural, 'resolved_hitl_interrupt_ids': ['t:c', 'i2']}) == []


def test_oauth_pending_versus_resolved(history):
    meta = {'authorization_requests': [{'tool_call_id': 'c1'}, {'tool_run_id': 42}]}
    assert len(history.pending_oauth(meta)) == 2
    assert history.turn_outcome(turn(1, agent_metas=[meta])) == history.OAUTH_OPEN
    resolved = {**meta, 'resolved_authorization_request_ids': ['c1', 42]}
    assert history.pending_oauth(resolved) == []
    assert history.turn_outcome(turn(1, agent_metas=[resolved])) == history.OK


def test_pending_helpers_tolerate_junk(history):
    for meta in (None, 'x', {}, {'hitl_interrupts': ['x']}, {'authorization_requests': [None]}):
        assert history.pending_hitl(meta) == []
        assert history.pending_oauth(meta) == []


# --- observed classifier ----------------------------------------------------

def test_observed_reason_at_threshold(history):
    assert history.observed_reason({'ok': 2, 'hitl_open': 2}, 0.5) == (
        'HITL_REQUIRED', '2 of 4 recent turns waited for a person who never answered')
    assert history.observed_reason({'ok': 3, 'hitl_open': 2}, 0.5) is None
    assert history.observed_reason({'ok': 1, 'error': 3}, 0.5)[0] == 'AGENT_BROKEN'
    assert history.observed_reason({'oauth_open': 1}, 0.5)[0] == 'OAUTH_REQUIRED'


def test_observed_reason_ignores_blocked_and_skipped(history):
    assert history.observed_reason({'guardrail': 9, 'skip': 9}, 0.5) is None
    assert history.observed_reason({'ok': 2, 'error': 1, 'guardrail': 9}, 0.5) is None


# --- planning and sampling --------------------------------------------------

def test_no_traffic_below_min_turns(history, settings):
    plan = history.plan_history([turn(1), turn(2, text='[Scheduled execution triggered]')], settings, 's')
    assert plan['reason'] == ('NO_TRAFFIC', '1 turns in 28 days, minimum 3')
    assert plan['sample'] == []
    assert plan['meta']['eligible'] == 1


def test_broken_agent_wins_over_no_traffic(history, settings):
    turns = [turn(i, agent_metas=[{'is_error': True}]) for i in range(1, 4)] + [turn(9)]
    plan = history.plan_history(turns, settings, 's')
    assert plan['reason'][0] == 'AGENT_BROKEN'
    assert plan['meta']['excluded']['error'] == 3


def test_plan_counts_and_meta(history, settings):
    turns = [turn(i, conversation=i, user=i) for i in range(1, 6)]
    turns.append(turn(50, user_meta={'input_guardrail': {'action': 'block'}}))
    turns.append(turn(51, agent_metas=[{'is_error': True}]))
    plan = history.plan_history(turns, settings, 's')
    assert plan['reason'] is None
    meta = plan['meta']
    assert meta['eligible'] == 5 and meta['sampled'] == 5
    assert meta['conversations'] == 5 and meta['users'] == 5
    assert meta['excluded'] == {'error': 1, 'hitl_open': 0, 'oauth_open': 0, 'guardrail': 1}
    assert meta['window_days'] == 28 and meta['strategy'] == 'stratified'
    assert meta['reference_free'] is True and meta['version_attribution'] == 'conversation'


def test_sample_is_oldest_first_and_capped(history, settings):
    turns = [turn(i, conversation=i, user=i) for i in range(1, 40)]
    sample = history.select_turns(turns, settings, 'job:1')
    assert len(sample) == settings['history_max_turns']
    times = [t['created_at'] for t in sample]
    assert times == sorted(times)


def test_stratified_is_stable_per_salt(history, settings):
    turns = [turn(i, conversation=i, user=i) for i in range(1, 40)]
    first = [t['id'] for t in history.select_turns(turns, settings, 'job:1')]
    assert first == [t['id'] for t in history.select_turns(list(reversed(turns)), settings, 'job:1')]
    assert first != [t['id'] for t in history.select_turns(turns, settings, 'job:2')]


def test_stratified_caps_conversation_and_user(history, settings):
    # 20 turns in one long chat by one heavy user, plus 10 single-turn chats by others.
    turns = [turn(i, conversation=1, user=1) for i in range(1, 21)]
    turns += [turn(100 + i, conversation=100 + i, user=100 + i) for i in range(10)]
    sample = history.select_turns(turns, settings, 's')
    assert len(sample) == 10
    assert sum(1 for t in sample if t['conversation_id'] == 1) <= settings['history_max_turns_per_conversation']


def test_stratified_fills_to_min_turns_from_held_back(history, settings):
    # One user, one chat: the caps alone allow 2 turns, the floor is 3.
    turns = [turn(i) for i in range(1, 8)]
    sample = history.select_turns(turns, settings, 's')
    assert len(sample) == settings['history_min_turns']


def test_recent_takes_newest(history, settings):
    cfg = {**settings, 'history_strategy': 'recent', 'history_max_turns': 3}
    turns = [turn(i) for i in range(1, 8)]
    assert [t['id'] for t in history.select_turns(turns, cfg, 's')] == [5, 6, 7]


def test_latency_ms(history):
    assert history.latency_ms([{'execution_time_seconds': 1.25}, {'execution_time_seconds': 0.5}]) == 1750
    assert history.latency_ms([{}, {'execution_time_seconds': True}]) is None
    assert history.latency_ms(None) is None


def _conversations(**overrides):
    conversations = {1: {'is_private': True, 'version_id': 5, 'source': 'elitea', 'meta': {}},
                     2: {'is_private': False, 'version_id': 5, 'source': 'evaluation', 'meta': {}}}
    conversations.update(overrides)
    return conversations


def test_assemble_turns_joins_replies_in_order(history):
    users = [{'id': 1, 'conversation_id': 1, 'user_id': 10, 'created_at': BASE, 'meta': None, 'is_streaming': False}]
    agents = [{'id': 3, 'reply_to_id': 1, 'meta': {'execution_time_seconds': 2}, 'is_streaming': True},
              {'id': 2, 'reply_to_id': 1, 'meta': {'execution_time_seconds': 1}, 'is_streaming': False},
              {'id': 9, 'reply_to_id': 77, 'meta': {}, 'is_streaming': False}]
    texts = {1: 'Question', 2: 'First part', 3: 'Second part'}
    [assembled] = history.assemble_turns(users, agents, texts, _conversations())
    assert assembled['input'] == 'Question'
    assert assembled['output'] == 'First part\n\nSecond part'
    assert assembled['group_ids'] == [2, 3]
    assert assembled['is_private'] is True and assembled['version_id'] == 5
    assert assembled['streaming'] is True and assembled['latency_ms'] == 3000
    assert assembled['user_meta'] == {}


def test_assemble_turns_drops_evaluation_and_unknown_conversations(history):
    users = [{'id': 1, 'conversation_id': 2, 'user_id': 10, 'created_at': BASE},
             {'id': 4, 'conversation_id': 99, 'user_id': 10, 'created_at': BASE}]
    assert history.assemble_turns(users, [], {}, _conversations()) == []


def test_assemble_turns_without_reply(history):
    users = [{'id': 1, 'conversation_id': 1, 'user_id': 10, 'created_at': BASE}]
    [assembled] = history.assemble_turns(users, [], {}, _conversations())
    assert assembled['output'] is None and assembled['group_ids'] == [] and assembled['latency_ms'] is None
    assert assembled['input'] == ''
    assert history.turn_outcome(assembled) != history.OK


def test_history_settings_defaults_and_bounds(rules):
    cfg = rules.effective_config({})
    assert cfg['mode'] == rules.MODE_LIVE
    assert cfg['history_reevaluate_after_days'] == 7 and cfg['max_concurrent_history_runs'] == 4
    cfg = rules.effective_config({'mode': 'nonsense', 'history_window_days': 500, 'history_max_turns': 5,
                                  'history_min_turns': 20, 'history_version': 'any'})
    assert cfg['mode'] == rules.MODE_LIVE
    assert cfg['history_window_days'] == 90
    assert cfg['history_min_turns'] == 5
    assert cfg['history_version'] == rules.HISTORY_VERSION_ANY
    assert rules.effective_config({'mode': 'hybrid'})['mode'] == rules.MODE_HYBRID
