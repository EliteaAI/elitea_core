"""#6898: context window budget, per-chat usage & cost, and the review fixes on elitea_core#556.

Run via:
    python tests/run_tests.py unit/utils/test_6898_context_window_usage.py -v
"""
import pathlib
import sys
import types
from decimal import Decimal

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def cw(utils_path):
    return load_utils_module(utils_path, 'context_window')


@pytest.fixture(scope='module')
def usage(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    return load_utils_module(utils_path, 'chat_usage')


# --- resolve_context_budget ---------------------------------------------------------------

def test_balanced_window_at_or_below_272k_is_the_model_window(cw):
    budget = cw.resolve_context_budget('balanced', 200_000)
    assert budget['effective_window'] == 200_000
    assert budget['safety_margin'] == 2_000
    assert budget['available_window'] == 198_000
    assert budget['reserved_output'] == cw.AUTO_OUTPUT_RESERVE
    assert budget['usable_input'] == 198_000 - cw.AUTO_OUTPUT_RESERVE
    assert budget['compaction_threshold'] == int((198_000 - cw.AUTO_OUTPUT_RESERVE) * 0.9)

    exact = cw.resolve_context_budget('balanced', 272_000)
    assert exact['effective_window'] == 272_000


def test_balanced_caps_large_windows_and_full_does_not(cw):
    assert cw.resolve_context_budget('balanced', 1_000_000)['effective_window'] == 272_000
    assert cw.resolve_context_budget('full', 1_000_000)['effective_window'] == 1_000_000


def test_unknown_window_and_mode_fall_back(cw):
    budget = cw.resolve_context_budget('bogus', None)
    assert budget['window_mode'] == 'balanced'
    assert budget['model_window'] == cw.DEFAULT_MODEL_WINDOW
    assert budget['window_source'] == 'default'


def test_custom_max_tokens_is_clamped_to_model_max_output(cw):
    budget = cw.resolve_context_budget('full', 200_000, model_max_output=8_000, llm_max_tokens=50_000)
    assert budget['reserved_output'] == 8_000
    assert budget['reserved_output_source'] == 'custom'
    assert cw.resolve_context_budget('full', 200_000, None, 4_000)['reserved_output'] == 4_000


def test_tiny_window_clamps_usable_input(cw):
    budget = cw.resolve_context_budget('full', 2_500, model_max_output=2_000, llm_max_tokens=2_000)
    assert budget['usable_input'] == cw.MIN_USABLE_INPUT


# --- turn_usage_by_model / merge_usage_totals ------------------------------------------------

def test_rollup_plus_auxiliary_summary_usage(usage):
    delta = usage.turn_usage_by_model({
        'usage_by_model': {'gpt-x': {'input_tokens': 100, 'output_tokens': 10, 'cache_read_tokens': 20}},
        'context_info': {'auxiliary_usage': [
            {'model_name': 'gpt-mini', 'input_tokens': 50, 'output_tokens': 5},
            {'model_name': 'gpt-x', 'input_tokens': 1, 'output_tokens': 1},
        ]},
    })
    assert delta['gpt-x'] == {'input_tokens': 101, 'output_tokens': 11,
                              'cache_read_tokens': 20, 'cache_creation_tokens': 0}
    assert delta['gpt-mini']['input_tokens'] == 50


def test_old_indexer_totals_go_to_unknown(usage):
    delta = usage.turn_usage_by_model({'chat_history_tokens_input': 30, 'llm_response_tokens_output': 7})
    assert delta == {'unknown': {'input_tokens': 30, 'output_tokens': 7,
                                 'cache_read_tokens': 0, 'cache_creation_tokens': 0}}


def test_unpriced_models_are_listed_and_never_zero(usage):
    delta = {'m': {'input_tokens': 5, 'output_tokens': 1, 'cache_read_tokens': 0, 'cache_creation_tokens': 0}}
    totals = usage.merge_usage_totals(None, delta, {'m': None})
    assert totals['cost'] is None
    assert totals['cost_source'] == usage.COST_UNPRICED
    assert totals['unpriced_models'] == ['m']

    priced = usage.merge_usage_totals(totals, {'p': dict(delta['m'])}, {'p': Decimal('0.5')})
    assert priced['cost'] == '0.5'
    assert priced['cost_source'] == usage.COST_RUNTIME
    assert priced['unpriced_models'] == ['m']
    assert priced['input_tokens'] == 10


# --- price cache --------------------------------------------------------------------------

def test_failing_pricer_is_called_once_within_ttl(usage):
    calls = []

    def failing(**kwargs):
        calls.append(kwargs)
        raise TimeoutError('costs not registered')

    clock = [0.0]
    cache = usage.UnitPriceCache(failing, ttl=600, clock=lambda: clock[0])
    delta = {m: {'input_tokens': 10, 'output_tokens': 1, 'cache_read_tokens': 0, 'cache_creation_tokens': 0}
             for m in ('a', 'b')}
    for _ in range(3):
        assert usage.price_models(delta, cache) == {'a': None, 'b': None}
    assert len(calls) == 1
    clock[0] = 601
    usage.price_models({'a': delta['a']}, cache)
    assert len(calls) == 2


def test_unit_prices_are_cached_and_computed_locally(usage):
    rates = {'input': Decimal('0.000002'), 'output': Decimal('0.00001'),
             'cache_read': Decimal('0.0000002'), 'cache_creation': Decimal('0.0000025')}
    calls = []

    def rpc(model_name, input_tokens, output_tokens, cache_read_input_tokens=0, cache_creation_input_tokens=0):
        calls.append(model_name)
        if model_name == 'unpriced':
            return {'cost': None}
        return {'cost': input_tokens * rates['input'] + output_tokens * rates['output']
                + cache_read_input_tokens * rates['cache_read']
                + cache_creation_input_tokens * rates['cache_creation']}

    cache = usage.UnitPriceCache(rpc, clock=lambda: 0.0)
    bucket = {'input_tokens': 1_300, 'output_tokens': 100, 'cache_read_tokens': 200, 'cache_creation_tokens': 100}
    expected = 1_000 * rates['input'] + 100 * rates['output'] + 200 * rates['cache_read'] + 100 * rates['cache_creation']
    for _ in range(2):
        prices = usage.price_models({'m': bucket, 'unpriced': bucket}, cache)
        assert prices['m'] == expected
        assert prices['unpriced'] is None
    assert calls.count('m') == 4
    assert calls.count('unpriced') == 1


# --- dedup --------------------------------------------------------------------------------

class _Column:
    def __eq__(self, other):
        return True

    __hash__ = object.__hash__


class _Query:
    def __init__(self, conversation):
        self._conversation = conversation

    def filter(self, *_):
        return self

    def with_for_update(self, *_, **__):
        return self

    def populate_existing(self):
        return self

    def first(self):
        return self._conversation


class _Session:
    def __init__(self, conversation):
        self.conversation = conversation

    def query(self, *_):
        return _Query(self.conversation)

    def flush(self):
        pass


@pytest.fixture
def accumulate(usage, monkeypatch):
    conversation_cls = type('Conversation', (), {'id': _Column()})
    models_pkg = types.ModuleType('plugins.elitea_core.models')
    models_pkg.__path__ = []
    conversation_mod = types.ModuleType('plugins.elitea_core.models.conversation')
    conversation_mod.Conversation = conversation_cls
    attributes_mod = types.ModuleType('sqlalchemy.orm.attributes')
    attributes_mod.flag_modified = lambda *_: None
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models', models_pkg)
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models.conversation', conversation_mod)
    monkeypatch.setitem(sys.modules, 'sqlalchemy.orm.attributes', attributes_mod)
    monkeypatch.setattr(usage, 'costs_pricer', lambda: None)

    conversation = types.SimpleNamespace(meta={})
    group = types.SimpleNamespace(id=1, conversation_id=1, meta={})
    session = _Session(conversation)

    def run(response_meta):
        usage.accumulate_chat_usage(session, group, response_meta)
        return (conversation.meta.get('usage_totals') or {}).get('input_tokens', 0)
    return run


def _meta(run_id=None, tokens=100):
    meta = {'thread_id': 't1', 'usage_by_model': {'m': {'input_tokens': tokens, 'output_tokens': 1}}}
    if run_id:
        meta['usage_run_id'] = run_id
    return meta


def test_repeated_stream_end_with_same_run_id_is_not_counted(accumulate):
    assert accumulate(_meta('r1')) == 100
    assert accumulate(_meta('r1')) == 100


def test_new_run_id_is_counted(accumulate):
    accumulate(_meta('r1'))
    assert accumulate(_meta('r2')) == 200


def test_missing_run_id_with_identical_payload_is_not_counted(accumulate):
    assert accumulate(_meta()) == 100
    assert accumulate(_meta()) == 100
    assert accumulate(_meta(tokens=50)) == 150


# --- server-owned meta --------------------------------------------------------------------

def test_client_meta_cannot_forge_server_owned_keys(usage):
    stored = {'usage_totals': {'cost': '1.5'}, 'context_budget': {'usable_input': 10}, 'persona': 'old'}
    incoming = {'usage_totals': {'cost': '0'}, 'context_budget': {'x': 1},
                'context_analytics': {'summaries_generated': 99}, 'persona': 'new'}
    result = usage.strip_server_owned_meta(incoming, stored)
    assert result['usage_totals'] == {'cost': '1.5'}
    assert result['context_budget'] == {'usable_input': 10}
    assert 'context_analytics' not in result
    assert result['persona'] == 'new'
    assert usage.strip_server_owned_meta(None, stored) is None


# --- manual trigger 403 rule --------------------------------------------------------------

@pytest.mark.parametrize('fields, new, stored, allowed, admin, denied', [
    ({'manual_max_context_tokens'}, 5_000, {}, True, False, True),
    ({'manual_max_context_tokens'}, 5_000, {}, False, True, True),
    ({'manual_max_context_tokens'}, 5_000, {}, True, True, False),
    ({'manual_max_context_tokens'}, 5_000, {'manual_max_context_tokens': 5_000}, False, False, False),
    ({'manual_max_context_tokens'}, None, {}, False, False, False),
    ({'context_window_mode'}, None, {'manual_max_context_tokens': 5_000}, False, False, False),
])
def test_manual_change_denied(cw, fields, new, stored, allowed, admin, denied):
    checks = []

    def allowed_fn():
        checks.append('flag')
        return allowed

    assert cw.manual_change_denied(fields, new, stored, 7, allowed_fn, lambda _uid: admin) is denied
    if not denied and not (allowed and admin):
        assert checks == []


# --- legacy max_context_tokens (AC22) -----------------------------------------------------

def test_marked_legacy_trigger_is_honored(cw):
    strategy = {'max_context_tokens': 30_000, 'legacy_trigger': True, 'context_window_mode': 'balanced'}
    assert cw.legacy_context_tokens(strategy) == 30_000
    predict = cw.strategy_for_predict(strategy)
    assert predict['max_context_tokens'] == 30_000
    assert 'context_window_mode' not in predict
    assert 'legacy_trigger' not in predict

    budget = cw.resolve_context_budget('balanced', 30_000)
    assert cw.status_max_tokens(budget, legacy=30_000) == budget['usable_input']
    assert cw.status_max_tokens(budget, legacy=10_000) == 10_000


def test_unmarked_old_default_follows_balanced(cw):
    strategy = {'max_context_tokens': 64_000}
    assert cw.legacy_context_tokens(strategy) is None
    predict = cw.strategy_for_predict(strategy)
    assert predict['context_window_mode'] == 'balanced'
    assert 'max_context_tokens' not in predict
    budget = cw.resolve_context_budget('balanced', 128_000)
    assert cw.status_max_tokens(budget) == budget['available_window']


# --- chat model pick ----------------------------------------------------------------------

def test_user_participant_model_is_preferred(cw):
    participants = [
        ('application', {'llm_settings': {'model_name': 'agent-model'}}),
        ('dummy', {}),
        ('user', {'llm_settings': {'model_name': 'chat-model'}}),
    ]
    assert cw.pick_chat_llm_settings(participants)['model_name'] == 'chat-model'
    assert cw.pick_chat_llm_settings(participants[:2])['model_name'] == 'agent-model'
    assert cw.pick_chat_llm_settings([]) == {}


# --- TTL cache ----------------------------------------------------------------------------

def test_cached_loader_runs_once_and_does_not_cache_failures(cw):
    calls = []

    def load():
        calls.append(1)
        return 'full'
    key = ('test_6898', len(cw._value_cache))
    assert cw._cached(key, load, 'balanced') == 'full'
    assert cw._cached(key, load, 'balanced') == 'full'
    assert len(calls) == 1

    def boom():
        calls.append(1)
        raise RuntimeError('rpc down')
    fail_key = key + ('fail',)
    assert cw._cached(fail_key, boom, 'balanced') == 'balanced'
    assert cw._cached(fail_key, boom, 'balanced') == 'balanced'
    assert len(calls) == 3
