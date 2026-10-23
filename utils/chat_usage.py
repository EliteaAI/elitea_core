""" Cumulative per-chat token usage and approximate cost (#6898) """

import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Optional

from .evaluation_usage import COST_RUNTIME, COST_UNPRICED

USAGE_FIELDS = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_creation_tokens')
# Written by the server only; client-supplied conversation meta never sets them
SERVER_OWNED_META_KEYS = ('usage_totals', 'context_budget', 'context_analytics')
PRICE_TTL_SECONDS = 600
PRICE_RPC_TIMEOUT = 2
_PRICE_UNIT_TOKENS = 1_000_000
# The costs RPC prices nothing without input or output tokens, so cache units ride on 1M input
_PRICE_UNITS = (
    ('input_tokens', {'input_tokens': _PRICE_UNIT_TOKENS}),
    ('output_tokens', {'output_tokens': _PRICE_UNIT_TOKENS}),
    ('cache_read_tokens', {'input_tokens': _PRICE_UNIT_TOKENS, 'cache_read_input_tokens': _PRICE_UNIT_TOKENS}),
    ('cache_creation_tokens', {'input_tokens': _PRICE_UNIT_TOKENS, 'cache_creation_input_tokens': _PRICE_UNIT_TOKENS}),
)


def _count(value) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return 0
    return int(value)


def turn_usage_by_model(response_meta: dict) -> dict:
    """{model: {input_tokens, output_tokens, cache_*}} of one completed response.

    The indexer's per-model rollup covers every callback-traced call (agent turns, tool loops,
    sub-agents). The SDK invokes summary calls without run callbacks, so they are not in that
    rollup and the SDK reports them separately in context_info.auxiliary_usage.
    Older indexers send only the totals; they go to an unknown model and stay unpriced.
    """
    models: dict = {}

    def add(model, usage):
        bucket = models.setdefault(model or 'unknown', {f: 0 for f in USAGE_FIELDS})
        for field in USAGE_FIELDS:
            bucket[field] += _count((usage or {}).get(field))

    by_model = response_meta.get('usage_by_model')
    if isinstance(by_model, dict) and by_model:
        for model, usage in by_model.items():
            add(model, usage)
    else:
        add(None, {
            'input_tokens': response_meta.get('chat_history_tokens_input'),
            'output_tokens': response_meta.get('llm_response_tokens_output'),
        })
    for usage in (response_meta.get('context_info') or {}).get('auxiliary_usage') or []:
        if isinstance(usage, dict):
            add(usage.get('model_name'), usage)
    return {model: bucket for model, bucket in models.items() if any(bucket.values())}


def price_models(models: dict, pricer: Optional[Callable[..., dict]]) -> dict:
    """{model: cost Decimal | None}; None for unpriced models or a failing pricer."""
    prices = {}
    for model, bucket in models.items():
        if pricer is None or model == 'unknown':
            prices[model] = None
            continue
        billable = max(0, bucket['input_tokens'] - bucket['cache_read_tokens'] - bucket['cache_creation_tokens'])
        try:
            priced = pricer(
                model_name=model, input_tokens=billable, output_tokens=bucket['output_tokens'],
                cache_read_input_tokens=bucket['cache_read_tokens'],
                cache_creation_input_tokens=bucket['cache_creation_tokens'],
            ) or {}
        except Exception:  # pylint: disable=W0718 - pricing must never fail the response save
            priced = {}
        prices[model] = None if priced.get('cost') is None else Decimal(str(priced['cost']))
    return prices


class UnitPriceCache:
    """Per-model unit prices (USD per 1M tokens of each kind), so a turn is priced locally.

    Each model is priced through the RPC at most once per TTL; an unpriced model is cached too.
    A failing RPC (costs plugin missing or slow) disables pricing for the TTL, so it costs ~0 after.
    """

    def __init__(self, rpc: Callable[..., dict], ttl: float = PRICE_TTL_SECONDS, clock=time.monotonic):
        self._rpc = rpc
        self._ttl = ttl
        self._clock = clock
        self._lock = threading.Lock()
        self._units: dict = {}
        self._down_until = 0.0

    def _unit_prices(self, model: str) -> Optional[dict]:
        now = self._clock()
        with self._lock:
            if now < self._down_until:
                return None
            cached = self._units.get(model)
            if cached and now - cached[0] < self._ttl:
                return cached[1]
        units = {}
        try:
            for field, tokens in _PRICE_UNITS:
                call = {'input_tokens': 0, 'output_tokens': 0, **tokens}
                priced = self._rpc(model_name=model, **call) or {}
                if priced.get('cost') is None:
                    units = None
                    break
                units[field] = Decimal(str(priced['cost']))
                if field != 'input_tokens' and 'input_tokens' in tokens:
                    units[field] -= units['input_tokens']
        except Exception:  # pylint: disable=W0718 - an unavailable costs plugin means unpriced, never an error
            with self._lock:
                self._down_until = now + self._ttl
            return None
        with self._lock:
            self._units[model] = (now, units)
        return units

    def __call__(self, model_name, input_tokens=0, output_tokens=0,
                 cache_read_input_tokens=0, cache_creation_input_tokens=0, **_kwargs) -> dict:
        units = self._unit_prices(model_name)
        if units is None:
            return {'cost': None}
        counts = {
            'input_tokens': input_tokens, 'output_tokens': output_tokens,
            'cache_read_tokens': cache_read_input_tokens, 'cache_creation_tokens': cache_creation_input_tokens,
        }
        cost = sum((units[field] * (counts[field] or 0) for field in units), Decimal(0)) / _PRICE_UNIT_TOKENS
        return {'cost': cost}


def merge_usage_totals(totals: Optional[dict], delta: dict, prices: dict) -> dict:
    """Add one response's usage into ``conversation.meta.usage_totals``."""
    totals = dict(totals or {})
    totals.setdefault('tracking_started_at', datetime.now(tz=timezone.utc).isoformat())
    by_model = {k: dict(v) for k, v in (totals.get('by_model') or {}).items()}
    unpriced = set(totals.get('unpriced_models') or [])
    cost = Decimal(totals['cost']) if totals.get('cost') is not None else None

    for model, bucket in delta.items():
        entry = by_model.setdefault(model, {f: 0 for f in USAGE_FIELDS})
        for field in USAGE_FIELDS:
            entry[field] = entry.get(field, 0) + bucket[field]
            totals[field] = _count(totals.get(field)) + bucket[field]
        price = prices.get(model)
        if price is None:
            unpriced.add(model)
        else:
            entry['cost'] = str(Decimal(entry.get('cost') or 0) + price)
            cost = (cost or Decimal(0)) + price

    for field in USAGE_FIELDS:
        totals.setdefault(field, 0)
    totals['by_model'] = by_model
    totals['unpriced_models'] = sorted(unpriced)
    totals['cost'] = None if cost is None else str(cost)
    totals['cost_source'] = COST_UNPRICED if cost is None and unpriced else COST_RUNTIME
    return totals


def _costs_rpc(**kwargs):
    from tools import context as pylon_context  # pylint: disable=E0401,C0415
    return pylon_context.rpc_manager.timeout(PRICE_RPC_TIMEOUT).costs_compute_llm_cost(**kwargs)


_price_cache = UnitPriceCache(_costs_rpc)


def costs_pricer() -> Optional[Callable[..., dict]]:
    return _price_cache


def usage_fingerprint(response_meta: dict, delta: dict) -> str:
    """Stable key of one response's usage, for indexers that send no ``usage_run_id``."""
    payload = json.dumps(
        {'thread_id': response_meta.get('thread_id'), 'usage': delta}, sort_keys=True, default=str,
    )
    return 'fp:' + hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]


def usage_count_key(response_meta: dict, delta: dict) -> str:
    return response_meta.get('usage_run_id') or usage_fingerprint(response_meta, delta)


def strip_server_owned_meta(incoming_meta: Optional[dict], stored_meta: Optional[dict]) -> Optional[dict]:
    """Client-supplied meta with server-owned keys replaced by their stored values (dropped when unset)."""
    if not isinstance(incoming_meta, dict):
        return incoming_meta
    stored_meta = stored_meta or {}
    for key in SERVER_OWNED_META_KEYS:
        incoming_meta.pop(key, None)
        if key in stored_meta:
            incoming_meta[key] = stored_meta[key]
    return incoming_meta


def accumulate_chat_usage(session, msg_group, response_meta: dict) -> None:
    """Add a completed response's usage to its conversation's totals, once per indexer run.

    Priced before the conversation row is locked; the lock covers only the merge. The group
    remembers the runs it counted (``usage_run_id``, else a fingerprint of the usage), so a
    re-sent stream end never double counts while continue / regenerate (new runs) add theirs.
    """
    from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
    from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
    from ..models.conversation import Conversation  # pylint: disable=C0415
    try:
        delta = turn_usage_by_model(response_meta)
        if not delta:
            return
        group_meta = msg_group.meta or {}
        counted_runs = list(group_meta.get('usage_counted_runs') or [])
        count_key = usage_count_key(response_meta, delta)
        if count_key in counted_runs:
            return
        prices = price_models(delta, costs_pricer())
        conversation = session.query(Conversation).filter(
            Conversation.id == msg_group.conversation_id
        ).with_for_update().populate_existing().first()
        if conversation is None:
            return
        meta = dict(conversation.meta or {})
        meta['usage_totals'] = merge_usage_totals(meta.get('usage_totals'), delta, prices)
        conversation.meta = meta
        flag_modified(conversation, 'meta')
        msg_group.meta = {**group_meta, 'usage_counted_runs': counted_runs + [count_key]}
        flag_modified(msg_group, 'meta')
        session.flush()
    except Exception as e:  # pylint: disable=W0718 - usage is evidence, never a reason to lose the response
        log.warning(f"Failed to accumulate chat usage for message group {getattr(msg_group, 'id', None)}: {e}")
