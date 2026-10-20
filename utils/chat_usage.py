""" Cumulative per-chat token usage and approximate cost (#6898) """

from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Optional

from .evaluation_usage import COST_RUNTIME, COST_UNPRICED

USAGE_FIELDS = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_creation_tokens')


def _count(value) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return 0
    return int(value)


def turn_usage_by_model(response_meta: dict) -> dict:
    """{model: {input_tokens, output_tokens, cache_*}} of one completed response.

    The indexer's per-model rollup covers every callback-traced call (agent turns, tool loops,
    sub-agents). Summarization calls bypass callbacks, so the SDK reports them in context_info.
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


def costs_pricer() -> Optional[Callable[..., dict]]:
    try:
        from tools import context as pylon_context  # pylint: disable=E0401,C0415
    except Exception:  # pylint: disable=W0718
        return None

    def _pricer(**kwargs):
        return pylon_context.rpc_manager.timeout(5).costs_compute_llm_cost(**kwargs)
    return _pricer


def accumulate_chat_usage(session, msg_group, response_meta: dict) -> None:
    """Add a completed response's usage to its conversation's totals, once per indexer run.

    The conversation row is locked so concurrent participants do not lose each other's updates.
    The group remembers the runs it counted, so a re-sent stream end never double counts while
    continue / regenerate (new runs) add theirs.
    """
    from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
    from sqlalchemy.orm.attributes import flag_modified  # pylint: disable=C0415
    from ..models.conversation import Conversation  # pylint: disable=C0415
    try:
        run_id = response_meta.get('usage_run_id')
        group_meta = msg_group.meta or {}
        counted_runs = list(group_meta.get('usage_counted_runs') or [])
        if run_id and run_id in counted_runs:
            return
        delta = turn_usage_by_model(response_meta)
        if not delta:
            return
        conversation = session.query(Conversation).filter(
            Conversation.id == msg_group.conversation_id
        ).with_for_update().populate_existing().first()
        if conversation is None:
            return
        meta = dict(conversation.meta or {})
        meta['usage_totals'] = merge_usage_totals(meta.get('usage_totals'), delta, price_models(delta, costs_pricer()))
        conversation.meta = meta
        flag_modified(conversation, 'meta')
        if run_id:
            msg_group.meta = {**group_meta, 'usage_counted_runs': counted_runs + [run_id]}
            flag_modified(msg_group, 'meta')
        session.flush()
    except Exception as e:  # pylint: disable=W0718 - usage is evidence, never a reason to lose the response
        log.warning(f"Failed to accumulate chat usage for message group {getattr(msg_group, 'id', None)}: {e}")
