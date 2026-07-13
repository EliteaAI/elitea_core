"""
Lightweight utility to estimate LLM cost from token counts.

Uses the model_pricing table when available; returns None if the model is not
found (missing rows are treated as "unpriced" rather than an error).
"""

import threading

from pylon.core.tools import log

# In-process cache to avoid per-event DB lookups: {model_name: (input_cpm, output_cpm)}
_PRICE_CACHE: dict = {}
_CACHE_LOADED = False
_CACHE_LOCK = threading.Lock()


def _load_cache():
    """Load all pricing rows into the in-process cache once per process."""
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    with _CACHE_LOCK:
        if _CACHE_LOADED:
            return
        try:
            from tools import db
            from ..models.model_pricing import ModelPricing
            with db.with_project_schema_session(None) as session:
                rows = session.query(
                    ModelPricing.model_name,
                    ModelPricing.input_cost_per_1k,
                    ModelPricing.output_cost_per_1k,
                ).all()
                for r in rows:
                    _PRICE_CACHE[r.model_name] = (r.input_cost_per_1k, r.output_cost_per_1k)
            _CACHE_LOADED = True
        except Exception as e:
            log.debug(f"llm_cost: failed to load pricing cache: {e}")


def estimate_cost(model_name: str, input_tokens: int, output_tokens: int):
    """
    Estimate USD cost for one LLM call.

    Returns float USD or None if model not priced.
    """
    if not model_name:
        return None
    _load_cache()
    pricing = _PRICE_CACHE.get(model_name)
    if not pricing:
        return None
    input_cpm, output_cpm = pricing
    if input_cpm is None or output_cpm is None:
        return None
    try:
        cost = (input_tokens or 0) / 1000.0 * float(input_cpm) + (output_tokens or 0) / 1000.0 * float(output_cpm)
        return round(cost, 8)
    except (TypeError, ValueError):
        return None


def invalidate_cache():
    """Call after model_pricing table updates to force reload on next estimate."""
    global _CACHE_LOADED
    with _CACHE_LOCK:
        _CACHE_LOADED = False
        _PRICE_CACHE.clear()
