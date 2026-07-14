"""
Lightweight fallback cost estimator for LLM calls.

Primary cost source is LiteLLM's response_cost (captured at event time by the
tracing plugin). This module provides a best-effort fallback when response_cost
is unavailable (e.g., direct SDK calls bypassing the proxy).

Prices are approximate and may drift from actual billing. The authoritative
source is always the gateway's per-call response_cost.
"""

_PRICE_DEFAULTS = {
    "gpt-4o": (0.0025, 0.010),
    "gpt-4o-mini": (0.00015, 0.0006),
    "gpt-4-turbo": (0.01, 0.03),
    "gpt-4": (0.03, 0.06),
    "gpt-3.5-turbo": (0.0005, 0.0015),
    "claude-3-5-sonnet": (0.003, 0.015),
    "claude-3-5-haiku": (0.0008, 0.004),
    "claude-3-opus": (0.015, 0.075),
    "claude-sonnet-4": (0.003, 0.015),
    "claude-opus-4": (0.015, 0.075),
    "gemini-1.5-pro": (0.00125, 0.005),
    "gemini-1.5-flash": (0.000075, 0.0003),
    "gemini-2.0-flash": (0.0001, 0.0004),
}


def estimate_cost(model_name, input_tokens=0, output_tokens=0):
    """
    Best-effort USD cost estimate from hardcoded defaults.

    Returns float or None if model is unknown. This is a FALLBACK —
    prefer response_cost from LiteLLM when available.
    """
    if not model_name:
        return None
    pricing = _PRICE_DEFAULTS.get(model_name)
    if not pricing:
        for key in _PRICE_DEFAULTS:
            if key in model_name or model_name in key:
                pricing = _PRICE_DEFAULTS[key]
                break
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
