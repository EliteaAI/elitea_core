"""
Tracing utilities for elitea_core plugin.
Helpers for propagating trace context to indexer tasks.
"""

from typing import Dict, Any, Optional
from pylon.core.tools import log


def get_current_traceparent() -> Optional[str]:
    """
    Get the current W3C traceparent from the active OpenTelemetry span.

    Returns:
        str: W3C traceparent header value or None if tracing is disabled/no active trace
    """
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span and span.get_span_context().is_valid:
            ctx = span.get_span_context()
            trace_id = format(ctx.trace_id, '032x')
            span_id = format(ctx.span_id, '016x')
            flags = '01' if ctx.trace_flags.sampled else '00'
            traceparent = f"00-{trace_id}-{span_id}-{flags}"
            log.debug(f"Generated traceparent for propagation: {traceparent}")
            return traceparent
        else:
            log.debug("No active span context available for traceparent")
            return None
    except ImportError:
        log.debug("OpenTelemetry not available, skipping trace propagation")
        return None
    except Exception as e:
        log.debug(f"Failed to get traceparent: {e}")
        return None


def add_trace_context_to_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add trace context to task metadata for propagation to indexer.

    Args:
        meta: Task metadata dict

    Returns:
        Updated metadata dict with traceparent added (if available)
    """
    traceparent = get_current_traceparent()
    if traceparent:
        meta['traceparent'] = traceparent
    return meta
