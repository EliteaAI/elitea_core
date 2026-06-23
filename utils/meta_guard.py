"""Oversized message-group meta protection (simplified Option A).

A single message_group.meta can reach tens of MB (hundreds of tool_calls / thinking_steps, each with
full tool_output / raw LLM message bodies). Serializing that on the gevent hub is pure CPU with no
yield and freezes pylon-main (confirmed: a 25 MB meta took 33-66 s; froze the platform 2026-06-22/23).

We strip only the two unbounded keys (tool_calls / thinking_steps); every other key is preserved.
Detection uses octet_length(meta::text) (DECOMPRESSED), not pg_column_size() which reports the
pglz-compressed on-disk size and under-reports ~2.9x for repetitive tool_calls JSON.
"""
from sqlalchemy import func, case, cast, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

# 3 MB per single group; 5 MB cumulative across one response.
META_SIZE_LIMIT_BYTES: int = 3 * 1024 * 1024
RESPONSE_META_BUDGET_BYTES: int = 5 * 1024 * 1024
HEAVY_META_KEYS: tuple = ('tool_calls', 'thinking_steps')


def meta_bytes_expr(meta_column):
    """SQL: decompressed byte size of the meta JSONB."""
    return func.octet_length(meta_column.cast(Text))


def safe_meta_expr(meta_column):
    """SQL: full meta if under the per-group limit, else heavy keys stripped + _oversized flag.

    Keeps the heavy blob inside Postgres when oversized so it never crosses the wire / hub.
    """
    return case(
        (meta_bytes_expr(meta_column) > META_SIZE_LIMIT_BYTES,
         meta_column.op('-')(cast(list(HEAVY_META_KEYS), ARRAY(Text)))
         .op('||')(cast({'_oversized': True}, JSONB))),
        else_=meta_column,
    )


def strip_heavy_meta_keys(meta: dict) -> dict:
    """Drop tool_calls / thinking_steps; keep every other key. _oversized is the sole signal.

    Used by the read paths after the SQL strip (safe_meta_expr) has already replaced an oversized
    meta with `{..lightweight keys.., _oversized: true}`, and for the cumulative-budget trim where a
    group is individually fine but the response total would freeze the hub.
    """
    trimmed = {k: v for k, v in (meta or {}).items() if k not in HEAVY_META_KEYS}
    trimmed['_oversized'] = True
    return trimmed
