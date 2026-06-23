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
    """Drop tool_calls / thinking_steps; keep every other key. _oversized is the sole signal."""
    trimmed = {k: v for k, v in (meta or {}).items() if k not in HEAVY_META_KEYS}
    trimmed['_oversized'] = True
    return trimmed


def _heavy_bytes(meta: dict) -> int:
    """Decompressed byte size of just the heavy keys (0 if none). Cheap relative to a full dump."""
    import json
    if not (meta.get('tool_calls') or meta.get('thinking_steps')):
        return 0
    heavy = {k: meta[k] for k in HEAVY_META_KEYS if k in meta}
    return len(json.dumps(heavy, separators=(',', ':'), default=str).encode())


def guard_meta_dict(meta: dict) -> dict:
    """Python-side guard for from_orm/serialize paths (no SQL strip). Strips heavy keys if oversized."""
    if _heavy_bytes(meta or {}) > META_SIZE_LIMIT_BYTES:
        return strip_heavy_meta_keys(meta)
    return meta or {}


def guard_meta_cumulative(meta: dict, running_bytes: int) -> tuple:
    """Per-group + cumulative-budget guard for list endpoints (no SQL strip available).

    Returns (guarded_meta, new_running_bytes). Oversized single groups are stripped and don't charge
    the budget; once the running total would exceed the response budget the group is also stripped.
    """
    meta = meta or {}
    hb = _heavy_bytes(meta)
    if hb > META_SIZE_LIMIT_BYTES:
        return strip_heavy_meta_keys(meta), running_bytes
    if running_bytes + hb > RESPONSE_META_BUDGET_BYTES:
        return strip_heavy_meta_keys(meta), running_bytes
    return meta, running_bytes + hb
