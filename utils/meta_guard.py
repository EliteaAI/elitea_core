"""Unconditional strip of the two heavy message-group meta keys on read.

tool_calls / thinking_steps now live in message_trace_step (Epic #5724). New groups never carry
them in meta; old (pre-migration) groups still hold the monolithic blob until the backfill removes
it. Reads unconditionally drop both keys in SQL so an old group's blob is never detoasted onto the
gevent hub (the freeze class from 2026-06). This is a safety net, not a fallback — the data is
served from the table via the trace-step endpoints.
"""
from sqlalchemy import cast, Text
from sqlalchemy.dialects.postgresql import ARRAY

HEAVY_META_KEYS: tuple = ('tool_calls', 'thinking_steps')


def strip_heavy_meta_expr(meta_column):
    """SQL: meta with tool_calls / thinking_steps removed (jsonb `-` text[] operator)."""
    return meta_column.op('-')(cast(list(HEAVY_META_KEYS), ARRAY(Text)))


def strip_heavy_meta_keys(meta: dict) -> dict:
    """Python: drop tool_calls / thinking_steps; keep every other key."""
    return {k: v for k, v in (meta or {}).items() if k not in HEAVY_META_KEYS}
