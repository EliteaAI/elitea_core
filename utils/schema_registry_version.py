"""A counter of the changes of the dynamic toolkit schemas of this process (C-1009 performance).

Provider-hub toolkits and MCP servers come and go while Main runs; their schemas are aggregated on demand
(``toolkits_utils.get_toolkit_schemas``). What is derived from them and kept (the inline secret field names of a toolkit
type, ``credential_decision.toolkit_secret_fields``) is valid for the number this counter had when it was derived. Every
registration, removal or refresh of a provider or an MCP server calls ``bump``.

``bump`` hands out a number nobody had before, so two bumps never leave the counter where a cached value expects it,
and nothing needs a lock.
"""
import itertools

_NUMBERS = itertools.count(1)
_current = 0


def bump() -> int:
    """The schemas changed: whatever was derived from the number before this is stale."""
    global _current  # pylint: disable=W0603
    _current = next(_NUMBERS)
    return _current


def current() -> int:
    return _current
