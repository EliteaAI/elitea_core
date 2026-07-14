"""Redis-based event deduplication for horizontal scaling.

Prevents duplicate processing of events when multiple pods receive the same
event (e.g. via pub/sub broadcast or retry). Uses Redis SET NX with TTL to
track processed event IDs. If an event ID has already been seen within the
TTL window, it is considered a duplicate and should be skipped.

Key layout:
    event_dedup:{event_id}  — value "1", with TTL

Usage:
    dedup = EventDeduplicator(redis_client)

    # Check-and-mark in one call
    if dedup.is_duplicate("evt-abc-123"):
        return  # already processed

    # Or use the decorator on event handlers
    @deduplicate(redis_client, ttl=300)
    def handle_task_event(event_data):
        ...  # only runs once per event_id
"""

import functools
import hashlib
import json
import time

from pylon.core.tools import log


DEFAULT_TTL = 300  # 5 minutes
KEY_PREFIX = "event_dedup"


class EventDeduplicator:
    """Redis-based event deduplication using SET NX EX."""

    def __init__(self, redis_client, key_prefix: str = KEY_PREFIX,
                 default_ttl: int = DEFAULT_TTL):
        """Initialize the deduplicator.

        Args:
            redis_client: Redis client instance.
            key_prefix: Prefix for dedup keys in Redis.
            default_ttl: Default TTL in seconds for dedup entries.
        """
        self._client = redis_client
        self._prefix = key_prefix
        self._default_ttl = default_ttl

    def _key(self, event_id: str) -> str:
        return f"{self._prefix}:{event_id}"

    def is_duplicate(self, event_id: str, ttl_seconds: int = None) -> bool:
        """Check if an event has already been processed.

        Uses SET NX EX atomically: if the key doesn't exist, it's set with
        TTL and returns False (not a duplicate). If the key exists, returns
        True (duplicate).

        Args:
            event_id: Unique identifier for the event.
            ttl_seconds: TTL for the dedup entry. Defaults to default_ttl.

        Returns:
            True if the event was already processed (duplicate).
            False if this is the first time seeing this event.
        """
        if not event_id:
            return False

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        key = self._key(event_id)

        was_set = self._client.set(key, "1", nx=True, ex=ttl)
        if was_set:
            return False  # First time — not a duplicate
        return True  # Already exists — duplicate

    def mark_processed(self, event_id: str, ttl_seconds: int = None) -> bool:
        """Explicitly mark an event as processed.

        Useful when you want to separate the check from the mark (e.g.,
        mark only after successful processing).

        Args:
            event_id: Unique identifier for the event.
            ttl_seconds: TTL for the dedup entry.

        Returns:
            True if marked (was not previously processed).
            False if already marked.
        """
        if not event_id:
            return False

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        key = self._key(event_id)

        was_set = self._client.set(key, "1", nx=True, ex=ttl)
        return bool(was_set)

    def is_processed(self, event_id: str) -> bool:
        """Check if an event was previously processed (without marking it).

        Args:
            event_id: Unique identifier for the event.

        Returns:
            True if the event was already processed.
        """
        if not event_id:
            return False

        key = self._key(event_id)
        return bool(self._client.exists(key))

    def clear(self, event_id: str) -> bool:
        """Remove a dedup entry, allowing the event to be processed again.

        Args:
            event_id: Unique identifier for the event to clear.

        Returns:
            True if the entry was removed, False if it didn't exist.
        """
        key = self._key(event_id)
        return bool(self._client.delete(key))

    def get_ttl(self, event_id: str) -> int:
        """Get remaining TTL for a dedup entry.

        Args:
            event_id: Unique identifier for the event.

        Returns:
            Remaining TTL in seconds, -2 if key doesn't exist, -1 if no expiry.
        """
        key = self._key(event_id)
        return self._client.ttl(key)

    def bulk_check(self, event_ids: list) -> dict:
        """Check multiple event IDs for duplicates in a single pipeline call.

        Args:
            event_ids: List of event IDs to check.

        Returns:
            Dict mapping event_id -> bool (True if duplicate).
        """
        if not event_ids:
            return {}

        pipe = self._client.pipeline(transaction=False)
        keys = []
        for event_id in event_ids:
            if event_id:
                key = self._key(event_id)
                keys.append((event_id, key))
                pipe.exists(key)

        results = pipe.execute()

        return {
            event_id: bool(exists)
            for (event_id, _), exists in zip(keys, results)
        }

    def bulk_mark(self, event_ids: list, ttl_seconds: int = None) -> dict:
        """Mark multiple events as processed in a single pipeline call.

        Args:
            event_ids: List of event IDs to mark.
            ttl_seconds: TTL for entries.

        Returns:
            Dict mapping event_id -> bool (True if newly marked, False if existed).
        """
        if not event_ids:
            return {}

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        pipe = self._client.pipeline(transaction=False)
        valid_ids = []
        for event_id in event_ids:
            if event_id:
                key = self._key(event_id)
                valid_ids.append(event_id)
                pipe.set(key, "1", nx=True, ex=ttl)

        results = pipe.execute()

        return {
            event_id: bool(was_set)
            for event_id, was_set in zip(valid_ids, results)
        }


def generate_event_id(*args) -> str:
    """Generate a deterministic event ID from arbitrary arguments.

    Useful for creating dedup keys from event payload fields. Uses SHA-256
    hash of the JSON-serialized arguments for collision resistance.

    Args:
        *args: Values to hash (must be JSON-serializable).

    Returns:
        Hex string event ID.
    """
    content = json.dumps(args, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def deduplicate(redis_client, ttl: int = DEFAULT_TTL,
                event_id_func=None, key_prefix: str = KEY_PREFIX):
    """Decorator for event handlers that ensures at-most-once processing.

    The decorated function is only called if the event has not been seen
    before within the TTL window. The event_id is extracted from the first
    positional argument (assumed to be event data dict with an 'event_id'
    field) or computed via the event_id_func.

    Args:
        redis_client: Redis client instance.
        ttl: TTL in seconds for the dedup window.
        event_id_func: Optional callable(event_data) -> str to extract/generate
                       the event ID. If None, uses event_data.get('event_id')
                       or generates from the full payload.
        key_prefix: Redis key prefix.

    Returns:
        Decorator function.

    Usage:
        @deduplicate(redis_client, ttl=300)
        def handle_event(event_data):
            ...

        @deduplicate(redis_client, ttl=60, event_id_func=lambda d: d['task_id'])
        def handle_task(event_data):
            ...
    """
    deduplicator = EventDeduplicator(redis_client, key_prefix=key_prefix,
                                     default_ttl=ttl)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            event_data = args[0] if args else kwargs.get('event_data', {})

            if event_id_func is not None:
                event_id = event_id_func(event_data)
            elif isinstance(event_data, dict) and 'event_id' in event_data:
                event_id = event_data['event_id']
            else:
                event_id = generate_event_id(event_data)

            if deduplicator.is_duplicate(event_id, ttl):
                log.debug(
                    "Event '%s' deduplicated (already processed within %ds)",
                    event_id, ttl
                )
                return None

            return func(*args, **kwargs)

        wrapper._deduplicator = deduplicator
        return wrapper
    return decorator
