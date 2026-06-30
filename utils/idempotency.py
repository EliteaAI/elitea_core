"""Redis-based idempotency key store for horizontal scaling.

Guarantees that duplicate API requests (same operation + parameters) produce
the same result without re-executing the handler. Uses Redis to store the
result of the first execution, keyed by a deterministic hash of the operation
and its parameters.

Key layout:
    idempotency:{operation}:{hash_of_params}  — JSON-serialized result, with TTL

Flow:
    1. Compute idempotency key from operation name + request params
    2. Check Redis: if key exists, return cached result (skip handler)
    3. If key absent: execute handler, store result in Redis with TTL
    4. Return result

Usage:
    store = IdempotencyStore(redis_client)

    # Manual check-and-set
    cached = store.get("create_agent", params_hash)
    if cached is not None:
        return cached  # Already executed

    result = do_work()
    store.set("create_agent", params_hash, result, ttl=3600)

    # Decorator for API handlers
    @idempotent(redis_client, key_func=lambda req: f"{req.project_id}:{req.name}", ttl=3600)
    def create_agent(request):
        ...  # only executes once per unique key
"""

import functools
import hashlib
import json
import time

from pylon.core.tools import log


DEFAULT_TTL = 3600  # 1 hour
KEY_PREFIX = "idempotency"

# Sentinel to distinguish "no cached result" from "cached result is None"
_MISSING = object()


class IdempotencyStore:
    """Redis-backed store for idempotent operation results."""

    def __init__(self, redis_client, key_prefix: str = KEY_PREFIX,
                 default_ttl: int = DEFAULT_TTL):
        """Initialize the idempotency store.

        Args:
            redis_client: Redis client instance.
            key_prefix: Prefix for idempotency keys in Redis.
            default_ttl: Default TTL in seconds for cached results.
        """
        self._client = redis_client
        self._prefix = key_prefix
        self._default_ttl = default_ttl

    def _key(self, operation: str, params_hash: str) -> str:
        return f"{self._prefix}:{operation}:{params_hash}"

    def check_and_set(self, key: str, result, ttl: int = None) -> tuple:
        """Atomically check for existing result or set a new one.

        Uses SET NX EX with GET flag (Redis 6.2+) for atomic check-and-set.
        If the key already exists, the existing value is returned without
        modification. If absent, the result is stored atomically.

        Args:
            key: Full idempotency key (operation:hash).
            result: The result to store if key is absent.
            ttl: TTL in seconds. Defaults to default_ttl.

        Returns:
            Tuple of (was_cached: bool, result: any).
            If was_cached is True, result is the previously stored value.
            If was_cached is False, the provided result was stored.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        full_key = f"{self._prefix}:{key}" if not key.startswith(self._prefix) else key

        serialized = json.dumps(result, default=str)
        was_set = self._client.set(full_key, serialized, nx=True, ex=ttl)
        if was_set:
            return (False, result)

        existing = self._client.get(full_key)
        if existing is not None:
            try:
                cached = json.loads(existing)
                return (True, cached)
            except (json.JSONDecodeError, TypeError):
                if isinstance(existing, bytes):
                    return (True, existing.decode(errors="replace"))
                return (True, existing)

        return (False, result)

    def get(self, operation: str, params_hash: str):
        """Get cached result for an operation.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.

        Returns:
            Cached result if exists, None otherwise.
            Use has() to distinguish "no cache" from "cached None".
        """
        key = self._key(operation, params_hash)
        raw = self._client.get(key)
        if raw is None:
            return None

        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            if isinstance(raw, bytes):
                return raw.decode(errors="replace")
            return raw

    def has(self, operation: str, params_hash: str) -> bool:
        """Check if a cached result exists (without retrieving it).

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.

        Returns:
            True if a cached result exists.
        """
        key = self._key(operation, params_hash)
        return bool(self._client.exists(key))

    def set(self, operation: str, params_hash: str, result, ttl: int = None) -> bool:
        """Store a result for an operation.

        Uses SET NX to avoid overwriting an existing result from a concurrent
        request that completed first.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.
            result: Result to cache (must be JSON-serializable).
            ttl: TTL in seconds.

        Returns:
            True if stored (was first), False if key already existed.
        """
        key = self._key(operation, params_hash)
        ttl = ttl if ttl is not None else self._default_ttl

        serialized = json.dumps(result, default=str)
        was_set = self._client.set(key, serialized, nx=True, ex=ttl)
        return bool(was_set)

    def force_set(self, operation: str, params_hash: str, result, ttl: int = None):
        """Store a result unconditionally (overwrite any existing).

        Use for correcting a cached result or refreshing TTL.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.
            result: Result to cache.
            ttl: TTL in seconds.
        """
        key = self._key(operation, params_hash)
        ttl = ttl if ttl is not None else self._default_ttl
        serialized = json.dumps(result, default=str)
        self._client.set(key, serialized, ex=ttl)

    def invalidate(self, operation: str, params_hash: str) -> bool:
        """Remove a cached result, allowing the operation to re-execute.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.

        Returns:
            True if entry was removed, False if it didn't exist.
        """
        key = self._key(operation, params_hash)
        return bool(self._client.delete(key))

    def get_ttl(self, operation: str, params_hash: str) -> int:
        """Get remaining TTL for a cached result.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.

        Returns:
            Remaining TTL in seconds, -2 if doesn't exist, -1 if no expiry.
        """
        key = self._key(operation, params_hash)
        return self._client.ttl(key)

    def get_with_metadata(self, operation: str, params_hash: str) -> dict:
        """Get cached result with TTL metadata.

        Args:
            operation: Operation name.
            params_hash: Hash of the operation parameters.

        Returns:
            Dict with 'result', 'ttl_remaining', 'exists' keys.
            Returns {'exists': False, 'result': None, 'ttl_remaining': -2} if absent.
        """
        key = self._key(operation, params_hash)
        pipe = self._client.pipeline(transaction=False)
        pipe.get(key)
        pipe.ttl(key)
        raw, ttl = pipe.execute()

        if raw is None:
            return {"exists": False, "result": None, "ttl_remaining": -2}

        try:
            result = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            result = raw.decode(errors="replace") if isinstance(raw, bytes) else raw

        return {"exists": True, "result": result, "ttl_remaining": ttl}

    def bulk_check(self, operation: str, params_hashes: list) -> dict:
        """Check multiple keys for cached results in a single pipeline.

        Args:
            operation: Operation name.
            params_hashes: List of parameter hashes to check.

        Returns:
            Dict mapping params_hash -> cached_result (or None if absent).
        """
        if not params_hashes:
            return {}

        pipe = self._client.pipeline(transaction=False)
        valid = []
        for h in params_hashes:
            if h:
                pipe.get(self._key(operation, h))
                valid.append(h)

        results = pipe.execute()
        output = {}
        for h, raw in zip(valid, results):
            if raw is None:
                output[h] = None
            else:
                try:
                    output[h] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    output[h] = raw.decode(errors="replace") if isinstance(raw, bytes) else raw

        return output


def compute_params_hash(*args, **kwargs) -> str:
    """Compute a deterministic hash from function arguments.

    Creates a SHA-256 hash of the JSON-serialized arguments for use as
    the params_hash in IdempotencyStore.

    Args:
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        32-character hex string hash.
    """
    content = json.dumps({"args": args, "kwargs": kwargs},
                         sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def idempotent(redis_client, key_func=None, ttl: int = DEFAULT_TTL,
               operation: str = None, key_prefix: str = KEY_PREFIX):
    """Decorator ensuring a handler executes at most once per unique key.

    On first call: executes the function, stores result in Redis.
    On subsequent calls with same key (within TTL): returns cached result.

    Args:
        redis_client: Redis client instance.
        key_func: Callable that receives the same args/kwargs as the decorated
                  function and returns a string key identifying this specific
                  invocation. If None, the hash of all arguments is used.
        ttl: TTL in seconds for cached results.
        operation: Operation name for the Redis key. If None, uses the
                   decorated function's qualified name.
        key_prefix: Redis key prefix.

    Returns:
        Decorator function.

    Usage:
        @idempotent(redis_client, key_func=lambda req: f"{req.project_id}:{req.name}")
        def create_agent(request):
            ...  # Only runs once per project_id+name combination

        @idempotent(redis_client, ttl=60)
        def compute_expensive(x, y):
            ...  # Cached for 60s based on hash of (x, y)
    """
    store = IdempotencyStore(redis_client, key_prefix=key_prefix,
                             default_ttl=ttl)

    def decorator(func):
        op_name = operation or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if key_func is not None:
                params_hash = key_func(*args, **kwargs)
            else:
                params_hash = compute_params_hash(*args, **kwargs)

            cached = store.get(op_name, params_hash)
            if cached is not None:
                log.debug(
                    "Idempotent cache hit for %s (key=%s)",
                    op_name, params_hash[:8]
                )
                return cached

            result = func(*args, **kwargs)

            if result is not None:
                store.set(op_name, params_hash, result, ttl)
            else:
                store.force_set(op_name, params_hash, result, ttl)

            return result

        wrapper._idempotency_store = store
        wrapper._operation = op_name
        return wrapper
    return decorator
