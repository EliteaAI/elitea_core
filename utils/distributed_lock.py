"""Redis-based distributed lock for horizontal scaling.

Provides mutual exclusion across multiple pylon pods using Redis SET NX EX.
Each lock is identified by a name and protected by a unique token (UUID) so
that only the holder can release it. TTL auto-releases the lock if the holder
crashes, preventing deadlocks.

Usage:
    lock = DistributedLock(redis_client)

    # Explicit acquire/release — caller owns the token
    token = lock.acquire("my_resource", ttl=30)
    if token:
        try:
            do_work()
        finally:
            lock.release("my_resource", token)

    # Context manager (recommended)
    with lock.lock("my_resource", ttl=30) as token:
        if token:
            do_work()
        else:
            handle_contention()

    # Blocking context manager (raises on timeout)
    with lock.lock("my_resource", ttl=30, wait=True, wait_timeout=10) as token:
        do_work()
"""

import contextlib
import time
import uuid

from pylon.core.tools import log


DEFAULT_TTL = 30
DEFAULT_WAIT_TIMEOUT = 10
DEFAULT_POLL_INTERVAL = 0.1

# Lua script for safe release: only delete if value matches our token
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Lua script for extending TTL: only extend if we still hold the lock
_EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""


class LockNotAcquired(Exception):
    """Raised when a blocking lock acquisition times out."""


class DistributedLock:
    """Redis-based distributed lock using SET NX EX + Lua release.

    Thread-safe and WSGI-worker-safe: no shared mutable state. The lock token
    is returned to the caller from acquire() and must be passed back to
    release()/extend(). This ensures correct behavior across threads, greenlets,
    and forked workers.
    """

    def __init__(self, redis_client, key_prefix: str = "lock"):
        self._client = redis_client
        self._prefix = key_prefix
        self._release_script = self._client.register_script(_RELEASE_SCRIPT)
        self._extend_script = self._client.register_script(_EXTEND_SCRIPT)

    def _key(self, name: str) -> str:
        return f"{self._prefix}:{name}"

    def acquire(self, name: str, ttl: int = DEFAULT_TTL):
        """Try to acquire a lock (non-blocking).

        Args:
            name: Lock name (resource identifier).
            ttl: Time-to-live in seconds. Lock auto-releases after this.

        Returns:
            str: The lock token (UUID) if acquired — caller must keep this
                 and pass to release()/extend(). None if lock is already held.
        """
        key = self._key(name)
        token = str(uuid.uuid4())

        acquired = self._client.set(key, token, nx=True, ex=ttl)
        if acquired:
            log.info("Distributed lock '%s' acquired (ttl=%ds)", name, ttl)
            return token
        return None

    def release(self, name: str, token: str) -> bool:
        """Release a lock. Only succeeds if we hold it (token matches).

        Args:
            name: Lock name to release.
            token: The token returned by acquire(). Must match Redis value.

        Returns:
            True if lock was released, False if we didn't hold it.
        """
        if not token:
            log.warning("Attempted to release lock '%s' with empty token", name)
            return False

        key = self._key(name)
        result = self._release_script(keys=[key], args=[token])
        if result:
            log.info("Distributed lock '%s' released", name)
            return True
        else:
            log.warning("Distributed lock '%s' release failed (expired or stolen)", name)
            return False

    def extend(self, name: str, token: str, additional_ms: int) -> bool:
        """Extend the TTL of a lock we hold.

        Args:
            name: Lock name to extend.
            token: The token returned by acquire().
            additional_ms: Milliseconds to set as new TTL.

        Returns:
            True if extended, False if we don't hold it.
        """
        if not token:
            return False

        key = self._key(name)
        result = self._extend_script(keys=[key], args=[token, str(additional_ms)])
        return bool(result)

    def is_locked(self, name: str) -> bool:
        """Check if a lock is currently held by anyone.

        Args:
            name: Lock name to check.

        Returns:
            True if the lock key exists in Redis.
        """
        key = self._key(name)
        return bool(self._client.exists(key))

    def acquire_blocking(self, name: str, ttl: int = DEFAULT_TTL,
                         wait_timeout: float = DEFAULT_WAIT_TIMEOUT,
                         poll_interval: float = DEFAULT_POLL_INTERVAL):
        """Acquire a lock, blocking until available or timeout.

        Args:
            name: Lock name.
            ttl: Time-to-live in seconds once acquired.
            wait_timeout: Max seconds to wait for acquisition.
            poll_interval: Seconds between retry attempts.

        Returns:
            str: The lock token if acquired.

        Raises:
            LockNotAcquired: If timeout expires before lock is available.
        """
        start = time.time()
        while True:
            token = self.acquire(name, ttl)
            if token:
                return token

            elapsed = time.time() - start
            if elapsed + poll_interval > wait_timeout:
                raise LockNotAcquired(
                    f"Could not acquire lock '{name}' within {wait_timeout}s"
                )
            time.sleep(poll_interval)

    @contextlib.contextmanager
    def lock(self, name: str, ttl: int = DEFAULT_TTL,
             wait: bool = False, wait_timeout: float = DEFAULT_WAIT_TIMEOUT,
             poll_interval: float = DEFAULT_POLL_INTERVAL):
        """Context manager for distributed lock.

        Args:
            name: Lock name.
            ttl: TTL in seconds.
            wait: If True, block until acquired (raises LockNotAcquired on timeout).
                  If False, yield token or None indicating acquisition status.
            wait_timeout: Max seconds to wait (only when wait=True).
            poll_interval: Seconds between retries (only when wait=True).

        Yields:
            str or None: The lock token if acquired, None if not (only when wait=False).
            When wait=True, always yields the token or raises LockNotAcquired.

        Raises:
            LockNotAcquired: When wait=True and timeout expires.
        """
        if wait:
            token = self.acquire_blocking(name, ttl, wait_timeout, poll_interval)
            try:
                yield token
            finally:
                self.release(name, token)
        else:
            token = self.acquire(name, ttl)
            try:
                yield token
            finally:
                if token:
                    self.release(name, token)
