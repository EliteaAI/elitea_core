"""Redis-based distributed lock for horizontal scaling.

Provides mutual exclusion across multiple pylon pods using Redis SET NX EX.
Each lock is identified by a name and protected by a unique token (UUID) so
that only the holder can release it. TTL auto-releases the lock if the holder
crashes, preventing deadlocks.

Usage:
    lock = DistributedLock(redis_client)

    # Explicit acquire/release
    if lock.acquire("my_resource", ttl=30):
        try:
            do_work()
        finally:
            lock.release("my_resource")

    # Context manager
    with lock.lock("my_resource", ttl=30) as acquired:
        if acquired:
            do_work()
        else:
            handle_contention()

    # Blocking context manager (raises on timeout)
    with lock.lock("my_resource", ttl=30, wait=True, wait_timeout=10):
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
    """Redis-based distributed lock using SET NX EX + Lua release."""

    def __init__(self, redis_client, key_prefix: str = "lock"):
        self._client = redis_client
        self._prefix = key_prefix
        self._tokens = {}
        self._release_script = self._client.register_script(_RELEASE_SCRIPT)
        self._extend_script = self._client.register_script(_EXTEND_SCRIPT)

    def _key(self, name: str) -> str:
        return f"{self._prefix}:{name}"

    def acquire(self, name: str, ttl: int = DEFAULT_TTL) -> bool:
        """Try to acquire a lock (non-blocking).

        Args:
            name: Lock name (resource identifier).
            ttl: Time-to-live in seconds. Lock auto-releases after this.

        Returns:
            True if lock was acquired, False if already held by another.
        """
        key = self._key(name)
        token = str(uuid.uuid4())

        acquired = self._client.set(key, token, nx=True, ex=ttl)
        if acquired:
            self._tokens[name] = token
            log.info("Distributed lock '%s' acquired (ttl=%ds)", name, ttl)
            return True
        return False

    def release(self, name: str) -> bool:
        """Release a lock. Only succeeds if we hold it (token matches).

        Args:
            name: Lock name to release.

        Returns:
            True if lock was released, False if we didn't hold it.
        """
        token = self._tokens.pop(name, None)
        if token is None:
            log.warning("Attempted to release lock '%s' but no token found", name)
            return False

        key = self._key(name)
        result = self._release_script(keys=[key], args=[token])
        if result:
            log.info("Distributed lock '%s' released", name)
            return True
        else:
            log.warning("Distributed lock '%s' release failed (expired or stolen)", name)
            return False

    def extend(self, name: str, additional_ms: int) -> bool:
        """Extend the TTL of a lock we hold.

        Args:
            name: Lock name to extend.
            additional_ms: Milliseconds to set as new TTL.

        Returns:
            True if extended, False if we don't hold it.
        """
        token = self._tokens.get(name)
        if token is None:
            return False

        key = self._key(name)
        result = self._extend_script(keys=[key], args=[token, str(additional_ms)])
        return bool(result)

    def is_held(self, name: str) -> bool:
        """Check if we currently hold a lock (token exists locally).

        Args:
            name: Lock name to check.

        Returns:
            True if we have a token for this lock.
        """
        return name in self._tokens

    def acquire_blocking(self, name: str, ttl: int = DEFAULT_TTL,
                         wait_timeout: float = DEFAULT_WAIT_TIMEOUT,
                         poll_interval: float = DEFAULT_POLL_INTERVAL) -> bool:
        """Acquire a lock, blocking until available or timeout.

        Args:
            name: Lock name.
            ttl: Time-to-live in seconds once acquired.
            wait_timeout: Max seconds to wait for acquisition.
            poll_interval: Seconds between retry attempts.

        Returns:
            True if acquired.

        Raises:
            LockNotAcquired: If timeout expires before lock is available.
        """
        start = time.time()
        while True:
            if self.acquire(name, ttl):
                return True

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
                  If False, yield True/False indicating acquisition status.
            wait_timeout: Max seconds to wait (only when wait=True).
            poll_interval: Seconds between retries (only when wait=True).

        Yields:
            bool: True if lock was acquired, False otherwise (only when wait=False).
            When wait=True, always yields True or raises LockNotAcquired.

        Raises:
            LockNotAcquired: When wait=True and timeout expires.
        """
        if wait:
            self.acquire_blocking(name, ttl, wait_timeout, poll_interval)
            try:
                yield True
            finally:
                self.release(name)
        else:
            acquired = self.acquire(name, ttl)
            try:
                yield acquired
            finally:
                if acquired:
                    self.release(name)
