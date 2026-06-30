import json
import time


DEFAULT_TTL = 120
MAX_RETRIES = 3
VERSION_KEY_SUFFIX = ":version"


class CanvasVersionConflict(Exception):
    """Raised when a canvas update conflicts with a concurrent modification."""

    def __init__(self, canvas_key, current_version):
        self.canvas_key = canvas_key
        self.current_version = current_version
        super().__init__(
            f"Version conflict on {canvas_key}, current version: {current_version}"
        )


class RedisCanvasVersioning:
    """Provides optimistic locking for canvas content stored in Redis.

    Uses WATCH + MULTI/EXEC to detect concurrent modifications.
    Each canvas key has a companion version counter key that is
    atomically incremented on every update.
    """

    def __init__(self, redis_client, content_ttl=DEFAULT_TTL):
        self._client = redis_client
        self._content_ttl = content_ttl

    def _version_key(self, canvas_key):
        return canvas_key + VERSION_KEY_SUFFIX

    def get_content(self, canvas_key):
        """Get the current canvas content and version.

        Returns:
            tuple: (content: str or None, version: int)
        """
        pipe = self._client.pipeline(transaction=False)
        pipe.get(canvas_key)
        pipe.get(self._version_key(canvas_key))
        content, version_raw = pipe.execute()
        version = int(version_raw) if version_raw else 0
        return content, version

    def set_content_atomic(self, canvas_key, content, expected_version=None, ttl=None):
        """Atomically set canvas content with optimistic locking.

        If expected_version is None, sets unconditionally (first write or
        non-versioned context like join_canvas initialization).

        If expected_version is provided, uses WATCH on the version key to
        detect concurrent modifications. Raises CanvasVersionConflict if the
        version has changed since the caller last read it.

        Args:
            canvas_key: The Redis key for the canvas content.
            content: New content string to store.
            expected_version: The version the caller expects (from get_content).
                              None means unconditional write.
            ttl: TTL in seconds for both content and version keys.
                 Defaults to self._content_ttl.

        Returns:
            int: The new version number after the update.

        Raises:
            CanvasVersionConflict: If a concurrent modification was detected.
        """
        if ttl is None:
            ttl = self._content_ttl

        version_key = self._version_key(canvas_key)

        if expected_version is None:
            return self._unconditional_set(canvas_key, version_key, content, ttl)

        return self._conditional_set(
            canvas_key, version_key, content, expected_version, ttl
        )

    def _unconditional_set(self, canvas_key, version_key, content, ttl):
        """Set content without checking version — used for initial writes."""
        pipe = self._client.pipeline(transaction=True)
        pipe.set(canvas_key, content, ex=ttl)
        pipe.incr(version_key)
        pipe.expire(version_key, ttl)
        results = pipe.execute()
        new_version = results[1]
        return new_version

    def _conditional_set(self, canvas_key, version_key, content, expected_version, ttl):
        """Set content with optimistic locking via WATCH/MULTI/EXEC."""
        with self._client.pipeline(transaction=True) as pipe:
            pipe.watch(version_key)
            current_version_raw = pipe.get(version_key)
            current_version = int(current_version_raw) if current_version_raw else 0

            if current_version != expected_version:
                pipe.unwatch()
                raise CanvasVersionConflict(canvas_key, current_version)

            pipe.multi()
            pipe.set(canvas_key, content, ex=ttl)
            pipe.incr(version_key)
            pipe.expire(version_key, ttl)

            try:
                results = pipe.execute()
            except Exception as e:
                if "WatchError" in type(e).__name__ or "WATCH" in str(e):
                    current = self._get_current_version(version_key)
                    raise CanvasVersionConflict(canvas_key, current) from e
                raise

            new_version = results[1]
            return new_version

    def _get_current_version(self, version_key):
        raw = self._client.get(version_key)
        return int(raw) if raw else 0

    def set_content_with_retry(
        self, canvas_key, content, expected_version=None, ttl=None, max_retries=MAX_RETRIES
    ):
        """Attempt to set content with automatic retry on WatchError.

        On each retry, re-reads the current version and retries the write.
        Useful when the caller wants best-effort atomicity without manual retry
        logic. Only retries if expected_version was provided.

        Args:
            canvas_key: The Redis key for the canvas content.
            content: New content string to store.
            expected_version: Starting expected version. If None, does unconditional set.
            ttl: TTL in seconds.
            max_retries: Maximum number of retry attempts.

        Returns:
            int: The new version number after the update.

        Raises:
            CanvasVersionConflict: If all retries exhausted.
        """
        if expected_version is None:
            return self.set_content_atomic(canvas_key, content, None, ttl)

        last_error = None
        for _ in range(max_retries):
            try:
                return self.set_content_atomic(
                    canvas_key, content, expected_version, ttl
                )
            except CanvasVersionConflict as e:
                last_error = e
                expected_version = e.current_version

        raise last_error

    def delete_content(self, canvas_key):
        """Delete canvas content and its version key."""
        version_key = self._version_key(canvas_key)
        self._client.delete(canvas_key, version_key)

    def get_version(self, canvas_key):
        """Get the current version number for a canvas key."""
        return self._get_current_version(self._version_key(canvas_key))

    def refresh_ttl(self, canvas_key, ttl=None):
        """Refresh TTL on both content and version keys."""
        if ttl is None:
            ttl = self._content_ttl
        version_key = self._version_key(canvas_key)
        pipe = self._client.pipeline(transaction=False)
        pipe.expire(canvas_key, ttl)
        pipe.expire(version_key, ttl)
        pipe.execute()
