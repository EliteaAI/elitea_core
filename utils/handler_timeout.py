"""Event handler timeout decorator for horizontal scaling.

Prevents event handlers from running indefinitely by enforcing a configurable
timeout. When a handler exceeds its timeout, the decorator:
1. Logs a warning with handler name and elapsed time
2. Raises HandlerTimeoutError (caught by the stream consumer to NACK)
3. Increments a Redis counter for monitoring: metrics:handler_timeouts:{handler_name}

Implementation strategy:
- On Linux/macOS: uses signal.SIGALRM (main thread only, zero overhead)
- Fallback: uses threading.Timer (works in any thread, small thread overhead)

The decorator auto-detects whether signal-based timeout is available and falls
back to threading.Timer transparently.

Usage:
    @timeout(seconds=30)
    def handle_task(event_data):
        ...  # raises HandlerTimeoutError if >30s

    @timeout(seconds=60, redis_client=redis)
    def handle_heavy_task(event_data):
        ...  # also tracks timeout count in Redis

    # With NACK integration (stream consumer pattern):
    try:
        handler(event_data)
    except HandlerTimeoutError:
        consumer.nack(msg_id)  # returns to pending
"""

import functools
import os
import platform
import signal
import threading
import time

from pylon.core.tools import log


DEFAULT_TIMEOUT_SECONDS = 30
METRICS_KEY_PREFIX = "metrics:handler_timeouts"


class HandlerTimeoutError(Exception):
    """Raised when an event handler exceeds its timeout limit."""

    def __init__(self, handler_name: str = "", timeout_seconds: int = 0, elapsed: float = None):
        self.handler_name = handler_name
        self.timeout_seconds = timeout_seconds
        self.elapsed = elapsed if elapsed is not None else float(timeout_seconds)
        msg = (
            f"Handler '{handler_name}' timed out after {self.elapsed:.1f}s "
            f"(limit: {timeout_seconds}s)"
        ) if handler_name else "Handler timed out"
        super().__init__(msg)


def _supports_signal_timeout() -> bool:
    """Check if signal.SIGALRM is available (Unix main thread only)."""
    if platform.system() == "Windows":
        return False
    if not hasattr(signal, "SIGALRM"):
        return False
    try:
        return threading.current_thread() is threading.main_thread()
    except RuntimeError:
        return False


class _SignalTimeout:
    """Context manager using signal.SIGALRM for timeout enforcement."""

    def __init__(self, seconds: int, handler_name: str):
        self._seconds = seconds
        self._handler_name = handler_name
        self._old_handler = None

    def __enter__(self):
        def _alarm_handler(signum, frame):
            raise HandlerTimeoutError(self._handler_name, self._seconds)

        self._old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(self._seconds)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self._old_handler or signal.SIG_DFL)
        return False


class _ThreadTimeoutInterrupt(BaseException):
    """Internal exception injected via ctypes to interrupt thread execution.

    Uses BaseException (not Exception) to avoid being caught by broad
    except clauses in user code. The decorator catches this and re-raises
    as HandlerTimeoutError with full context.
    """
    pass


class _ThreadTimeout:
    """Context manager using threading.Timer for timeout enforcement.

    Works in any thread. Uses ctypes PyThreadState_SetAsyncExc to inject
    a _ThreadTimeoutInterrupt into the target thread. The context manager
    catches it and re-raises as HandlerTimeoutError with full attributes.
    """

    def __init__(self, seconds: int, handler_name: str):
        self._seconds = seconds
        self._handler_name = handler_name
        self._timer = None
        self._target_thread_id = None
        self._timed_out = False
        self._start_time = None

    def __enter__(self):
        self._target_thread_id = threading.current_thread().ident
        self._start_time = time.time()

        def _on_timeout():
            self._timed_out = True
            self._inject_exception()

        self._timer = threading.Timer(self._seconds, _on_timeout)
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        if exc_type is _ThreadTimeoutInterrupt or self._timed_out:
            elapsed = time.time() - self._start_time if self._start_time else self._seconds
            raise HandlerTimeoutError(self._handler_name, self._seconds, elapsed)

        return False

    def _inject_exception(self):
        """Inject _ThreadTimeoutInterrupt in the target thread via ctypes."""
        import ctypes

        thread_id = self._target_thread_id
        if thread_id is None:
            return

        try:
            ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread_id),
                ctypes.py_object(_ThreadTimeoutInterrupt),
            )
            if ret == 0:
                log.warning(
                    "Timeout injection failed: thread %d not found", thread_id
                )
            elif ret > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(thread_id), None
                )
                log.warning(
                    "Timeout injection affected multiple threads, cleared"
                )
        except (SystemError, ValueError, AttributeError):
            pass


class TimeoutTracker:
    """Tracks handler timeout occurrences in Redis for monitoring."""

    def __init__(self, redis_client, key_prefix: str = METRICS_KEY_PREFIX):
        self._client = redis_client
        self._prefix = key_prefix

    def _key(self, handler_name: str) -> str:
        return f"{self._prefix}:{handler_name}"

    def record_timeout(self, handler_name: str, timeout_seconds: int = None) -> int:
        """Increment the timeout counter for a handler.

        Args:
            handler_name: Name of the handler that timed out.
            timeout_seconds: The timeout limit that was exceeded.

        Returns:
            New counter value.
        """
        key = self._key(handler_name)
        count = self._client.incr(key)
        if count == 1:
            self._client.expire(key, 86400 * 7)
        return count

    def get_timeout_count(self, handler_name: str) -> int:
        """Get the current timeout count for a handler.

        Args:
            handler_name: Name of the handler.

        Returns:
            Number of timeouts recorded, 0 if none.
        """
        key = self._key(handler_name)
        value = self._client.get(key)
        return int(value) if value else 0

    def get_all_counts(self) -> dict:
        """Get timeout counts for all handlers with recorded timeouts.

        Returns:
            Dict mapping handler_name -> timeout_count.
        """
        pattern = f"{self._prefix}:*"
        prefix_len = len(self._prefix) + 1
        result = {}

        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match=pattern, count=100)
            if keys:
                pipe = self._client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()

                for key, value in zip(keys, values):
                    if isinstance(key, bytes):
                        key = key.decode("utf-8")
                    handler_name = key[prefix_len:]
                    result[handler_name] = int(value) if value else 0

            if cursor == 0:
                break

        return result

    def reset_count(self, handler_name: str) -> bool:
        """Reset the timeout counter for a handler.

        Args:
            handler_name: Name of the handler.

        Returns:
            True if the counter existed and was deleted.
        """
        key = self._key(handler_name)
        return bool(self._client.delete(key))

    def reset_all(self) -> int:
        """Reset all timeout counters.

        Returns:
            Number of counters deleted.
        """
        pattern = f"{self._prefix}:*"
        deleted = 0
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match=pattern, count=100)
            if keys:
                deleted += self._client.delete(*keys)
            if cursor == 0:
                break
        return deleted


def timeout(seconds: int = DEFAULT_TIMEOUT_SECONDS, redis_client=None,
            use_signal: bool = None, handler_name: str = None):
    """Decorator that enforces a timeout on event handler execution.

    If the handler does not complete within `seconds`, a HandlerTimeoutError
    is raised. The stream consumer should catch this and NACK the message
    (returning it to pending for redelivery).

    Optionally tracks timeout occurrences in Redis for monitoring.

    Args:
        seconds: Maximum execution time in seconds (must be > 0).
        redis_client: Optional Redis client for tracking timeout metrics.
        use_signal: Force signal-based (True) or thread-based (False) timeout.
                    None = auto-detect (signal if available, else thread).
        handler_name: Override the handler name used in logs and metrics.
                      Defaults to the decorated function's __name__.

    Returns:
        Decorator function.

    Raises:
        ValueError: If seconds <= 0.

    Usage:
        @timeout(seconds=30)
        def handle_event(data):
            ...

        @timeout(seconds=60, redis_client=redis)
        def handle_slow_event(data):
            ...
    """
    if seconds <= 0:
        raise ValueError(f"Timeout seconds must be > 0, got {seconds}")

    def decorator(func):
        name = handler_name or func.__name__
        tracker = TimeoutTracker(redis_client) if redis_client else None

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if use_signal is True:
                ctx_class = _SignalTimeout
            elif use_signal is False:
                ctx_class = _ThreadTimeout
            else:
                ctx_class = _SignalTimeout if _supports_signal_timeout() else _ThreadTimeout

            start = time.time()
            try:
                with ctx_class(seconds, name):
                    return func(*args, **kwargs)
            except HandlerTimeoutError as e:
                elapsed = time.time() - start
                e.elapsed = elapsed
                log.warning(
                    "Handler '%s' timed out after %.1fs (limit: %ds)",
                    name, elapsed, seconds
                )
                if tracker:
                    try:
                        tracker.record_timeout(name, seconds)
                    except Exception as metric_err:
                        log.debug(
                            "Failed to record timeout metric for '%s': %s",
                            name, metric_err
                        )
                raise

        wrapper._timeout_seconds = seconds
        wrapper._handler_name = name
        wrapper._timeout_tracker = tracker
        return wrapper

    return decorator
