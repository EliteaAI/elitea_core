"""Distributed leader election via Redis for single-leader periodic tasks.

Only one pod per service_name holds the leader lock at any time. The leader
refreshes the lock every `refresh_interval` seconds (default 10s). If the
leader crashes, the lock expires after `ttl` seconds (default 30s) and another
pod acquires it automatically.

Usage:
    from elitea_core.utils.leader_election import LeaderElection, leader_only

    election = LeaderElection(redis_client, service_name="pylon_main")
    election.start()

    # Decorator — function only runs on leader pod
    @leader_only(election)
    def cleanup_stale_sessions():
        ...

    # Manual check
    if election.is_leader:
        run_periodic_aggregation()

    # Shutdown
    election.stop()
"""

import threading
import time
import uuid

from pylon.core.tools import log


DEFAULT_TTL = 30
DEFAULT_REFRESH_INTERVAL = 10
LEADER_KEY_PREFIX = "leader_lock"

# Lua: only extend TTL if our token still holds the lock
_EXTEND_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua: only delete if our token matches (safe release)
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class LeaderElection:
    """Redis-based leader election using SET NX EX with background refresh."""

    def __init__(self, redis_client, service_name: str,
                 ttl: int = DEFAULT_TTL,
                 refresh_interval: int = DEFAULT_REFRESH_INTERVAL,
                 key_prefix: str = LEADER_KEY_PREFIX):
        if not service_name:
            raise ValueError("service_name must be non-empty")
        if ttl < 1:
            raise ValueError("ttl must be >= 1 second")
        if refresh_interval >= ttl:
            raise ValueError("refresh_interval must be less than ttl")

        self._client = redis_client
        self._service_name = service_name
        self._ttl = ttl
        self._refresh_interval = refresh_interval
        self._key = f"{key_prefix}:{service_name}"
        self._token = str(uuid.uuid4())
        self._is_leader = False
        self._leader_lock = threading.Lock()
        self._running = False
        self._stop_event = threading.Event()
        self._thread = None
        self._extend_script = self._client.register_script(_EXTEND_SCRIPT)
        self._release_script = self._client.register_script(_RELEASE_SCRIPT)
        self._on_acquired_callbacks = []
        self._on_lost_callbacks = []

    @property
    def is_leader(self) -> bool:
        """Whether this instance currently holds the leader lock."""
        with self._leader_lock:
            return self._is_leader

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def token(self) -> str:
        return self._token

    def on_acquired(self, callback):
        """Register callback invoked when this instance becomes leader."""
        self._on_acquired_callbacks.append(callback)

    def on_lost(self, callback):
        """Register callback invoked when leadership is lost."""
        self._on_lost_callbacks.append(callback)

    def start(self):
        """Start the leader election background loop."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._election_loop,
            name=f"leader-election-{self._service_name}",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "Leader election started: service=%s, key=%s, ttl=%ds, refresh=%ds",
            self._service_name, self._key, self._ttl, self._refresh_interval,
        )

    def stop(self):
        """Stop the election loop and release leadership if held."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._refresh_interval + 1)
        if self._is_leader:
            self._release_leadership()
        log.info("Leader election stopped: service=%s", self._service_name)

    def try_acquire(self) -> bool:
        """Attempt a single leader lock acquisition."""
        acquired = self._client.set(
            self._key, self._token, nx=True, ex=self._ttl
        )
        if acquired:
            with self._leader_lock:
                was_leader = self._is_leader
                self._is_leader = True
            if not was_leader:
                log.info(
                    "Leadership acquired: service=%s, token=%s",
                    self._service_name, self._token[:8],
                )
                self._fire_acquired()
            return True
        return False

    def refresh(self) -> bool:
        """Refresh the leader lock TTL. Returns False if we lost it."""
        ttl_ms = self._ttl * 1000
        result = self._extend_script(
            keys=[self._key], args=[self._token, str(ttl_ms)]
        )
        if result:
            return True
        with self._leader_lock:
            was_leader = self._is_leader
            self._is_leader = False
        if was_leader:
            log.warning(
                "Leadership lost (refresh failed): service=%s",
                self._service_name,
            )
            self._fire_lost()
        return False

    def get_current_leader(self) -> str:
        """Return the token of the current leader, or empty string if none."""
        val = self._client.get(self._key)
        if val is None:
            return ""
        return val.decode() if isinstance(val, bytes) else str(val)

    def _release_leadership(self):
        """Release the leader lock if we hold it."""
        result = self._release_script(keys=[self._key], args=[self._token])
        with self._leader_lock:
            was_leader = self._is_leader
            self._is_leader = False
        if result:
            log.info("Leadership released: service=%s", self._service_name)
        if was_leader:
            self._fire_lost()

    def _election_loop(self):
        """Background loop: acquire or refresh leadership."""
        while not self._stop_event.is_set():
            try:
                with self._leader_lock:
                    currently_leader = self._is_leader
                if currently_leader:
                    self.refresh()
                else:
                    self.try_acquire()
            except Exception as exc:
                log.error(
                    "Leader election error: service=%s, error=%r",
                    self._service_name, exc,
                )
                with self._leader_lock:
                    was_leader = self._is_leader
                    self._is_leader = False
                if was_leader:
                    self._fire_lost()
            self._stop_event.wait(timeout=self._refresh_interval)

    def _fire_acquired(self):
        for cb in self._on_acquired_callbacks:
            try:
                cb()
            except Exception as exc:
                log.error("on_acquired callback error: %r", exc)

    def _fire_lost(self):
        for cb in self._on_lost_callbacks:
            try:
                cb()
            except Exception as exc:
                log.error("on_lost callback error: %r", exc)


def leader_only(election: LeaderElection):
    """Decorator that skips function execution if not the leader.

    Usage:
        @leader_only(election)
        def my_periodic_task():
            # Only runs on the leader pod
            ...
    """
    import functools

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not election.is_leader:
                log.debug(
                    "Skipping %s: not leader for %s",
                    func.__name__, election.service_name,
                )
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator
