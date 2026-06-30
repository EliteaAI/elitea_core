"""
Disconnect cleanup handler for horizontal scaling.

When a Socket.IO client disconnects, this module publishes a cleanup event to
Redis pub/sub. A subscriber on each pod watches for these events and, after a
configurable grace period, executes cleanup actions if the user has not
reconnected.

Cleanup actions:
- Release distributed locks held by the disconnected session
- Remove MCP server registrations (already done synchronously in sio_disconnect)
- Mark ASR sessions as abandoned

The grace period prevents premature cleanup during transient disconnects (e.g.
pod rolling update, brief network blip). If the client reconnects within the
grace period, the pending cleanup is cancelled.

Redis keys used:
  disconnect_pending:{sid}  — string "1" with TTL = grace_period_seconds
  Channel: sio_disconnect_cleanup — pub/sub for disconnect events
"""

import json
import threading
import time

from pylon.core.tools import log


CHANNEL_NAME = "sio_disconnect_cleanup"
PENDING_KEY_PREFIX = "disconnect_pending"
DEFAULT_GRACE_PERIOD = 60


class DisconnectCleanup:
    """Manages disconnect cleanup with grace period across pods.

    On disconnect: publishes event + sets a pending key with TTL.
    On reconnect: deletes pending key (cancels cleanup).
    Background subscriber: after grace period, checks pending key and runs cleanup.
    """

    def __init__(self, redis_client, grace_period: int = DEFAULT_GRACE_PERIOD,
                 cleanup_callbacks=None):
        """
        Args:
            redis_client: Redis client instance (from module.get_redis_client()).
            grace_period: Seconds to wait before executing cleanup after disconnect.
            cleanup_callbacks: List of callables, each taking (sid, disconnect_info).
                             Called sequentially after grace period expires.
        """
        self._client = redis_client
        self._grace_period = grace_period
        self._callbacks = cleanup_callbacks or []
        self._subscriber_thread = None
        self._running = False
        self._pending_timers = {}
        self._timers_lock = threading.Lock()

    def _pending_key(self, sid: str) -> str:
        return f"{PENDING_KEY_PREFIX}:{sid}"

    def publish_disconnect(self, sid: str, metadata: dict = None):
        """Publish a disconnect event and set the pending cleanup key.

        Called from the sio_disconnect handler on the pod where disconnect occurred.

        Args:
            sid: Socket.IO session ID of the disconnected client.
            metadata: Optional dict with context (project_id, user_id, etc.).
        """
        pending_key = self._pending_key(sid)
        info = {
            "sid": sid,
            "timestamp": time.time(),
            "metadata": metadata or {},
        }
        info_json = json.dumps(info)

        pipe = self._client.pipeline(transaction=False)
        pipe.set(pending_key, info_json, ex=self._grace_period)
        pipe.publish(CHANNEL_NAME, info_json)
        pipe.execute()

        log.info("Disconnect cleanup scheduled for SID %s (grace=%ds)", sid, self._grace_period)

    def cancel_cleanup(self, sid: str) -> bool:
        """Cancel pending cleanup for a reconnected session.

        Called when a client reconnects (in the connect handler).

        Args:
            sid: Socket.IO session ID that reconnected.

        Returns:
            True if a pending cleanup was cancelled, False if none was pending.
        """
        pending_key = self._pending_key(sid)
        deleted = self._client.delete(pending_key)

        with self._timers_lock:
            timer = self._pending_timers.pop(sid, None)
            if timer is not None:
                timer.cancel()

        if deleted:
            log.info("Disconnect cleanup cancelled for SID %s (reconnected)", sid)
            return True
        return False

    def start_subscriber(self):
        """Start the background subscriber thread.

        Should be called once during module initialization. The subscriber
        listens for disconnect events and schedules deferred cleanup.
        """
        if self._running:
            return

        self._running = True
        self._subscriber_thread = threading.Thread(
            target=self._subscriber_loop,
            daemon=True,
            name="disconnect_cleanup_subscriber",
        )
        self._subscriber_thread.start()
        log.info("Disconnect cleanup subscriber started (grace_period=%ds)", self._grace_period)

    def stop_subscriber(self):
        """Stop the background subscriber thread."""
        self._running = False

        with self._timers_lock:
            for timer in self._pending_timers.values():
                timer.cancel()
            self._pending_timers.clear()

        if self._subscriber_thread is not None:
            self._subscriber_thread.join(timeout=5)
            self._subscriber_thread = None
        log.info("Disconnect cleanup subscriber stopped")

    def _subscriber_loop(self):
        """Background thread: subscribe to disconnect events and schedule cleanup."""
        while self._running:
            try:
                pubsub = self._client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(CHANNEL_NAME)

                for message in pubsub.listen():
                    if not self._running:
                        break
                    if message is None or message.get("type") != "message":
                        continue

                    try:
                        data_raw = message["data"]
                        if isinstance(data_raw, bytes):
                            data_raw = data_raw.decode("utf-8")
                        disconnect_info = json.loads(data_raw)
                        self._schedule_deferred_cleanup(disconnect_info)
                    except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
                        log.warning("Invalid disconnect cleanup message: %s", e)

                pubsub.unsubscribe()
                pubsub.close()

            except Exception as e:
                if self._running:
                    log.error("Disconnect cleanup subscriber error: %s, retrying in 3s", e)
                    time.sleep(3)

    def _schedule_deferred_cleanup(self, disconnect_info: dict):
        """Schedule cleanup after grace period for a disconnect event."""
        sid = disconnect_info.get("sid")
        if not sid:
            return

        with self._timers_lock:
            existing = self._pending_timers.get(sid)
            if existing is not None:
                existing.cancel()

            timer = threading.Timer(
                self._grace_period,
                self._execute_cleanup,
                args=(sid, disconnect_info),
            )
            timer.daemon = True
            timer.name = f"cleanup_timer_{sid}"
            self._pending_timers[sid] = timer
            timer.start()

    def _execute_cleanup(self, sid: str, disconnect_info: dict):
        """Execute cleanup if the pending key still exists (user didn't reconnect)."""
        with self._timers_lock:
            self._pending_timers.pop(sid, None)

        pending_key = self._pending_key(sid)
        still_pending = self._client.get(pending_key)

        if not still_pending:
            log.debug("Cleanup for SID %s cancelled (key expired or reconnected)", sid)
            return

        self._client.delete(pending_key)
        log.info("Executing disconnect cleanup for SID %s", sid)

        for callback in self._callbacks:
            try:
                callback(sid, disconnect_info)
            except Exception as e:
                log.error("Disconnect cleanup callback failed for SID %s: %s", sid, e)

    def add_callback(self, callback):
        """Register an additional cleanup callback.

        Args:
            callback: Callable(sid: str, disconnect_info: dict).
        """
        self._callbacks.append(callback)

    @property
    def grace_period(self) -> int:
        return self._grace_period

    @property
    def is_running(self) -> bool:
        return self._running
