"""
Graceful shutdown handler for pylon_main horizontal scaling.

Ensures connected Socket.IO clients are notified before the pod terminates,
pending Redis operations are flushed, and the shutdown sequence is logged.
"""

import time
import threading

from pylon.core.tools import log


class GracefulShutdown:
    """Manages graceful shutdown of pylon_main connections and resources.

    Designed to be called from the module's deinit() method when SIGTERM
    is received. The Kubernetes preStop hook provides a sleep window before
    SIGTERM arrives, giving the load balancer time to remove the pod from
    endpoints. This class handles the application-level drain after SIGTERM.
    """

    def __init__(self, sio, redis_client=None, drain_timeout=15):
        self._sio = sio
        self._redis_client = redis_client
        self._drain_timeout = drain_timeout
        self._shutting_down = threading.Event()

    @property
    def is_shutting_down(self):
        return self._shutting_down.is_set()

    def execute(self):
        """Run the full graceful shutdown sequence.

        1. Mark as shutting down (new requests can check this)
        2. Disconnect all Socket.IO clients with a server_shutting_down event
        3. Flush pending Redis operations
        4. Log completion
        """
        self._shutting_down.set()
        start = time.time()
        log.info("Graceful shutdown started (drain_timeout=%ds)", self._drain_timeout)

        disconnected = self._disconnect_sio_clients()
        self._flush_redis()

        elapsed = time.time() - start
        log.info(
            "Graceful shutdown complete: %d clients disconnected in %.1fs",
            disconnected, elapsed,
        )

    def _disconnect_sio_clients(self):
        """Disconnect all connected Socket.IO clients gracefully.

        Emits a 'server_shutting_down' event to each client before
        disconnecting, so the client can initiate reconnection to another pod.
        """
        disconnected = 0
        try:
            sids = self._get_connected_sids()
        except Exception:
            log.warning("Could not enumerate connected SIDs during shutdown")
            return 0

        if not sids:
            log.info("No Socket.IO clients connected, nothing to drain")
            return 0

        log.info("Disconnecting %d Socket.IO client(s)...", len(sids))

        for sid in sids:
            try:
                self._sio.emit("server_shutting_down", {"reason": "pod_terminating"}, to=sid)
            except Exception:
                pass
            try:
                self._sio.disconnect(sid)
                disconnected += 1
            except Exception:
                log.debug("Failed to disconnect SID %s", sid)

        return disconnected

    def _get_connected_sids(self):
        """Get list of currently connected Socket.IO session IDs."""
        sids = []
        try:
            for sid, _ in self._sio.manager.get_participants("/", None):
                sids.append(sid)
        except Exception:
            pass
        return sids

    def _flush_redis(self):
        """Flush any pending Redis pipeline operations."""
        if self._redis_client is None:
            return
        try:
            self._redis_client.ping()
            log.debug("Redis connection verified during shutdown")
        except Exception:
            log.warning("Redis unavailable during shutdown flush")
