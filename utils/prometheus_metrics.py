"""Prometheus metrics exposition for pylon_main horizontal scaling.

Exposes custom metrics that drive HPA (Horizontal Pod Autoscaler) decisions:
- pylon_active_connections: current Socket.IO connections on this pod
- pylon_task_queue_depth: pending work items in the task distribution stream

Also exposes operational metrics from the event system and Redis state for
observability dashboards.

The MetricsCollector is a custom Prometheus collector that reads live values
from the pylon runtime on each scrape (pull model). This avoids push-based
stale counters and ensures Prometheus always sees current state.

Usage in Flask/pylon route:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from .prometheus_metrics import MetricsCollector, get_registry

    collector = MetricsCollector(sio_server, redis_client)
    registry = get_registry(collector)

    @route("/metrics")
    def metrics():
        return Response(generate_latest(registry), content_type=CONTENT_TYPE_LATEST)
"""

import time

from prometheus_client.core import (
    GaugeMetricFamily,
    CounterMetricFamily,
    REGISTRY,
)
from prometheus_client.registry import Collector, CollectorRegistry


STREAM_KEY = "stream:work:task_distribution"
METRICS_STREAMS_PREFIX = "metrics:streams"
STREAMS_REGISTRY_KEY = "metrics:streams:_registry"


class MetricsCollector(Collector):
    """Custom Prometheus collector for pylon_main scaling metrics.

    Reads live state from Socket.IO server and Redis on each scrape.
    This ensures metrics are always fresh (no stale counters).
    """

    def __init__(self, sio_server=None, redis_client=None):
        """Initialize the metrics collector.

        Args:
            sio_server: python-socketio Server instance (has manager with rooms/participants).
            redis_client: Redis client instance for reading queue depth and stream metrics.
        """
        self._sio = sio_server
        self._redis = redis_client

    def set_sio_server(self, sio_server):
        """Set or update the Socket.IO server reference (for late binding)."""
        self._sio = sio_server

    def set_redis_client(self, redis_client):
        """Set or update the Redis client reference (for late binding)."""
        self._redis = redis_client

    def collect(self):
        """Yield metric families on each Prometheus scrape."""
        yield from self._collect_connection_metrics()
        yield from self._collect_task_queue_metrics()
        yield from self._collect_stream_metrics()

    def _collect_connection_metrics(self):
        """Yield pylon_active_connections gauge."""
        gauge = GaugeMetricFamily(
            "pylon_active_connections",
            "Current number of active Socket.IO connections on this pod",
            labels=["namespace"],
        )

        count = self._get_active_connections()
        gauge.add_metric(["/"], count)
        yield gauge

    def _collect_task_queue_metrics(self):
        """Yield pylon_task_queue_depth gauge."""
        gauge = GaugeMetricFamily(
            "pylon_task_queue_depth",
            "Number of pending work items in the task distribution stream",
        )

        depth = self._get_task_queue_depth()
        gauge.add_metric([], depth)
        yield gauge

    def _collect_stream_metrics(self):
        """Yield per-stream operational metrics."""
        if not self._redis:
            return

        try:
            members = self._redis.smembers(STREAMS_REGISTRY_KEY)
        except Exception:
            return

        if not members:
            return

        published = CounterMetricFamily(
            "pylon_stream_messages_published_total",
            "Total messages published to each stream",
            labels=["stream"],
        )
        consumed = CounterMetricFamily(
            "pylon_stream_messages_consumed_total",
            "Total messages consumed from each stream",
            labels=["stream"],
        )
        failed = CounterMetricFamily(
            "pylon_stream_messages_failed_total",
            "Total messages that failed processing in each stream",
            labels=["stream"],
        )
        pending = GaugeMetricFamily(
            "pylon_stream_pending_count",
            "Current pending (unacknowledged) messages per stream",
            labels=["stream"],
        )

        for member in members:
            name = member.decode("utf-8") if isinstance(member, bytes) else member
            metrics_key = f"{METRICS_STREAMS_PREFIX}:{name}"
            try:
                raw = self._redis.hgetall(metrics_key)
            except Exception:
                continue
            if not raw:
                continue

            data = {}
            for k, v in raw.items():
                field = k.decode("utf-8") if isinstance(k, bytes) else k
                val = v.decode("utf-8") if isinstance(v, bytes) else v
                data[field] = val

            pub_val = int(data.get("messages_published", "0"))
            con_val = int(data.get("messages_consumed", "0"))
            fail_val = int(data.get("messages_failed", "0"))
            pend_val = int(data.get("pending_count", "0"))

            published.add_metric([name], pub_val)
            consumed.add_metric([name], con_val)
            failed.add_metric([name], fail_val)
            pending.add_metric([name], pend_val)

        yield published
        yield consumed
        yield failed
        yield pending

    def _get_active_connections(self) -> int:
        """Get the number of active Socket.IO connections on this pod.

        Uses the SIO server's internal manager to count connected SIDs
        in the default namespace ('/').
        """
        if not self._sio:
            return 0

        try:
            manager = getattr(self._sio, "manager", None)
            if manager is None:
                return 0

            if hasattr(manager, "get_participants"):
                participants = list(manager.get_participants("/", None))
                return len(participants)

            if hasattr(manager, "rooms") and callable(manager.rooms):
                rooms = manager.rooms("/")
                return len(rooms) if rooms else 0

            if hasattr(manager, "eio") and hasattr(manager.eio, "sockets"):
                return len(manager.eio.sockets)

        except Exception:
            pass

        return 0

    def _get_task_queue_depth(self) -> int:
        """Get the current depth of the task distribution stream."""
        if not self._redis:
            return 0

        try:
            length = self._redis.xlen(STREAM_KEY)
            return length if length else 0
        except Exception:
            return 0


def get_registry(collector: MetricsCollector) -> CollectorRegistry:
    """Create a dedicated CollectorRegistry with the given collector.

    Using a dedicated registry avoids polluting the global REGISTRY
    and prevents double-registration issues in tests.

    Args:
        collector: The MetricsCollector instance to register.

    Returns:
        A CollectorRegistry ready for generate_latest().
    """
    registry = CollectorRegistry()
    registry.register(collector)
    return registry
