"""Event metrics tracking for Redis Streams horizontal scaling.

Tracks per-stream operational metrics in Redis hashes for observability:
- messages_published: total events published to stream
- messages_consumed: total events successfully processed (ACKed)
- messages_failed: total events that failed processing
- pending_count: current unacknowledged messages (lag indicator)
- last_published_at: timestamp of most recent publish
- last_consumed_at: timestamp of most recent consume

Redis key layout:
  metrics:streams:{stream_name}  — hash with all counters for that stream

Provides get_stream_health() for per-stream diagnostics and
get_all_streams_health() for the /health/events endpoint.

Usage:
    metrics = EventMetrics(redis_client)
    metrics.record_published("work:task_distribution")
    metrics.record_consumed("work:task_distribution")
    metrics.record_failed("work:task_distribution")
    metrics.update_pending("work:task_distribution", count=5)

    health = metrics.get_stream_health("work:task_distribution")
    # {"messages_published": 1000, "messages_consumed": 995, ...}

    all_health = metrics.get_all_streams_health()
    # {"work:task_distribution": {...}, "work:voice_events": {...}}
"""

import time

from pylon.core.tools import log


METRICS_KEY_PREFIX = "metrics:streams"
STREAMS_REGISTRY_KEY = "metrics:streams:_registry"
METRICS_TTL = 604800  # 7 days — auto-expire if stream stops producing


class EventMetrics:
    """Tracks per-stream event metrics in Redis hashes."""

    def __init__(self, redis_client):
        """Initialize EventMetrics.

        Args:
            redis_client: Redis client instance.
        """
        self._client = redis_client

    def _metrics_key(self, stream_name: str) -> str:
        return f"{METRICS_KEY_PREFIX}:{stream_name}"

    def _register_stream(self, stream_name: str) -> None:
        """Register a stream in the global registry set (with TTL refresh)."""
        pipe = self._client.pipeline(transaction=False)
        pipe.sadd(STREAMS_REGISTRY_KEY, stream_name)
        pipe.expire(STREAMS_REGISTRY_KEY, METRICS_TTL)
        pipe.execute()

    def record_published(self, stream_name: str, count: int = 1) -> None:
        """Record that messages were published to a stream.

        Args:
            stream_name: Name of the stream.
            count: Number of messages published.
        """
        key = self._metrics_key(stream_name)
        pipe = self._client.pipeline(transaction=False)
        pipe.hincrby(key, "messages_published", count)
        pipe.hset(key, "last_published_at", str(time.time()))
        pipe.expire(key, METRICS_TTL)
        pipe.execute()
        self._register_stream(stream_name)

    def record_consumed(self, stream_name: str, count: int = 1) -> None:
        """Record that messages were successfully consumed (ACKed).

        Args:
            stream_name: Name of the stream.
            count: Number of messages consumed.
        """
        key = self._metrics_key(stream_name)
        pipe = self._client.pipeline(transaction=False)
        pipe.hincrby(key, "messages_consumed", count)
        pipe.hset(key, "last_consumed_at", str(time.time()))
        pipe.expire(key, METRICS_TTL)
        pipe.execute()

    def record_failed(self, stream_name: str, count: int = 1) -> None:
        """Record that messages failed processing.

        Args:
            stream_name: Name of the stream.
            count: Number of messages that failed.
        """
        key = self._metrics_key(stream_name)
        pipe = self._client.pipeline(transaction=False)
        pipe.hincrby(key, "messages_failed", count)
        pipe.hset(key, "last_failed_at", str(time.time()))
        pipe.expire(key, METRICS_TTL)
        pipe.execute()

    def update_pending(self, stream_name: str, count: int) -> None:
        """Update the current pending (lag) count for a stream.

        This is a gauge (set, not increment) — reflects point-in-time state.

        Args:
            stream_name: Name of the stream.
            count: Current number of pending messages.
        """
        key = self._metrics_key(stream_name)
        self._client.hset(key, "pending_count", str(count))

    def get_stream_health(self, stream_name: str) -> dict:
        """Get health metrics for a single stream.

        Args:
            stream_name: Name of the stream.

        Returns:
            Dict with lag, error_rate, throughput, and raw counters.
            Returns empty dict if stream has no metrics recorded.
        """
        key = self._metrics_key(stream_name)
        raw = self._client.hgetall(key)
        if not raw:
            return {}

        data = {}
        for k, v in raw.items():
            field = k.decode("utf-8") if isinstance(k, bytes) else k
            val = v.decode("utf-8") if isinstance(v, bytes) else v
            data[field] = val

        published = int(data.get("messages_published", "0"))
        consumed = int(data.get("messages_consumed", "0"))
        failed = int(data.get("messages_failed", "0"))
        pending = int(data.get("pending_count", "0"))

        total_processed = consumed + failed
        error_rate = (failed / total_processed) if total_processed > 0 else 0.0

        last_published = float(data.get("last_published_at", "0"))
        last_consumed = float(data.get("last_consumed_at", "0"))
        now = time.time()

        publish_age_s = now - last_published if last_published > 0 else None
        consume_age_s = now - last_consumed if last_consumed > 0 else None

        return {
            "stream_name": stream_name,
            "messages_published": published,
            "messages_consumed": consumed,
            "messages_failed": failed,
            "pending_count": pending,
            "error_rate": round(error_rate, 4),
            "last_published_at": last_published if last_published > 0 else None,
            "last_consumed_at": last_consumed if last_consumed > 0 else None,
            "publish_age_seconds": round(publish_age_s, 1) if publish_age_s is not None else None,
            "consume_age_seconds": round(consume_age_s, 1) if consume_age_s is not None else None,
            "status": self._compute_status(pending, error_rate, publish_age_s),
        }

    def get_all_streams_health(self) -> dict:
        """Get health metrics for all registered streams.

        Returns:
            Dict mapping stream_name → health dict.
        """
        members = self._client.smembers(STREAMS_REGISTRY_KEY)
        if not members:
            return {}

        result = {}
        for member in members:
            name = member.decode("utf-8") if isinstance(member, bytes) else member
            health = self.get_stream_health(name)
            if health:
                result[name] = health

        return result

    def get_summary(self) -> dict:
        """Get aggregated summary across all streams.

        Returns:
            Dict with total_streams, total_published, total_consumed,
            total_failed, total_pending, overall_error_rate, streams_unhealthy.
        """
        all_health = self.get_all_streams_health()
        if not all_health:
            return {
                "total_streams": 0,
                "total_published": 0,
                "total_consumed": 0,
                "total_failed": 0,
                "total_pending": 0,
                "overall_error_rate": 0.0,
                "streams_unhealthy": 0,
            }

        total_published = 0
        total_consumed = 0
        total_failed = 0
        total_pending = 0
        unhealthy_count = 0

        for health in all_health.values():
            total_published += health.get("messages_published", 0)
            total_consumed += health.get("messages_consumed", 0)
            total_failed += health.get("messages_failed", 0)
            total_pending += health.get("pending_count", 0)
            if health.get("status") == "unhealthy":
                unhealthy_count += 1

        total_processed = total_consumed + total_failed
        overall_error_rate = (total_failed / total_processed) if total_processed > 0 else 0.0

        return {
            "total_streams": len(all_health),
            "total_published": total_published,
            "total_consumed": total_consumed,
            "total_failed": total_failed,
            "total_pending": total_pending,
            "overall_error_rate": round(overall_error_rate, 4),
            "streams_unhealthy": unhealthy_count,
        }

    def reset_stream(self, stream_name: str) -> None:
        """Reset all metrics for a stream (for testing or maintenance).

        Args:
            stream_name: Name of the stream to reset.
        """
        key = self._metrics_key(stream_name)
        self._client.delete(key)
        self._client.srem(STREAMS_REGISTRY_KEY, stream_name)
        log.info("Reset metrics for stream: %s", stream_name)

    def _compute_status(self, pending: int, error_rate: float,
                        publish_age_s: float = None) -> str:
        """Compute stream health status from metrics.

        Status levels:
        - healthy: normal operation
        - degraded: elevated pending or error rate
        - unhealthy: critical pending backlog or high error rate

        Args:
            pending: Current pending message count.
            error_rate: Ratio of failed / (consumed + failed).
            publish_age_s: Seconds since last publish (None if never published).

        Returns:
            One of "healthy", "degraded", "unhealthy".
        """
        if error_rate >= 0.5:
            return "unhealthy"
        if pending >= 1000:
            return "unhealthy"
        if error_rate >= 0.1:
            return "degraded"
        if pending >= 100:
            return "degraded"
        return "healthy"
