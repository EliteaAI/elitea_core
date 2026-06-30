"""Streams monitoring for Redis Streams horizontal scaling.

Periodically scans all active streams for anomalies:
- Pending messages older than a configurable threshold (stuck consumers)
- Consumer groups with no active consumers (orphaned groups)
- Dead letter queue depth exceeding threshold (unresolved failures)

Runs as a leader-only periodic task (every 60s by default) to avoid
duplicate monitoring across pods.

Provides /health/streams endpoint data via get_streams_status().

Redis key layout:
  metrics:streams:_registry  — set of known stream names (from EventMetrics)
  stream:{stream_name}       — the actual stream
  stream:dlq:{stream_name}   — dead letter queue streams

Usage:
    from elitea_core.utils.streams_monitor import StreamsMonitor

    monitor = StreamsMonitor(redis_client)
    anomalies = monitor.check_all()
    # [{"stream": "work:task_distribution", "type": "stuck_consumers", ...}]

    status = monitor.get_streams_status()
    # {"status": "healthy"|"degraded"|"unhealthy", "streams": [...], "anomalies": [...]}
"""

import time

from pylon.core.tools import log

from .redis_streams import STREAM_PREFIX
from .dead_letter_queue import DLQ_STREAM_PREFIX
from .event_metrics import STREAMS_REGISTRY_KEY


DEFAULT_CHECK_INTERVAL_S = 60
DEFAULT_PENDING_AGE_THRESHOLD_MS = 300_000  # 5 minutes
DEFAULT_DLQ_DEPTH_THRESHOLD = 100
DEFAULT_IDLE_CONSUMER_THRESHOLD_MS = 600_000  # 10 minutes


class StreamsMonitor:
    """Monitors Redis Streams health and detects anomalies."""

    def __init__(self, redis_client,
                 pending_age_threshold_ms: int = DEFAULT_PENDING_AGE_THRESHOLD_MS,
                 dlq_depth_threshold: int = DEFAULT_DLQ_DEPTH_THRESHOLD,
                 idle_consumer_threshold_ms: int = DEFAULT_IDLE_CONSUMER_THRESHOLD_MS):
        """Initialize the streams monitor.

        Args:
            redis_client: Redis client instance.
            pending_age_threshold_ms: Alert if pending messages older than this (ms).
            dlq_depth_threshold: Alert if DLQ stream has more entries than this.
            idle_consumer_threshold_ms: Alert if a consumer group has no consumer
                seen within this threshold (ms).
        """
        self._client = redis_client
        self._pending_age_threshold_ms = pending_age_threshold_ms
        self._dlq_depth_threshold = dlq_depth_threshold
        self._idle_consumer_threshold_ms = idle_consumer_threshold_ms

    def _stream_key(self, stream_name: str) -> str:
        if stream_name.startswith(STREAM_PREFIX):
            return stream_name
        return f"{STREAM_PREFIX}{stream_name}"

    def _get_registered_streams(self) -> list:
        """Get all stream names from the metrics registry."""
        members = self._client.smembers(STREAMS_REGISTRY_KEY)
        if not members:
            return []
        result = []
        for m in members:
            name = m.decode("utf-8") if isinstance(m, bytes) else m
            result.append(name)
        return sorted(result)

    def check_stuck_consumers(self, stream_name: str) -> list:
        """Check for pending messages older than threshold in a stream.

        Args:
            stream_name: Name of the stream to check.

        Returns:
            List of anomaly dicts for stuck consumers found.
        """
        key = self._stream_key(stream_name)
        anomalies = []

        try:
            groups_info = self._client.xinfo_groups(key)
        except Exception:
            return anomalies

        if not groups_info:
            return anomalies

        for group_info in groups_info:
            group_name = _decode_field(group_info, "name")
            pending = _int_field(group_info, "pending")

            if pending == 0:
                continue

            stale_messages = self._get_stale_pending(
                key, group_name, self._pending_age_threshold_ms
            )

            if stale_messages > 0:
                anomalies.append({
                    "stream": stream_name,
                    "type": "stuck_consumers",
                    "group": group_name,
                    "stale_pending_count": stale_messages,
                    "threshold_ms": self._pending_age_threshold_ms,
                    "detected_at": time.time(),
                })
                log.warning(
                    "Streams monitor: stuck consumers detected — "
                    "stream=%s, group=%s, stale_pending=%d, threshold=%dms",
                    stream_name, group_name, stale_messages,
                    self._pending_age_threshold_ms,
                )

        return anomalies

    def check_inactive_groups(self, stream_name: str) -> list:
        """Check for consumer groups with no active consumers.

        Args:
            stream_name: Name of the stream to check.

        Returns:
            List of anomaly dicts for inactive groups found.
        """
        key = self._stream_key(stream_name)
        anomalies = []

        try:
            groups_info = self._client.xinfo_groups(key)
        except Exception:
            return anomalies

        if not groups_info:
            return anomalies

        for group_info in groups_info:
            group_name = _decode_field(group_info, "name")
            consumers_count = _int_field(group_info, "consumers")
            last_delivered = _decode_field(group_info, "last-delivered-id")

            if consumers_count == 0 and last_delivered:
                anomalies.append({
                    "stream": stream_name,
                    "type": "no_active_consumers",
                    "group": group_name,
                    "last_delivered_id": last_delivered,
                    "detected_at": time.time(),
                })
                log.warning(
                    "Streams monitor: no active consumers — "
                    "stream=%s, group=%s",
                    stream_name, group_name,
                )

        return anomalies

    def check_dlq_depth(self, stream_name: str) -> list:
        """Check if the DLQ for a stream exceeds the depth threshold.

        Args:
            stream_name: Name of the original stream (not the DLQ).

        Returns:
            List of anomaly dicts if DLQ depth exceeds threshold.
        """
        dlq_name = f"{DLQ_STREAM_PREFIX}{stream_name}"
        dlq_key = self._stream_key(dlq_name)
        anomalies = []

        try:
            length = self._client.xlen(dlq_key)
        except Exception:
            return anomalies

        if length > self._dlq_depth_threshold:
            anomalies.append({
                "stream": stream_name,
                "type": "dlq_depth_exceeded",
                "dlq_stream": dlq_name,
                "dlq_depth": length,
                "threshold": self._dlq_depth_threshold,
                "detected_at": time.time(),
            })
            log.warning(
                "Streams monitor: DLQ depth exceeded — "
                "stream=%s, dlq_depth=%d, threshold=%d",
                stream_name, length, self._dlq_depth_threshold,
            )

        return anomalies

    def check_stream(self, stream_name: str) -> list:
        """Run all checks on a single stream.

        Args:
            stream_name: Name of the stream to check.

        Returns:
            List of all anomalies found for this stream.
        """
        anomalies = []
        anomalies.extend(self.check_stuck_consumers(stream_name))
        anomalies.extend(self.check_inactive_groups(stream_name))
        anomalies.extend(self.check_dlq_depth(stream_name))
        return anomalies

    def check_all(self) -> list:
        """Run all monitoring checks across all registered streams.

        Returns:
            List of all anomalies found across all streams.
        """
        streams = self._get_registered_streams()
        all_anomalies = []

        for stream_name in streams:
            stream_anomalies = self.check_stream(stream_name)
            all_anomalies.extend(stream_anomalies)

        if all_anomalies:
            log.info(
                "Streams monitor: check complete — %d anomalies across %d streams",
                len(all_anomalies), len(streams),
            )
        return all_anomalies

    def get_streams_status(self) -> dict:
        """Get comprehensive streams monitoring status for /health/streams.

        Returns:
            Dict with overall status, per-stream info, and anomalies list.
        """
        streams = self._get_registered_streams()
        anomalies = []
        stream_details = []

        for stream_name in streams:
            detail = self._get_stream_detail(stream_name)
            stream_details.append(detail)
            stream_anomalies = self.check_stream(stream_name)
            anomalies.extend(stream_anomalies)

        status = "healthy"
        if anomalies:
            has_critical = any(
                a["type"] in ("stuck_consumers", "dlq_depth_exceeded")
                for a in anomalies
            )
            status = "unhealthy" if has_critical else "degraded"

        return {
            "status": status,
            "total_streams": len(streams),
            "streams": stream_details,
            "anomalies": anomalies,
            "checked_at": time.time(),
        }

    def _get_stream_detail(self, stream_name: str) -> dict:
        """Get detail info for a single stream."""
        key = self._stream_key(stream_name)
        detail = {
            "name": stream_name,
            "length": 0,
            "groups": [],
        }

        try:
            detail["length"] = self._client.xlen(key)
        except Exception:
            pass

        try:
            groups_info = self._client.xinfo_groups(key)
            if groups_info:
                for g in groups_info:
                    detail["groups"].append({
                        "name": _decode_field(g, "name"),
                        "consumers": _int_field(g, "consumers"),
                        "pending": _int_field(g, "pending"),
                        "last_delivered_id": _decode_field(g, "last-delivered-id"),
                    })
        except Exception:
            pass

        return detail

    def _get_stale_pending(self, stream_key: str, group_name: str,
                           threshold_ms: int) -> int:
        """Count pending messages older than threshold in a consumer group.

        Uses XPENDING with range to inspect delivery timestamps.

        Args:
            stream_key: Full Redis key for the stream.
            group_name: Consumer group name.
            threshold_ms: Age threshold in milliseconds.

        Returns:
            Number of pending messages older than threshold.
        """
        try:
            pending_info = self._client.xpending_range(
                stream_key, group_name,
                min="-", max="+", count=100,
            )
        except Exception:
            return 0

        if not pending_info:
            return 0

        stale_count = 0
        for entry in pending_info:
            idle_ms = _get_idle_time(entry)
            if idle_ms >= threshold_ms:
                stale_count += 1

        return stale_count


def _decode_field(info: dict, field: str) -> str:
    """Decode a field from xinfo response (handles bytes and different formats)."""
    if isinstance(info, dict):
        val = info.get(field) or info.get(field.encode("utf-8"), "")
    elif isinstance(info, (list, tuple)):
        for i in range(0, len(info) - 1, 2):
            k = info[i]
            if isinstance(k, bytes):
                k = k.decode("utf-8")
            if k == field:
                val = info[i + 1]
                if isinstance(val, bytes):
                    return val.decode("utf-8")
                return str(val) if val is not None else ""
        return ""
    else:
        return ""

    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val) if val is not None else ""


def _int_field(info: dict, field: str) -> int:
    """Extract an integer field from xinfo response."""
    raw = _decode_field(info, field)
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 0


def _get_idle_time(entry) -> int:
    """Extract idle time from an XPENDING range entry.

    XPENDING range returns entries as:
      - dict: {"message_id": ..., "consumer": ..., "time_since_delivered": ..., "times_delivered": ...}
      - or list/tuple: [message_id, consumer, idle_ms, delivery_count]
    """
    if isinstance(entry, dict):
        idle = entry.get("time_since_delivered") or entry.get("idle") or 0
        if isinstance(idle, bytes):
            idle = int(idle.decode("utf-8"))
        return int(idle)

    if isinstance(entry, (list, tuple)) and len(entry) >= 3:
        idle = entry[2]
        if isinstance(idle, bytes):
            idle = int(idle.decode("utf-8"))
        return int(idle)

    return 0
