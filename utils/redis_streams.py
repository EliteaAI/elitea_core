"""Redis Streams producer/consumer for horizontal scaling work events.

Provides exactly-once delivery semantics for work events using Redis Streams
consumer groups. Events classified as 'work' in the event_classification
registry should use Streams (XADD/XREADGROUP) instead of pub/sub broadcast
to ensure only one pod processes each event.

Key concepts:
- StreamProducer: publishes events to a named stream via XADD
- StreamConsumer: reads and acknowledges events via XREADGROUP/XACK
- Consumer groups: multiple pods share a group; each message delivered to one

Redis key layout:
  stream:{stream_name}  — the stream itself (XADD entries)
  Consumer group created on first use with XGROUP CREATE ... MKSTREAM

Retention policy (MAXLEN applied on every XADD, approximate ~ for performance):
  - work streams: 10000 messages (~24h of data at typical throughput)
  - notification streams: 1000 messages (ephemeral, loss acceptable)
  - dlq streams: 50000 messages (keep failures longer for inspection)

Usage:
    producer = StreamProducer(redis_client)
    producer.publish("work:task_distribution", {"task_id": "abc", "type": "predict"})

    consumer = StreamConsumer(redis_client, "work:task_distribution",
                              group="task_workers", consumer="pod-1")
    messages = consumer.consume(count=10, block_ms=5000)
    for msg_id, data in messages:
        process(data)
        consumer.ack(msg_id)
"""

import json
import time

from pylon.core.tools import log


DEFAULT_MAXLEN = 10000
DEFAULT_BLOCK_MS = 5000
DEFAULT_COUNT = 10
STREAM_PREFIX = "stream:"


class StreamProducer:
    """Publishes events to Redis Streams with MAXLEN trimming."""

    def __init__(self, redis_client, maxlen: int = DEFAULT_MAXLEN,
                 approximate_trim: bool = True,
                 use_classification_retention: bool = False):
        """Initialize the stream producer.

        Args:
            redis_client: Redis client instance.
            maxlen: Maximum stream length (trimmed on each publish).
                    Used as default when classification lookup yields nothing.
            approximate_trim: Use approximate (~) trimming for performance.
            use_classification_retention: If True, resolve MAXLEN from
                event_classification.get_stream_retention() on each publish,
                falling back to the maxlen parameter.
        """
        self._client = redis_client
        self._maxlen = maxlen
        self._approximate = approximate_trim
        self._use_classification = use_classification_retention

    def _stream_key(self, stream_name: str) -> str:
        if stream_name.startswith(STREAM_PREFIX):
            return stream_name
        return f"{STREAM_PREFIX}{stream_name}"

    def _resolve_retention(self, stream_name: str) -> int:
        """Resolve MAXLEN from event classification registry."""
        try:
            from .event_classification import get_stream_retention
            return get_stream_retention(stream_name)
        except (ImportError, ValueError):
            pass
        try:
            import sys
            ec = sys.modules.get("event_classification")
            if ec and hasattr(ec, "get_stream_retention"):
                return ec.get_stream_retention(stream_name)
        except Exception:
            pass
        return self._maxlen

    def publish(self, stream_name: str, event_data: dict,
                maxlen: int = None) -> str:
        """Publish an event to a stream.

        Args:
            stream_name: Name of the stream (e.g. "work:task_distribution").
            event_data: Dict payload to publish. Values are JSON-serialized.
            maxlen: Override default maxlen for this publish. Takes precedence
                    over classification-based retention.

        Returns:
            The message ID assigned by Redis (e.g. "1234567890123-0").
        """
        key = self._stream_key(stream_name)
        if maxlen is not None:
            trim_len = maxlen
        elif self._use_classification:
            try:
                trim_len = self._resolve_retention(stream_name)
            except Exception:
                trim_len = self._maxlen
        else:
            trim_len = self._maxlen

        payload = {
            "data": json.dumps(event_data),
            "published_at": str(time.time()),
        }

        msg_id = self._client.xadd(
            key,
            payload,
            maxlen=trim_len,
            approximate=self._approximate,
        )

        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode("utf-8")

        log.info(
            "Published to stream %s: id=%s, keys=%s",
            stream_name, msg_id, list(event_data.keys())
        )
        return msg_id

    def stream_length(self, stream_name: str) -> int:
        """Get the current number of entries in a stream."""
        key = self._stream_key(stream_name)
        return self._client.xlen(key)

    def stream_info(self, stream_name: str) -> dict:
        """Get stream metadata (length, groups, first/last entry)."""
        key = self._stream_key(stream_name)
        try:
            info = self._client.xinfo_stream(key)
            if isinstance(info, dict):
                return info
            return {}
        except Exception:
            return {}


class StreamConsumer:
    """Reads and acknowledges events from Redis Streams consumer groups."""

    def __init__(self, redis_client, stream_name: str, group: str,
                 consumer: str, create_group: bool = True):
        """Initialize the stream consumer.

        Args:
            redis_client: Redis client instance.
            stream_name: Name of the stream to consume from.
            group: Consumer group name (shared across pods).
            consumer: Unique consumer name (typically pod identifier).
            create_group: Auto-create group if it doesn't exist.
        """
        self._client = redis_client
        self._stream_name = stream_name
        self._stream_key = self._make_stream_key(stream_name)
        self._group = group
        self._consumer = consumer

        if create_group:
            self._ensure_group()

    def _make_stream_key(self, stream_name: str) -> str:
        if stream_name.startswith(STREAM_PREFIX):
            return stream_name
        return f"{STREAM_PREFIX}{stream_name}"

    def _ensure_group(self) -> None:
        """Create consumer group if it doesn't exist (idempotent)."""
        try:
            self._client.xgroup_create(
                self._stream_key,
                self._group,
                id="$",
                mkstream=True,
            )
            log.info(
                "Created consumer group '%s' on stream '%s'",
                self._group, self._stream_name
            )
        except Exception as e:
            err_msg = str(e)
            if "BUSYGROUP" in err_msg:
                pass
            else:
                log.warning(
                    "Failed to create group '%s' on '%s': %s",
                    self._group, self._stream_name, err_msg
                )

    def consume(self, count: int = DEFAULT_COUNT,
                block_ms: int = DEFAULT_BLOCK_MS) -> list:
        """Read new messages from the stream.

        Uses XREADGROUP with '>' to get only undelivered messages.

        Args:
            count: Maximum number of messages to read.
            block_ms: Milliseconds to block waiting for new messages.
                      Set to 0 for non-blocking, None for infinite block.

        Returns:
            List of (message_id, data_dict) tuples. Empty list if no messages.
        """
        try:
            response = self._client.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream_key: ">"},
                count=count,
                block=block_ms,
            )
        except Exception as e:
            log.error(
                "XREADGROUP failed on '%s' group '%s': %s",
                self._stream_name, self._group, e
            )
            return []

        if not response:
            return []

        return self._parse_response(response)

    def consume_pending(self, count: int = DEFAULT_COUNT,
                        min_idle_ms: int = 0) -> list:
        """Read pending messages (previously delivered but not ACKed).

        Uses XREADGROUP with '0' to re-read delivered but un-ACKed messages.
        Useful for recovery after consumer restart.

        Args:
            count: Maximum number of messages to read.
            min_idle_ms: Minimum idle time filter (not used in basic read).

        Returns:
            List of (message_id, data_dict) tuples.
        """
        try:
            response = self._client.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream_key: "0"},
                count=count,
                block=0,
            )
        except Exception as e:
            log.error(
                "XREADGROUP pending failed on '%s': %s",
                self._stream_name, e
            )
            return []

        if not response:
            return []

        return self._parse_response(response)

    def ack(self, message_id: str) -> bool:
        """Acknowledge a message as successfully processed.

        Args:
            message_id: The message ID to acknowledge.

        Returns:
            True if acknowledged, False on error.
        """
        try:
            result = self._client.xack(
                self._stream_key, self._group, message_id
            )
            return result > 0
        except Exception as e:
            log.error(
                "XACK failed for %s on '%s': %s",
                message_id, self._stream_name, e
            )
            return False

    def ack_many(self, message_ids: list) -> int:
        """Acknowledge multiple messages at once.

        Args:
            message_ids: List of message IDs to acknowledge.

        Returns:
            Number of messages successfully acknowledged.
        """
        if not message_ids:
            return 0
        try:
            result = self._client.xack(
                self._stream_key, self._group, *message_ids
            )
            return result
        except Exception as e:
            log.error(
                "XACK batch failed on '%s': %s", self._stream_name, e
            )
            return 0

    def pending_count(self) -> int:
        """Get the number of pending (unacked) messages for this group."""
        try:
            info = self._client.xpending(self._stream_key, self._group)
            if isinstance(info, dict):
                return info.get("pending", 0)
            if isinstance(info, (list, tuple)) and len(info) > 0:
                return info[0] if isinstance(info[0], int) else 0
            return 0
        except Exception as e:
            log.error(
                "XPENDING failed on '%s': %s", self._stream_name, e
            )
            return 0

    def pending_summary(self) -> dict:
        """Get pending messages summary for this consumer group.

        Returns:
            Dict with 'pending' count, 'min_id', 'max_id', and 'consumers'.
        """
        try:
            info = self._client.xpending(self._stream_key, self._group)
            if isinstance(info, dict):
                return info
            if isinstance(info, (list, tuple)) and len(info) >= 4:
                return {
                    "pending": info[0],
                    "min_id": _decode(info[1]),
                    "max_id": _decode(info[2]),
                    "consumers": info[3],
                }
            return {"pending": 0, "min_id": None, "max_id": None, "consumers": []}
        except Exception as e:
            log.error(
                "XPENDING summary failed on '%s': %s", self._stream_name, e
            )
            return {"pending": 0, "min_id": None, "max_id": None, "consumers": []}

    def claim_stale(self, min_idle_ms: int, count: int = DEFAULT_COUNT) -> list:
        """Claim messages idle longer than min_idle_ms from other consumers.

        Uses XAUTOCLAIM to take over messages that other consumers failed to
        process (e.g. crashed pods).

        Args:
            min_idle_ms: Minimum idle time in milliseconds.
            count: Maximum number of messages to claim.

        Returns:
            List of (message_id, data_dict) tuples for claimed messages.
        """
        try:
            result = self._client.xautoclaim(
                self._stream_key,
                self._group,
                self._consumer,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count,
            )
        except Exception as e:
            log.error(
                "XAUTOCLAIM failed on '%s': %s", self._stream_name, e
            )
            return []

        if not result:
            return []

        # xautoclaim returns: [next_start_id, [(id, fields), ...], [deleted_ids]]
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            entries = result[1]
            return self._parse_entries(entries)

        return []

    def _parse_response(self, response) -> list:
        """Parse XREADGROUP response into list of (id, data) tuples."""
        messages = []

        # Response format: [[stream_key, [(id, fields), ...]]]
        # or dict format: {stream_key: [(id, fields), ...]}
        if isinstance(response, dict):
            entries = response.get(self._stream_key, [])
            if not entries:
                bkey = self._stream_key.encode("utf-8") if isinstance(
                    self._stream_key, str) else self._stream_key
                entries = response.get(bkey, [])
            return self._parse_entries(entries)

        if isinstance(response, list):
            for item in response:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    entries = item[1]
                    messages.extend(self._parse_entries(entries))

        return messages

    def _parse_entries(self, entries) -> list:
        """Parse stream entries into (id, data_dict) tuples."""
        messages = []
        if not entries:
            return messages

        for entry in entries:
            if not entry or (isinstance(entry, (list, tuple)) and len(entry) < 2):
                continue

            msg_id = entry[0]
            fields = entry[1]

            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            if fields is None:
                continue

            data = self._decode_fields(fields)
            if "data" in data:
                try:
                    data["data"] = json.loads(data["data"])
                except (json.JSONDecodeError, TypeError):
                    pass

            messages.append((msg_id, data))

        return messages

    def _decode_fields(self, fields) -> dict:
        """Decode Redis fields (may be bytes) to string dict."""
        if isinstance(fields, dict):
            result = {}
            for k, v in fields.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else k
                val = v.decode("utf-8") if isinstance(v, bytes) else v
                result[key] = val
            return result
        return {}


def _decode(value):
    """Decode bytes to string if needed."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value
