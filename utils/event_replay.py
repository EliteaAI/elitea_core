"""Event replay for Redis Streams horizontal scaling.

Allows replaying historical stream messages for recovery, debugging, or
reprocessing. Reads a range of messages from a stream using XRANGE and
re-publishes them to the same or a different stream/consumer-group.

Supports:
- Full range replay (from_id to to_id)
- Dry-run mode (count without publishing)
- Rate-limiting to avoid overwhelming consumers
- Replay to the same stream (re-trigger) or different target stream

Usage:
    replayer = EventReplay(redis_client)

    # Dry run — see how many messages would be replayed
    stats = replayer.replay_stream(
        "work:task_distribution",
        from_id="0-0",
        to_id="+",
        dry_run=True
    )
    print(stats)  # {"total": 42, "replayed": 0, "skipped": 0, "dry_run": True}

    # Actual replay with rate limiting
    stats = replayer.replay_stream(
        "work:task_distribution",
        from_id="1719500000000-0",
        to_id="1719600000000-0",
        target_stream="work:task_distribution",
        delay_ms=50
    )
"""

import json
import time

from pylon.core.tools import log

from .redis_streams import STREAM_PREFIX, StreamProducer


DEFAULT_BATCH_SIZE = 100
DEFAULT_DELAY_MS = 0
MAX_BATCH_SIZE = 1000


class EventReplay:
    """Replays historical messages from Redis Streams."""

    def __init__(self, redis_client, producer: StreamProducer = None):
        """Initialize the event replayer.

        Args:
            redis_client: Redis client instance.
            producer: Optional StreamProducer instance for publishing.
                      If None, one is created with default settings.
        """
        self._client = redis_client
        self._producer = producer or StreamProducer(redis_client)

    def _stream_key(self, stream_name: str) -> str:
        if stream_name.startswith(STREAM_PREFIX):
            return stream_name
        return f"{STREAM_PREFIX}{stream_name}"

    def replay_stream(self, stream_name: str, from_id: str = "0-0",
                      to_id: str = "+", target_stream: str = None,
                      dry_run: bool = False, delay_ms: int = DEFAULT_DELAY_MS,
                      batch_size: int = DEFAULT_BATCH_SIZE,
                      max_messages: int = 10000) -> dict:
        """Replay messages from a stream within a given ID range.

        Reads messages using XRANGE and re-publishes them to a target stream.
        Messages are replayed in order, optionally rate-limited.

        Args:
            stream_name: Source stream name to read from.
            from_id: Start message ID (inclusive). Default "0-0" for oldest.
            to_id: End message ID (inclusive). Default "+" for newest.
            target_stream: Where to publish replayed messages. If None,
                          re-publishes to the same stream_name.
            dry_run: If True, count messages without publishing.
            delay_ms: Milliseconds to wait between each replayed message.
                     Set to 0 for no delay.
            batch_size: Number of messages to read per XRANGE call.
            max_messages: Stop after this many messages (default 10000, must be > 0).

        Returns:
            Dict with replay statistics:
                - total: messages found in the range
                - replayed: messages successfully re-published
                - failed: messages that failed to publish
                - skipped: messages skipped (dry_run mode)
                - dry_run: whether this was a dry run
                - from_id: starting ID used
                - to_id: ending ID used
                - source_stream: stream read from
                - target_stream: stream published to
        """
        source_key = self._stream_key(stream_name)
        target = target_stream if target_stream else stream_name
        effective_batch = min(batch_size, MAX_BATCH_SIZE)
        delay_seconds = delay_ms / 1000.0 if delay_ms > 0 else 0

        stats = {
            "total": 0,
            "replayed": 0,
            "failed": 0,
            "skipped": 0,
            "dry_run": dry_run,
            "from_id": from_id,
            "to_id": to_id,
            "source_stream": stream_name,
            "target_stream": target,
        }

        current_start = from_id

        while True:
            try:
                entries = self._client.xrange(
                    source_key,
                    min=current_start,
                    max=to_id,
                    count=effective_batch
                )
            except Exception as e:
                log.error(
                    "XRANGE failed on '%s' (start=%s): %s",
                    stream_name, current_start, e
                )
                break

            if not entries:
                break

            for msg_id, fields in entries:
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode("utf-8")

                stats["total"] += 1

                if max_messages > 0 and stats["total"] > max_messages:
                    stats["total"] -= 1
                    return stats

                if dry_run:
                    stats["skipped"] += 1
                    continue

                success = self._republish_message(
                    target, msg_id, fields
                )
                if success:
                    stats["replayed"] += 1
                else:
                    stats["failed"] += 1

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            last_id = entries[-1][0]
            if isinstance(last_id, bytes):
                last_id = last_id.decode("utf-8")

            current_start = self._increment_id(last_id)

            if current_start is None:
                break

            if max_messages > 0 and stats["total"] >= max_messages:
                break

        log.info(
            "Replay complete: stream=%s, total=%d, replayed=%d, failed=%d, "
            "dry_run=%s",
            stream_name, stats["total"], stats["replayed"],
            stats["failed"], dry_run
        )

        return stats

    def count_messages(self, stream_name: str, from_id: str = "0-0",
                       to_id: str = "+") -> int:
        """Count messages in a stream range without replaying.

        Equivalent to replay_stream(..., dry_run=True) but returns just the count.

        Args:
            stream_name: Stream to count messages in.
            from_id: Start message ID (inclusive).
            to_id: End message ID (inclusive).

        Returns:
            Number of messages in the range.
        """
        result = self.replay_stream(
            stream_name, from_id=from_id, to_id=to_id, dry_run=True
        )
        return result["total"]

    def replay_single(self, stream_name: str, message_id: str,
                      target_stream: str = None) -> bool:
        """Replay a single message by its ID.

        Args:
            stream_name: Source stream containing the message.
            message_id: The specific message ID to replay.
            target_stream: Target stream. If None, uses source stream.

        Returns:
            True if successfully replayed, False otherwise.
        """
        source_key = self._stream_key(stream_name)
        target = target_stream if target_stream else stream_name

        try:
            entries = self._client.xrange(
                source_key, min=message_id, max=message_id, count=1
            )
        except Exception as e:
            log.error(
                "Failed to read message %s from '%s': %s",
                message_id, stream_name, e
            )
            return False

        if not entries:
            log.warning(
                "Message %s not found in stream '%s'",
                message_id, stream_name
            )
            return False

        msg_id, fields = entries[0]
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode("utf-8")

        return self._republish_message(target, msg_id, fields)

    def _republish_message(self, target_stream: str, original_msg_id: str,
                           fields) -> bool:
        """Re-publish a message to the target stream.

        Preserves original data and adds replay metadata.

        Args:
            target_stream: Stream to publish to.
            original_msg_id: Original message ID (added as metadata).
            fields: Raw Redis stream entry fields.

        Returns:
            True if published successfully, False on error.
        """
        decoded = self._decode_fields(fields)

        data_raw = decoded.get("data", "{}")
        try:
            original_data = json.loads(data_raw)
        except (json.JSONDecodeError, TypeError):
            original_data = {"raw": data_raw}

        if not isinstance(original_data, dict):
            original_data = {"raw": str(original_data)}

        original_data["_replayed_from"] = original_msg_id
        original_data["_replayed_at"] = time.time()

        try:
            self._producer.publish(target_stream, original_data)
            return True
        except Exception as e:
            log.error(
                "Failed to republish message %s to '%s': %s",
                original_msg_id, target_stream, e
            )
            return False

    def _increment_id(self, msg_id: str) -> str:
        """Increment a Redis stream message ID for pagination.

        Redis stream IDs are formatted as "<timestamp>-<sequence>".
        To paginate XRANGE, we increment the sequence number.

        Args:
            msg_id: Current message ID (e.g. "1719500000000-5").

        Returns:
            Next ID string, or None if parsing fails.
        """
        parts = msg_id.split("-")
        if len(parts) != 2:
            return None
        try:
            timestamp = parts[0]
            sequence = int(parts[1]) + 1
            return f"{timestamp}-{sequence}"
        except (ValueError, IndexError):
            return None

    def _decode_fields(self, fields) -> dict:
        """Decode Redis hash fields (may contain bytes)."""
        if isinstance(fields, dict):
            result = {}
            for k, v in fields.items():
                key = k.decode("utf-8") if isinstance(k, bytes) else k
                val = v.decode("utf-8") if isinstance(v, bytes) else v
                result[key] = val
            return result
        return {}
