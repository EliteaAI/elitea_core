"""Dead Letter Queue (DLQ) for Redis Streams horizontal scaling.

Provides a reusable DLQ implementation for any Redis Stream consumer.
When a stream consumer exhausts its retry budget (default 3 attempts),
the message is moved to a corresponding DLQ stream for later inspection,
retry, or permanent discard.

DLQ stream naming: dlq:{original_stream_name}
  e.g. dlq:work:task_distribution

Each DLQ entry stores:
  - original_msg_id: the message ID from the source stream
  - original_stream: source stream name
  - data: the original message payload (JSON)
  - error: error description from last failure
  - retry_count: how many times processing was attempted
  - failed_at: Unix timestamp when the message was moved to DLQ
  - consumer: which consumer last attempted processing

Usage:
    dlq = DeadLetterQueue(redis_client)

    # Send a failed message to DLQ
    dlq.send("work:task_distribution", msg_id, original_data, "timeout", retry_count=3)

    # Inspect failures
    failed = dlq.list_failed("work:task_distribution", limit=50)

    # Retry a specific message (re-publishes to original stream)
    dlq.retry("work:task_distribution", dlq_message_id)

    # Permanently discard
    dlq.discard("work:task_distribution", dlq_message_id)
"""

import json
import time

from pylon.core.tools import log

from .redis_streams import StreamProducer, STREAM_PREFIX


DLQ_STREAM_PREFIX = "dlq:"
DLQ_MAXLEN = 50000
DEFAULT_LIST_LIMIT = 100


class DeadLetterQueue:
    """Manages dead letter queues for Redis Streams."""

    def __init__(self, redis_client, maxlen: int = DLQ_MAXLEN):
        """Initialize the DLQ manager.

        Args:
            redis_client: Redis client instance.
            maxlen: Maximum entries per DLQ stream (prevents unbounded growth).
        """
        self._client = redis_client
        self._maxlen = maxlen
        self._producer = StreamProducer(
            redis_client, maxlen=maxlen, approximate_trim=True
        )

    def _dlq_stream_key(self, original_stream: str) -> str:
        """Get the full Redis key for a DLQ stream."""
        dlq_name = f"{DLQ_STREAM_PREFIX}{original_stream}"
        if dlq_name.startswith(STREAM_PREFIX):
            return dlq_name
        return f"{STREAM_PREFIX}{dlq_name}"

    def _dlq_stream_name(self, original_stream: str) -> str:
        """Get the DLQ stream name (used for publish via StreamProducer)."""
        return f"{DLQ_STREAM_PREFIX}{original_stream}"

    def send(self, original_stream: str, original_msg_id: str,
             data: dict, error: str, retry_count: int = 0,
             consumer: str = "") -> str:
        """Move a failed message to the dead letter queue.

        Args:
            original_stream: The source stream name (without stream: prefix).
            original_msg_id: Message ID from the original stream.
            data: Original message payload.
            error: Error description from the last processing attempt.
            retry_count: Number of times processing was attempted.
            consumer: Name of the consumer that last attempted processing.

        Returns:
            The DLQ message ID assigned by Redis.
        """
        dlq_name = self._dlq_stream_name(original_stream)
        dlq_entry = {
            "original_msg_id": original_msg_id,
            "original_stream": original_stream,
            "data": data if isinstance(data, dict) else {"raw": str(data)},
            "error": str(error),
            "retry_count": retry_count,
            "consumer": consumer,
            "failed_at": time.time(),
        }

        try:
            msg_id = self._producer.publish(dlq_name, dlq_entry)
            log.info(
                "Message sent to DLQ: stream=%s, original_id=%s, error=%s",
                dlq_name, original_msg_id, error
            )
            return msg_id
        except Exception:
            log.exception(
                "Failed to send message %s to DLQ stream %s",
                original_msg_id, dlq_name
            )
            return ""

    def list_failed(self, original_stream: str, limit: int = DEFAULT_LIST_LIMIT,
                    start: str = "-", end: str = "+") -> list:
        """List failed messages in a DLQ stream.

        Args:
            original_stream: The source stream name.
            limit: Maximum number of entries to return.
            start: Start ID for range query (default: oldest).
            end: End ID for range query (default: newest).

        Returns:
            List of dicts, each containing:
                - dlq_msg_id: the message ID in the DLQ stream
                - original_msg_id: ID from the source stream
                - original_stream: source stream name
                - data: original payload
                - error: failure reason
                - retry_count: attempts made
                - consumer: last consumer that tried
                - failed_at: Unix timestamp
        """
        key = self._dlq_stream_key(original_stream)
        try:
            entries = self._client.xrange(key, min=start, max=end, count=limit)
        except Exception as e:
            log.error("Failed to list DLQ entries for %s: %s", original_stream, e)
            return []

        if not entries:
            return []

        results = []
        for msg_id, fields in entries:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")
            parsed = self._parse_dlq_entry(msg_id, fields)
            results.append(parsed)

        return results

    def count(self, original_stream: str) -> int:
        """Get the number of messages in a DLQ stream.

        Args:
            original_stream: The source stream name.

        Returns:
            Number of entries in the DLQ stream.
        """
        key = self._dlq_stream_key(original_stream)
        try:
            return self._client.xlen(key)
        except Exception as e:
            log.error("Failed to get DLQ length for %s: %s", original_stream, e)
            return 0

    def retry(self, original_stream: str, dlq_message_id: str) -> bool:
        """Re-enqueue a DLQ message back to its original stream.

        Reads the message from the DLQ, re-publishes to the original stream,
        then deletes from the DLQ.

        Args:
            original_stream: The source stream name.
            dlq_message_id: The message ID in the DLQ stream to retry.

        Returns:
            True if successfully re-enqueued, False on error.
        """
        key = self._dlq_stream_key(original_stream)

        try:
            entries = self._client.xrange(key, min=dlq_message_id,
                                          max=dlq_message_id, count=1)
        except Exception as e:
            log.error("Failed to read DLQ message %s: %s", dlq_message_id, e)
            return False

        if not entries:
            log.warning(
                "DLQ message %s not found in stream %s",
                dlq_message_id, original_stream
            )
            return False

        _, fields = entries[0]
        parsed = self._parse_dlq_entry(dlq_message_id, fields)
        original_data = parsed.get("data", {})

        try:
            retry_payload = dict(original_data) if isinstance(original_data, dict) else {"raw": str(original_data)}
            retry_payload["_retried_from_dlq"] = dlq_message_id
            original_producer = StreamProducer(self._client)
            new_msg_id = original_producer.publish(original_stream, retry_payload)
        except Exception as e:
            log.error(
                "Failed to re-publish DLQ message %s to %s: %s",
                dlq_message_id, original_stream, e
            )
            return False

        try:
            self._client.xdel(key, dlq_message_id)
        except Exception as e:
            log.warning(
                "Re-published but failed to delete from DLQ: %s (msg still in DLQ): %s",
                dlq_message_id, e
            )

        log.info(
            "DLQ message retried: dlq_id=%s → new_id=%s in stream %s",
            dlq_message_id, new_msg_id, original_stream
        )
        return True

    def retry_all(self, original_stream: str, limit: int = DEFAULT_LIST_LIMIT) -> int:
        """Re-enqueue all DLQ messages back to the original stream.

        Args:
            original_stream: The source stream name.
            limit: Maximum number of messages to retry in one call.

        Returns:
            Number of messages successfully re-enqueued.
        """
        messages = self.list_failed(original_stream, limit=limit)
        retried = 0
        for msg in messages:
            dlq_id = msg.get("dlq_msg_id")
            if dlq_id and self.retry(original_stream, dlq_id):
                retried += 1
        return retried

    def discard(self, original_stream: str, dlq_message_id: str) -> bool:
        """Permanently remove a message from the DLQ.

        Args:
            original_stream: The source stream name.
            dlq_message_id: The message ID in the DLQ stream to discard.

        Returns:
            True if deleted, False on error.
        """
        key = self._dlq_stream_key(original_stream)
        try:
            result = self._client.xdel(key, dlq_message_id)
            if result > 0:
                log.info(
                    "DLQ message discarded: id=%s from stream %s",
                    dlq_message_id, original_stream
                )
                return True
            log.warning(
                "DLQ message %s not found for discard in %s",
                dlq_message_id, original_stream
            )
            return False
        except Exception as e:
            log.error(
                "Failed to discard DLQ message %s: %s", dlq_message_id, e
            )
            return False

    def discard_all(self, original_stream: str, batch_size: int = 500) -> int:
        """Remove all messages from a DLQ stream in batches.

        Args:
            original_stream: The source stream name.
            batch_size: Number of IDs to delete per XDEL call.

        Returns:
            Number of messages discarded.
        """
        key = self._dlq_stream_key(original_stream)
        total_deleted = 0
        try:
            while True:
                entries = self._client.xrange(key, count=batch_size)
                if not entries:
                    break
                ids = []
                for msg_id, _ in entries:
                    if isinstance(msg_id, bytes):
                        msg_id = msg_id.decode("utf-8")
                    ids.append(msg_id)
                if ids:
                    deleted = self._client.xdel(key, *ids)
                    total_deleted += deleted
                else:
                    break
            if total_deleted > 0:
                log.info(
                    "Discarded %d messages from DLQ stream %s", total_deleted, key
                )
            return total_deleted
        except Exception as e:
            log.error("Failed to discard all from DLQ %s: %s", key, e)
            return total_deleted

    def get_message(self, original_stream: str, dlq_message_id: str) -> dict:
        """Get a specific DLQ message by ID.

        Args:
            original_stream: The source stream name.
            dlq_message_id: The message ID to retrieve.

        Returns:
            Parsed DLQ entry dict, or empty dict if not found.
        """
        key = self._dlq_stream_key(original_stream)
        try:
            entries = self._client.xrange(key, min=dlq_message_id,
                                          max=dlq_message_id, count=1)
        except Exception as e:
            log.error("Failed to get DLQ message %s: %s", dlq_message_id, e)
            return {}

        if not entries:
            return {}

        msg_id, fields = entries[0]
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode("utf-8")
        return self._parse_dlq_entry(msg_id, fields)

    def _parse_dlq_entry(self, msg_id: str, fields) -> dict:
        """Parse raw Redis stream fields into a structured DLQ entry.

        Redis stores entries as produced by StreamProducer.publish(), which wraps
        the payload: {"data": json.dumps(dlq_entry), "published_at": "..."}
        So the actual DLQ entry is JSON-encoded inside the "data" field.
        """
        decoded = self._decode_fields(fields)

        data_raw = decoded.get("data", "{}")
        try:
            envelope = json.loads(data_raw)
        except (json.JSONDecodeError, TypeError):
            envelope = {}

        if not isinstance(envelope, dict):
            envelope = {}

        original_data = envelope.get("data", {})
        if isinstance(original_data, str):
            try:
                original_data = json.loads(original_data)
            except (json.JSONDecodeError, TypeError):
                pass

        failed_at_raw = envelope.get("failed_at", "0")
        try:
            failed_at_val = float(failed_at_raw)
        except (ValueError, TypeError):
            failed_at_val = 0.0

        retry_count_raw = envelope.get("retry_count", "0")
        try:
            retry_count_val = int(retry_count_raw)
        except (ValueError, TypeError):
            retry_count_val = 0

        return {
            "dlq_msg_id": msg_id,
            "original_msg_id": envelope.get("original_msg_id", ""),
            "original_stream": envelope.get("original_stream", ""),
            "data": original_data,
            "error": envelope.get("error", ""),
            "retry_count": retry_count_val,
            "consumer": envelope.get("consumer", ""),
            "failed_at": failed_at_val,
        }

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
