"""Stream-based task distribution for horizontal scaling.

Replaces the broadcast pub/sub pattern for task distribution with Redis Streams
consumer groups. Each task request is published once to a stream and consumed
by exactly one worker pod, eliminating wasted broadcasts and duplicate processing.

Architecture:
  pylon_main (producer) --XADD--> stream:work:task_distribution --> consumer group
  pylon_indexer pod-1 (consumer) --XREADGROUP--> claims task
  pylon_indexer pod-2 (consumer) --XREADGROUP--> (idle, nothing to claim)

Stream key: stream:work:task_distribution
Consumer group: task_workers
Consumer name: {pod_identifier} (unique per pod, from HOSTNAME or uuid)

The distributor integrates with the existing arbiter TaskNode:
- Producer side (pylon_main): publishes task requests to the stream
- Consumer side (pylon_indexer): reads tasks from stream, invokes local TaskNode

Feature flag: REDIS_STREAMS_ENABLED (disabled by default, Phase 4 rollout)

Failure handling:
- If consumer crashes: message stays pending, claimed by another consumer
  after min_idle_ms (default 60s) via XAUTOCLAIM
- If processing fails: message is NACKed (not acked), retried up to max_retries
- After max_retries: message moved to DLQ stream (dlq:work:task_distribution)
"""

import json
import os
import threading
import time
import uuid

from pylon.core.tools import log

from .redis_streams import StreamProducer, StreamConsumer


STREAM_NAME = "work:task_distribution"
CONSUMER_GROUP = "task_workers"
DEFAULT_MAX_RETRIES = 3
DEFAULT_CLAIM_IDLE_MS = 60_000
DEFAULT_POLL_INTERVAL_MS = 5000
DEFAULT_POLL_COUNT = 10
DLQ_PREFIX = "dlq:"


def _get_consumer_name() -> str:
    hostname = os.environ.get("HOSTNAME", "")
    if hostname:
        return hostname
    return f"consumer-{uuid.uuid4().hex[:8]}"


class TaskDistributionProducer:
    """Publishes task distribution requests to a Redis Stream.

    Used by pylon_main to submit task requests. Each published message
    contains the full task specification that a worker needs to execute.
    """

    def __init__(self, redis_client, stream_name: str = STREAM_NAME,
                 maxlen: int = 10000):
        self._producer = StreamProducer(redis_client, maxlen=maxlen)
        self._stream_name = stream_name

    def submit_task(self, task_name: str, args: list = None,
                    kwargs: dict = None, pool: str = None,
                    meta: dict = None, task_id: str = None) -> str:
        """Submit a task for distribution to a worker pod.

        Args:
            task_name: Name of the registered task function.
            args: Positional arguments for the task.
            kwargs: Keyword arguments for the task.
            pool: TaskNode pool to target (e.g. "indexer", "agents").
            meta: Task metadata (project_id, user_context, etc).
            task_id: Optional pre-generated task ID. Generated if not provided.

        Returns:
            The stream message ID (can be used to track delivery).
        """
        if task_id is None:
            task_id = str(uuid.uuid4())

        event_data = {
            "task_id": task_id,
            "task_name": task_name,
            "args": args or [],
            "kwargs": kwargs or {},
            "pool": pool,
            "meta": meta or {},
            "submitted_at": time.time(),
        }

        msg_id = self._producer.publish(self._stream_name, event_data)
        log.info(
            "Task submitted to stream: task_id=%s, name=%s, pool=%s, msg_id=%s",
            task_id, task_name, pool, msg_id
        )
        return msg_id

    def stream_depth(self) -> int:
        """Get the current number of pending tasks in the stream."""
        return self._producer.stream_length(self._stream_name)


class TaskDistributionConsumer:
    """Consumes task distribution requests from a Redis Stream.

    Used by pylon_indexer pods to claim and execute tasks. Each pod runs
    a consumer loop that claims tasks from the shared consumer group.
    Only one pod receives each task (exactly-once delivery via XREADGROUP).
    """

    def __init__(self, redis_client, task_handler,
                 stream_name: str = STREAM_NAME,
                 group: str = CONSUMER_GROUP,
                 consumer: str = None,
                 max_retries: int = DEFAULT_MAX_RETRIES,
                 claim_idle_ms: int = DEFAULT_CLAIM_IDLE_MS,
                 poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS,
                 poll_count: int = DEFAULT_POLL_COUNT):
        """Initialize the task distribution consumer.

        Args:
            redis_client: Redis client instance.
            task_handler: Callable(task_data: dict) -> bool.
                          Returns True on success, False on failure.
            stream_name: Stream to consume from.
            group: Consumer group name.
            consumer: Unique consumer identifier (default: hostname).
            max_retries: Max retries before sending to DLQ.
            claim_idle_ms: Idle time before claiming abandoned messages.
            poll_interval_ms: How long to block waiting for messages.
            poll_count: Max messages to read per poll cycle.
        """
        self._client = redis_client
        self._task_handler = task_handler
        self._stream_name = stream_name
        self._group = group
        self._consumer_name = consumer or _get_consumer_name()
        self._max_retries = max_retries
        self._claim_idle_ms = claim_idle_ms
        self._poll_interval_ms = poll_interval_ms
        self._poll_count = poll_count
        self._stop_event = threading.Event()
        self._thread = None
        self._retry_counts: dict = {}

        self._stream_consumer = StreamConsumer(
            redis_client, stream_name, group, self._consumer_name,
            create_group=True
        )

        self._dlq_producer = StreamProducer(
            redis_client, maxlen=50000, approximate_trim=True
        )

    def start(self) -> None:
        """Start the consumer loop in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True,
            name=f"task-dist-consumer-{self._consumer_name}"
        )
        self._thread.start()
        log.info(
            "Task distribution consumer started: group=%s, consumer=%s",
            self._group, self._consumer_name
        )

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the consumer loop gracefully."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        log.info("Task distribution consumer stopped: %s", self._consumer_name)

    @property
    def running(self) -> bool:
        return not self._stop_event.is_set()

    def _run_loop(self) -> None:
        """Main consumer loop: poll for new messages, process, ack/nack."""
        self._recover_pending()

        while self.running:
            try:
                self._claim_abandoned()
                messages = self._stream_consumer.consume(
                    count=self._poll_count,
                    block_ms=self._poll_interval_ms,
                )
                for msg_id, raw_data in messages:
                    if not self.running:
                        break
                    self._process_message(msg_id, raw_data)
            except Exception:
                log.exception(
                    "Error in task distribution consumer loop, sleeping 1s"
                )
                if self.running:
                    time.sleep(1.0)

    def _recover_pending(self) -> None:
        """On startup, re-process any pending (unacked) messages from before crash."""
        pending = self._stream_consumer.consume_pending(count=100)
        if pending:
            log.info(
                "Recovering %d pending messages for consumer %s",
                len(pending), self._consumer_name
            )
            for msg_id, raw_data in pending:
                if not self.running:
                    break
                if raw_data:
                    self._process_message(msg_id, raw_data)
                else:
                    self._stream_consumer.ack(msg_id)

    def _claim_abandoned(self) -> None:
        """Claim messages idle longer than claim_idle_ms from dead consumers."""
        claimed = self._stream_consumer.claim_stale(
            min_idle_ms=self._claim_idle_ms, count=5
        )
        if claimed:
            log.info(
                "Claimed %d abandoned messages for %s",
                len(claimed), self._consumer_name
            )
            for msg_id, raw_data in claimed:
                if not self.running:
                    break
                self._process_message(msg_id, raw_data)

    def _process_message(self, msg_id: str, raw_data: dict) -> None:
        """Process a single message: extract task data, invoke handler, ack/nack."""
        task_data = raw_data.get("data", raw_data)
        if isinstance(task_data, str):
            try:
                task_data = json.loads(task_data)
            except (json.JSONDecodeError, TypeError):
                log.warning("Invalid task data in message %s, sending to DLQ", msg_id)
                self._send_to_dlq(msg_id, task_data, "invalid_json")
                self._stream_consumer.ack(msg_id)
                return

        task_id = task_data.get("task_id", msg_id)
        retry_key = msg_id

        try:
            success = self._task_handler(task_data)
            if success:
                self._stream_consumer.ack(msg_id)
                self._retry_counts.pop(retry_key, None)
                log.info("Task processed successfully: task_id=%s, msg_id=%s", task_id, msg_id)
            else:
                self._handle_failure(msg_id, task_data, task_id, retry_key, "handler_returned_false")
        except Exception as exc:
            log.exception("Task handler raised for task_id=%s, msg_id=%s", task_id, msg_id)
            self._handle_failure(msg_id, task_data, task_id, retry_key, str(exc))

    def _handle_failure(self, msg_id: str, task_data: dict, task_id: str,
                        retry_key: str, error: str) -> None:
        """Handle task processing failure: retry or DLQ."""
        current_retries = self._retry_counts.get(retry_key, 0) + 1
        self._retry_counts[retry_key] = current_retries

        if current_retries >= self._max_retries:
            log.warning(
                "Task exhausted retries (%d/%d): task_id=%s, msg_id=%s. Sending to DLQ.",
                current_retries, self._max_retries, task_id, msg_id
            )
            self._send_to_dlq(msg_id, task_data, error)
            self._stream_consumer.ack(msg_id)
            self._retry_counts.pop(retry_key, None)
        else:
            log.warning(
                "Task failed (%d/%d retries): task_id=%s, msg_id=%s",
                current_retries, self._max_retries, task_id, msg_id
            )

    def _send_to_dlq(self, msg_id: str, task_data, error: str) -> None:
        """Move a failed message to the dead letter queue stream."""
        dlq_stream = f"{DLQ_PREFIX}{self._stream_name}"
        dlq_data = {
            "original_msg_id": msg_id,
            "original_stream": self._stream_name,
            "task_data": task_data if isinstance(task_data, dict) else {"raw": str(task_data)},
            "error": error,
            "consumer": self._consumer_name,
            "failed_at": time.time(),
        }
        try:
            self._dlq_producer.publish(dlq_stream, dlq_data)
        except Exception:
            log.exception("Failed to send message %s to DLQ", msg_id)

    def pending_count(self) -> int:
        """Get the number of pending (unacked) messages."""
        return self._stream_consumer.pending_count()

    def consumer_name(self) -> str:
        """Get this consumer's unique name."""
        return self._consumer_name
