"""Redis-backed task logs cache for horizontal scaling.

Replaces the in-memory `task_logs` dict with Redis sorted sets so that
log entries are accessible from any pylon_main replica. When a client
subscribes to task logs on one pod and the logs arrive on another, both
pods can serve the cached history via Redis.

Redis key layout:
  task_logs:{task_id}  — sorted set: score=timestamp, member=JSON(record)

Each task's log entries are stored as JSON members in a sorted set with
timestamp scores for time-ordered retrieval. A TTL is applied after each
write to automatically evict old logs.
"""

import json
import time

from pylon.core.tools import log


DEFAULT_TTL = 604800  # 7 days
DEFAULT_MAX_ENTRIES = 500


class TaskLogsRedis:
    """Manages task log entries in Redis sorted sets for horizontal scaling."""

    def __init__(self, redis_client, ttl: int = DEFAULT_TTL,
                 max_entries: int = DEFAULT_MAX_ENTRIES):
        self._client = redis_client
        self._ttl = ttl
        self._max_entries = max_entries

    def _key(self, task_id: str) -> str:
        return f"task_logs:{task_id}"

    def append(self, task_id: str, record: dict) -> None:
        """Append a log record to the task's log stream.

        The record is stored as a JSON-encoded member with the current
        timestamp as score for time-ordered retrieval.

        Args:
            task_id: The task identifier
            record: A log record dict (must be JSON-serializable)
        """
        key = self._key(task_id)
        timestamp = record.get("time", time.time())
        member = json.dumps(record, default=str)
        pipe = self._client.pipeline()
        pipe.zadd(key, {member: timestamp})
        pipe.zremrangebyrank(key, 0, -(self._max_entries + 1))
        pipe.expire(key, self._ttl)
        pipe.execute()

    def append_batch(self, task_id: str, records: list) -> None:
        """Append multiple log records to the task's log stream.

        Args:
            task_id: The task identifier
            records: List of log record dicts
        """
        if not records:
            return
        key = self._key(task_id)
        mapping = {}
        for record in records:
            timestamp = record.get("time", time.time())
            member = json.dumps(record, default=str)
            mapping[member] = timestamp
        pipe = self._client.pipeline()
        pipe.zadd(key, mapping)
        pipe.zremrangebyrank(key, 0, -(self._max_entries + 1))
        pipe.expire(key, self._ttl)
        pipe.execute()

    def get_latest(self, task_id: str, count: int = 100) -> list:
        """Get the most recent log entries for a task.

        Returns entries in chronological order (oldest first).

        Args:
            task_id: The task identifier
            count: Maximum number of entries to return (default 100)

        Returns:
            List of log record dicts ordered by timestamp ascending
        """
        key = self._key(task_id)
        members = self._client.zrange(key, -count, -1)
        result = []
        for member in members:
            raw = member if isinstance(member, str) else member.decode()
            try:
                result.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                log.warning("Skipping malformed log entry for task %s", task_id)
        return result

    def get_all(self, task_id: str) -> list:
        """Get all log entries for a task in chronological order.

        Args:
            task_id: The task identifier

        Returns:
            List of log record dicts ordered by timestamp ascending
        """
        key = self._key(task_id)
        members = self._client.zrange(key, 0, -1)
        result = []
        for member in members:
            raw = member if isinstance(member, str) else member.decode()
            try:
                result.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                log.warning("Skipping malformed log entry for task %s", task_id)
        return result

    def get_since(self, task_id: str, since_timestamp: float) -> list:
        """Get log entries after a given timestamp.

        Useful for incremental polling by clients that already have
        older entries cached locally.

        Args:
            task_id: The task identifier
            since_timestamp: Unix timestamp; returns entries strictly after this

        Returns:
            List of log record dicts ordered by timestamp ascending
        """
        key = self._key(task_id)
        members = self._client.zrangebyscore(
            key, f"({since_timestamp}", "+inf"
        )
        result = []
        for member in members:
            raw = member if isinstance(member, str) else member.decode()
            try:
                result.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                log.warning("Skipping malformed log entry for task %s", task_id)
        return result

    def clear(self, task_id: str) -> bool:
        """Remove all log entries for a task.

        Args:
            task_id: The task identifier

        Returns:
            True if the key existed and was deleted, False otherwise
        """
        key = self._key(task_id)
        return bool(self._client.delete(key))

    def count(self, task_id: str) -> int:
        """Get the number of log entries for a task.

        Args:
            task_id: The task identifier

        Returns:
            Number of entries in the sorted set
        """
        key = self._key(task_id)
        return self._client.zcard(key)

    def exists(self, task_id: str) -> bool:
        """Check if any log entries exist for a task.

        Args:
            task_id: The task identifier

        Returns:
            True if the task has log entries
        """
        key = self._key(task_id)
        return bool(self._client.exists(key))

    def set_ttl(self, task_id: str, ttl: int = None) -> bool:
        """Reset the TTL on a task's log entries.

        Args:
            task_id: The task identifier
            ttl: TTL in seconds (defaults to instance TTL)

        Returns:
            True if the key exists and TTL was set
        """
        key = self._key(task_id)
        return bool(self._client.expire(key, ttl or self._ttl))
