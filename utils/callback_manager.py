"""Redis-backed callback task manager for horizontal scaling.

Replaces the in-memory `callback_tasks` dict with Redis so that callback
data is accessible from any pylon_main replica. When a predict/pipeline
request includes a callback URL, the callback is registered in Redis. When
the task completes (on potentially a different pod), the callback data is
retrieved and consumed.

Redis key layout:
  callback_tasks:{task_id}  — string: JSON({callback_url, callback_headers})
"""

import json

from pylon.core.tools import log


DEFAULT_TTL = 86400  # 24 hours


class CallbackManager:
    """Manages task callback registrations in Redis for horizontal scaling."""

    def __init__(self, redis_client, ttl: int = DEFAULT_TTL):
        self._client = redis_client
        self._ttl = ttl

    def _key(self, task_id: str) -> str:
        return f"callback_tasks:{task_id}"

    def register_callback(self, task_id: str, callback_url: str,
                          callback_headers: dict = None) -> None:
        """Register a callback for a task.

        Args:
            task_id: The task identifier
            callback_url: URL to POST results to when task completes
            callback_headers: Optional HTTP headers to include in the callback POST
        """
        data = {
            "callback_url": callback_url,
            "callback_headers": callback_headers,
        }
        key = self._key(task_id)
        self._client.set(key, json.dumps(data), ex=self._ttl)
        log.info("Registered callback for task %s -> %s", task_id, callback_url)

    def get_callback(self, task_id: str) -> dict:
        """Get callback data for a task without removing it.

        Args:
            task_id: The task identifier

        Returns:
            Dict with callback_url and callback_headers, or None if not found
        """
        key = self._key(task_id)
        data = self._client.get(key)
        if data is None:
            return None
        return json.loads(data)

    def pop_callback(self, task_id: str) -> dict:
        """Get and remove callback data for a task (atomic).

        Uses GETDEL (Redis 6.2+) for atomicity — ensures exactly-once
        consumption even if multiple pods race on the same task completion.

        Args:
            task_id: The task identifier

        Returns:
            Dict with callback_url and callback_headers, or None if not found
        """
        key = self._key(task_id)
        data = self._client.getdel(key)
        if data is None:
            return None
        log.info("Consumed callback for task %s", task_id)
        return json.loads(data)

    def remove_callback(self, task_id: str) -> bool:
        """Remove a callback registration without returning its data.

        Args:
            task_id: The task identifier

        Returns:
            True if the callback existed and was removed, False otherwise
        """
        key = self._key(task_id)
        removed = self._client.delete(key)
        return removed > 0

    def exists(self, task_id: str) -> bool:
        """Check if a callback is registered for a task.

        Args:
            task_id: The task identifier

        Returns:
            True if a callback is registered
        """
        key = self._key(task_id)
        return bool(self._client.exists(key))
