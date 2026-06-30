"""Redis-backed toolkit schemas registry for horizontal scaling.

Replaces the in-memory `toolkit_schemas` dict on module with Redis so all
pylon_main replicas share the same toolkit registry without needing sticky
sessions or requiring each pod to receive the event_node broadcast.

The registry is populated at startup when the indexer emits toolkit schemas
via event_node. All pods write the same schemas (idempotent via HSET). At
read time, any pod can retrieve the full registry from Redis.

Redis key layout:
  toolkit_schemas:global  — hash: toolkit_title → JSON(schema)

TTL: 1 hour (3600s). Refreshed on each write. If the key expires (Redis
restart without persistence, etc.) the schemas will be re-populated on the
next startup or can be re-requested via event_node.
"""

import json

from pylon.core.tools import log


DEFAULT_TTL = 3600  # 1 hour


class RedisToolkitSchemas:
    """Manages toolkit schema registry in Redis for horizontal scaling.

    Drop-in replacement for `self.toolkit_schemas = {}` in module.py.
    Supports the same dict-like access pattern used in get_toolkit_schemas()
    and application_tools.py.
    """

    def __init__(self, redis_client, ttl: int = DEFAULT_TTL):
        self._client = redis_client
        self._ttl = ttl
        self._key = "toolkit_schemas:global"

    def set_schema(self, title: str, schema: dict) -> None:
        """Store a toolkit schema.

        Args:
            title: The toolkit title (used as the registry key)
            schema: The full toolkit schema dict
        """
        pipe = self._client.pipeline()
        pipe.hset(self._key, title, json.dumps(schema, default=str))
        pipe.expire(self._key, self._ttl)
        pipe.execute()

    def set_schemas_batch(self, schemas: list) -> None:
        """Store multiple toolkit schemas at once (pipeline).

        Typically called during startup when the indexer emits all schemas.

        Args:
            schemas: List of schema dicts, each must have a 'title' key
        """
        if not schemas:
            return
        pipe = self._client.pipeline()
        for schema in schemas:
            title = schema.get("title")
            if not title:
                log.warning("Skipping toolkit schema without title")
                continue
            pipe.hset(self._key, title, json.dumps(schema, default=str))
        pipe.expire(self._key, self._ttl)
        pipe.execute()
        log.info("Stored %d toolkit schemas in Redis", len(schemas))

    def get_schema(self, title: str):
        """Get a single toolkit schema by title.

        Args:
            title: The toolkit title

        Returns:
            The schema dict, or None if not found
        """
        data = self._client.hget(self._key, title)
        if data is None:
            return None
        raw = data if isinstance(data, str) else data.decode()
        return json.loads(raw)

    def get_all(self) -> dict:
        """Get the full toolkit schemas registry.

        Returns:
            Dict mapping toolkit title to schema dict.
            Returns empty dict if the key doesn't exist.
        """
        raw_data = self._client.hgetall(self._key)
        if not raw_data:
            return {}
        result = {}
        for k, v in raw_data.items():
            key_str = k if isinstance(k, str) else k.decode()
            val_str = v if isinstance(v, str) else v.decode()
            try:
                result[key_str] = json.loads(val_str)
            except (json.JSONDecodeError, TypeError):
                log.warning("Skipping malformed toolkit schema for key: %s", key_str)
        return result

    def remove_schema(self, title: str) -> bool:
        """Remove a toolkit schema from the registry.

        Args:
            title: The toolkit title to remove

        Returns:
            True if the schema existed and was removed
        """
        removed = self._client.hdel(self._key, title)
        return removed > 0

    def clear(self) -> bool:
        """Remove the entire toolkit schemas registry.

        Returns:
            True if the key existed and was deleted
        """
        return bool(self._client.delete(self._key))

    def count(self) -> int:
        """Get the number of registered toolkit schemas.

        Returns:
            Number of schemas in the registry
        """
        return self._client.hlen(self._key)

    def exists(self, title: str) -> bool:
        """Check if a specific toolkit schema is registered.

        Args:
            title: The toolkit title

        Returns:
            True if the schema exists in the registry
        """
        return bool(self._client.hexists(self._key, title))

    def keys(self) -> list:
        """Get all registered toolkit titles.

        Returns:
            List of toolkit title strings
        """
        raw_keys = self._client.hkeys(self._key)
        return [k if isinstance(k, str) else k.decode() for k in raw_keys]

    def refresh_ttl(self) -> bool:
        """Reset the TTL on the registry key.

        Useful after bulk operations or periodic refresh.

        Returns:
            True if the key exists and TTL was set
        """
        return bool(self._client.expire(self._key, self._ttl))

    # --- Dict-like interface for compatibility with existing code ---

    def __getitem__(self, title: str) -> dict:
        """Dict-like access: store[title] -> schema.

        Raises KeyError if title not found (same as dict).
        """
        schema = self.get_schema(title)
        if schema is None:
            raise KeyError(title)
        return schema

    def __setitem__(self, title: str, schema: dict) -> None:
        """Dict-like assignment: store[title] = schema."""
        self.set_schema(title, schema)

    def __contains__(self, title: str) -> bool:
        """Dict-like 'in' operator: title in store."""
        return self.exists(title)

    def __len__(self) -> int:
        """Dict-like len(): len(store)."""
        return self.count()

    def get(self, title: str, default=None):
        """Dict-like get with default."""
        result = self.get_schema(title)
        return result if result is not None else default

    def items(self):
        """Dict-like items() returning (title, schema) pairs."""
        return self.get_all().items()

    def values(self):
        """Dict-like values() returning schema dicts."""
        return self.get_all().values()
