"""Redis-backed index types registry for horizontal scaling.

Replaces the in-memory `index_types` dict on module with Redis so all
pylon_main replicas share the same file loader type mappings without needing
sticky sessions or requiring each pod to receive the event_node broadcast.

The registry is populated at startup when the indexer emits file loader types
via event_node. All pods write the same data (idempotent). At read time, any
pod can retrieve the full registry from Redis.

Redis key layout:
  index_types:global  — hash with 3 fields:
    document_types → JSON({extension: mime_type, ...})
    image_types    → JSON({extension: mime_type, ...})
    code_types     → JSON({extension: mime_type, ...})

TTL: 1 hour (3600s). Refreshed on each write.
"""

import json

from pylon.core.tools import log


DEFAULT_TTL = 3600  # 1 hour


class RedisIndexTypes:
    """Manages index types (file loader mappings) in Redis for horizontal scaling.

    Drop-in replacement for `self.index_types = {}` in module.py.
    Supports the same dict-like access pattern used in api/v2/index_types.py,
    rpc/application.py, and utils/attachments.py.
    """

    CATEGORY_KEYS = ("document_types", "image_types", "code_types")

    def __init__(self, redis_client, ttl: int = DEFAULT_TTL):
        self._client = redis_client
        self._ttl = ttl
        self._key = "index_types:global"

    def set_all(self, payload: dict) -> None:
        """Store the full index types payload from the indexer event.

        Args:
            payload: Dict with keys 'document_types', 'image_types', 'code_types',
                     each mapping file extension to mime type string.
        """
        if not payload:
            return
        pipe = self._client.pipeline()
        for category in self.CATEGORY_KEYS:
            value = payload.get(category, {})
            pipe.hset(self._key, category, json.dumps(value))
        pipe.expire(self._key, self._ttl)
        pipe.execute()
        total = sum(len(payload.get(k, {})) for k in self.CATEGORY_KEYS)
        log.info("Stored index_types in Redis (%d total extensions)", total)

    def get_all(self) -> dict:
        """Get the full index types registry.

        Returns:
            Dict with keys 'document_types', 'image_types', 'code_types'.
            Returns empty dict for missing categories.
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
                log.warning("Skipping malformed index_types category: %s", key_str)
                result[key_str] = {}
        return result

    def get_category(self, category: str) -> dict:
        """Get a single category of index types.

        Args:
            category: One of 'document_types', 'image_types', 'code_types'

        Returns:
            Dict mapping file extension to mime type, or empty dict if not found.
        """
        data = self._client.hget(self._key, category)
        if data is None:
            return {}
        raw = data if isinstance(data, str) else data.decode()
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning("Malformed index_types data for category: %s", category)
            return {}

    def clear(self) -> bool:
        """Remove the entire index types registry.

        Returns:
            True if the key existed and was deleted.
        """
        return bool(self._client.delete(self._key))

    def count(self) -> int:
        """Get the total number of file extensions across all categories.

        Returns:
            Total count of extensions in all categories.
        """
        all_types = self.get_all()
        return sum(len(v) for v in all_types.values() if isinstance(v, dict))

    def exists(self) -> bool:
        """Check if the index types registry exists in Redis.

        Returns:
            True if the key exists.
        """
        return bool(self._client.exists(self._key))

    def refresh_ttl(self) -> bool:
        """Reset the TTL on the registry key.

        Returns:
            True if the key exists and TTL was set.
        """
        return bool(self._client.expire(self._key, self._ttl))

    # --- Dict-like interface for compatibility with existing code ---

    def __getitem__(self, key: str):
        """Dict-like access: store['document_types'] -> {ext: mime, ...}.

        Used by api/v2/index_types.py which returns self.module.index_types directly.
        """
        all_data = self.get_all()
        if key not in all_data:
            raise KeyError(key)
        return all_data[key]

    def __setitem__(self, key: str, value: dict) -> None:
        """Dict-like assignment: store['document_types'] = {...}."""
        pipe = self._client.pipeline()
        pipe.hset(self._key, key, json.dumps(value))
        pipe.expire(self._key, self._ttl)
        pipe.execute()

    def __contains__(self, key: str) -> bool:
        """Dict-like 'in' operator: 'document_types' in store."""
        return bool(self._client.hexists(self._key, key))

    def __len__(self) -> int:
        """Dict-like len(): number of categories stored."""
        return self._client.hlen(self._key)

    def __bool__(self) -> bool:
        """Truthy when there are any categories stored."""
        return self.__len__() > 0

    def get(self, key: str, default=None):
        """Dict-like get with default.

        Used by rpc/application.py: self.index_types.get('document_types', {})
        """
        data = self._client.hget(self._key, key)
        if data is None:
            return default
        raw = data if isinstance(data, str) else data.decode()
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return default

    def items(self):
        """Dict-like items() returning (category, type_dict) pairs."""
        return self.get_all().items()

    def values(self):
        """Dict-like values() returning type dicts."""
        return self.get_all().values()

    def keys(self):
        """Dict-like keys() returning category names."""
        raw_keys = self._client.hkeys(self._key)
        return [k if isinstance(k, str) else k.decode() for k in raw_keys]
