"""Redis-backed provider health state for horizontal scaling.

Replaces the in-memory `present_providers` and `unhealthy_providers` dicts on
module with Redis so all pylon_main replicas share the same provider registry
without needing sticky sessions.

Both dicts follow the structure:
  {project_id: {provider_name: {service_location_url: descriptor}}}

The descriptor is a Pydantic v2 model (dynamically generated from JSON schema)
that supports model_dump_json() / model_validate_json().

Redis key layout:
  provider_health:{category}:{project_id}  — hash: "{provider_name}\x1f{url}" → JSON

Where category is "present" or "unhealthy".
The field separator \x1f (unit separator) is safe since neither provider_name nor
URL would contain this control character.

TTL: 5 minutes (300s). Refreshed on each write — providers are refreshed
frequently via health checks on startup.
"""

import json

from pylon.core.tools import log


DEFAULT_TTL = 300  # 5 minutes
FIELD_SEP = "\x1f"


def _make_field(provider_name: str, url: str) -> str:
    return f"{provider_name}{FIELD_SEP}{url}"


def _parse_field(field: str):
    parts = field.split(FIELD_SEP, 1)
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]


class _UrlDict:
    """Proxy for the innermost level: {url: descriptor}.

    Supports dict-like access for backward compatibility with code like:
      providers = self.present_providers[project][provider_name]
      random.choice(list(providers.values()))
    """

    def __init__(self, store, category: str, project_id: str, provider_name: str):
        self._store = store
        self._category = category
        self._project_id = project_id
        self._provider_name = provider_name

    def _get_all(self) -> dict:
        return self._store._get_urls_for_provider(
            self._category, self._project_id, self._provider_name
        )

    def __setitem__(self, url: str, descriptor):
        self._store._set_entry(
            self._category, self._project_id, self._provider_name, url, descriptor
        )

    def __getitem__(self, url: str):
        result = self._store._get_entry(
            self._category, self._project_id, self._provider_name, url
        )
        if result is None:
            raise KeyError(url)
        return result

    def __contains__(self, url: str) -> bool:
        return self._store._has_entry(
            self._category, self._project_id, self._provider_name, url
        )

    def __len__(self) -> int:
        return len(self._get_all())

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def __iter__(self):
        return iter(self._get_all())

    def pop(self, url: str, *args):
        result = self._store._pop_entry(
            self._category, self._project_id, self._provider_name, url
        )
        if result is None:
            if args:
                return args[0]
            raise KeyError(url)
        return result

    def values(self):
        return self._get_all().values()

    def keys(self):
        return self._get_all().keys()

    def items(self):
        return self._get_all().items()

    def get(self, url: str, default=None):
        result = self._store._get_entry(
            self._category, self._project_id, self._provider_name, url
        )
        return result if result is not None else default


class _ProviderDict:
    """Proxy for the middle level: {provider_name: {url: descriptor}}.

    Supports dict-like access for backward compatibility with code like:
      if provider_name not in self.present_providers[project]:
          continue
      providers = self.present_providers[project][provider_name]
    """

    def __init__(self, store, category: str, project_id: str):
        self._store = store
        self._category = category
        self._project_id = project_id

    def __contains__(self, provider_name: str) -> bool:
        return self._store._has_provider(
            self._category, self._project_id, provider_name
        )

    def __getitem__(self, provider_name: str):
        if provider_name not in self:
            raise KeyError(provider_name)
        return _UrlDict(
            self._store, self._category, self._project_id, provider_name
        )

    def __setitem__(self, provider_name: str, value: dict):
        for url, descriptor in value.items():
            self._store._set_entry(
                self._category, self._project_id, provider_name, url, descriptor
            )

    def __len__(self) -> int:
        return len(self._store._get_providers_for_project(
            self._category, self._project_id
        ))

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def __iter__(self):
        return iter(self._store._get_providers_for_project(
            self._category, self._project_id
        ))

    def keys(self):
        return self._store._get_providers_for_project(
            self._category, self._project_id
        )

    def items(self):
        providers = self._store._get_providers_for_project(
            self._category, self._project_id
        )
        return [
            (name, _UrlDict(self._store, self._category, self._project_id, name))
            for name in providers
        ]

    def values(self):
        providers = self._store._get_providers_for_project(
            self._category, self._project_id
        )
        return [
            _UrlDict(self._store, self._category, self._project_id, name)
            for name in providers
        ]

    def get(self, provider_name: str, default=None):
        if provider_name not in self:
            return default
        return _UrlDict(
            self._store, self._category, self._project_id, provider_name
        )


class RedisProviderHealth:
    """Redis-backed provider health registry for horizontal scaling.

    Drop-in replacement for `self.present_providers = {}` or
    `self.unhealthy_providers = {}` in module.py.

    Supports nested dict-like access:
      store[project_id][provider_name][url] = descriptor
      store[project_id][provider_name].pop(url, None)
      for project_id in store: ...
      if project_id in store: ...
    """

    def __init__(self, redis_client, category: str, descriptor_model=None,
                 ttl: int = DEFAULT_TTL):
        """Initialize the provider health store.

        Args:
            redis_client: Redis client instance
            category: "present" or "unhealthy"
            descriptor_model: Pydantic model class for deserialization (optional).
                If None, descriptors are returned as raw dicts.
            ttl: TTL in seconds for each project key (default 300s / 5 min)
        """
        self._client = redis_client
        self._category = category
        self._descriptor_model = descriptor_model
        self._ttl = ttl
        self._key_prefix = f"provider_health:{category}"

    def _redis_key(self, project_id: str) -> str:
        return f"{self._key_prefix}:{project_id}"

    def _serialize_descriptor(self, descriptor) -> str:
        if hasattr(descriptor, 'model_dump_json'):
            return descriptor.model_dump_json()
        if hasattr(descriptor, 'json'):
            return descriptor.json()
        return json.dumps(descriptor, default=str)

    def _deserialize_descriptor(self, data: str):
        if self._descriptor_model is not None:
            try:
                return self._descriptor_model.model_validate_json(data)
            except Exception:
                log.warning("Failed to deserialize descriptor with model, using dict")
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return None

    def _set_entry(self, category: str, project_id: str, provider_name: str,
                   url: str, descriptor) -> None:
        key = self._redis_key(project_id)
        field = _make_field(provider_name, url)
        value = self._serialize_descriptor(descriptor)
        pipe = self._client.pipeline()
        pipe.hset(key, field, value)
        pipe.expire(key, self._ttl)
        pipe.execute()

    def _get_entry(self, category: str, project_id: str, provider_name: str,
                   url: str):
        key = self._redis_key(project_id)
        field = _make_field(provider_name, url)
        data = self._client.hget(key, field)
        if data is None:
            return None
        raw = data if isinstance(data, str) else data.decode()
        return self._deserialize_descriptor(raw)

    def _has_entry(self, category: str, project_id: str, provider_name: str,
                   url: str) -> bool:
        key = self._redis_key(project_id)
        field = _make_field(provider_name, url)
        return bool(self._client.hexists(key, field))

    def _pop_entry(self, category: str, project_id: str, provider_name: str,
                   url: str):
        key = self._redis_key(project_id)
        field = _make_field(provider_name, url)
        data = self._client.hget(key, field)
        if data is None:
            return None
        self._client.hdel(key, field)
        raw = data if isinstance(data, str) else data.decode()
        return self._deserialize_descriptor(raw)

    def _has_provider(self, category: str, project_id: str,
                      provider_name: str) -> bool:
        key = self._redis_key(project_id)
        raw_fields = self._client.hkeys(key)
        if not raw_fields:
            return False
        prefix = f"{provider_name}{FIELD_SEP}"
        for f in raw_fields:
            field_str = f if isinstance(f, str) else f.decode()
            if field_str.startswith(prefix):
                return True
        return False

    def _get_providers_for_project(self, category: str, project_id: str) -> list:
        key = self._redis_key(project_id)
        raw_fields = self._client.hkeys(key)
        if not raw_fields:
            return []
        providers = set()
        for f in raw_fields:
            field_str = f if isinstance(f, str) else f.decode()
            provider_name, _ = _parse_field(field_str)
            if provider_name:
                providers.add(provider_name)
        return list(providers)

    def _get_urls_for_provider(self, category: str, project_id: str,
                               provider_name: str) -> dict:
        key = self._redis_key(project_id)
        raw_data = self._client.hgetall(key)
        if not raw_data:
            return {}
        result = {}
        prefix = f"{provider_name}{FIELD_SEP}"
        for f, v in raw_data.items():
            field_str = f if isinstance(f, str) else f.decode()
            if not field_str.startswith(prefix):
                continue
            _, url = _parse_field(field_str)
            if url is None:
                continue
            val_str = v if isinstance(v, str) else v.decode()
            descriptor = self._deserialize_descriptor(val_str)
            if descriptor is not None:
                result[url] = descriptor
        return result

    def _get_all_project_ids(self) -> list:
        pattern = f"{self._key_prefix}:*"
        project_ids = []
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor=cursor, match=pattern, count=100)
            for k in keys:
                key_str = k if isinstance(k, str) else k.decode()
                project_id = key_str[len(self._key_prefix) + 1:]
                if project_id:
                    project_ids.append(project_id)
            if cursor == 0:
                break
        return project_ids

    # --- Top-level dict interface ---

    def __contains__(self, project_id: str) -> bool:
        key = self._redis_key(project_id)
        return bool(self._client.exists(key))

    def __getitem__(self, project_id: str):
        if project_id not in self:
            raise KeyError(project_id)
        return _ProviderDict(self, self._category, project_id)

    def __setitem__(self, project_id: str, value: dict):
        for provider_name, urls in value.items():
            for url, descriptor in urls.items():
                self._set_entry(
                    self._category, project_id, provider_name, url, descriptor
                )

    def __iter__(self):
        return iter(self._get_all_project_ids())

    def __len__(self) -> int:
        return len(self._get_all_project_ids())

    def __bool__(self) -> bool:
        return self.__len__() > 0

    def keys(self):
        return self._get_all_project_ids()

    def items(self):
        project_ids = self._get_all_project_ids()
        return [
            (pid, _ProviderDict(self, self._category, pid))
            for pid in project_ids
        ]

    def values(self):
        project_ids = self._get_all_project_ids()
        return [
            _ProviderDict(self, self._category, pid)
            for pid in project_ids
        ]

    def get(self, project_id: str, default=None):
        if project_id not in self:
            return default
        return _ProviderDict(self, self._category, project_id)

    def clear(self) -> None:
        """Remove all provider health entries for this category."""
        project_ids = self._get_all_project_ids()
        if project_ids:
            keys = [self._redis_key(pid) for pid in project_ids]
            self._client.delete(*keys)

    def set_descriptor_model(self, model) -> None:
        """Set the descriptor model for deserialization.

        Called after the model is dynamically generated on startup.
        """
        self._descriptor_model = model

    def refresh_ttl(self, project_id: str) -> bool:
        """Reset TTL on a project's provider health key."""
        key = self._redis_key(project_id)
        return bool(self._client.expire(key, self._ttl))

    def get_flat_list(self) -> list:
        """Get a flat list of all entries for admin API compatibility.

        Returns:
            List of dicts with project_id, provider_name, service_location_url, descriptor
        """
        result = []
        project_ids = self._get_all_project_ids()
        for project_id in project_ids:
            key = self._redis_key(project_id)
            raw_data = self._client.hgetall(key)
            if not raw_data:
                continue
            for f, v in raw_data.items():
                field_str = f if isinstance(f, str) else f.decode()
                provider_name, url = _parse_field(field_str)
                if provider_name is None:
                    continue
                val_str = v if isinstance(v, str) else v.decode()
                descriptor = self._deserialize_descriptor(val_str)
                if descriptor is not None:
                    result.append({
                        "project_id": project_id,
                        "provider_name": provider_name,
                        "service_location_url": url,
                        "descriptor": descriptor,
                    })
        return result
