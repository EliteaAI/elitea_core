import hashlib
import json
import threading
import time
from collections import OrderedDict
from copy import deepcopy
from typing import Optional


# Caps settings, not schema: schemas reach ~42KB but are only hashed, while the stored result mirrors settings
MAX_CACHEABLE_SETTINGS_BYTES = 32 * 1024
MAX_TOTAL_CACHE_BYTES = 32 * 1024 * 1024


def _dumps(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def make_validator_cache_key(type_: str, settings: dict, schema: dict) -> Optional[str]:
    # Validator is a pure schema parse, so its output is fully determined by these three inputs
    try:
        settings_json = _dumps(settings)
        if len(settings_json) > MAX_CACHEABLE_SETTINGS_BYTES:
            return None
        payload = "\n".join((_dumps(type_), settings_json, _dumps(schema)))
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _result_size(result) -> int:
    return len(json.dumps(result, separators=(",", ":"), default=str))


class ValidatorResultCache:
    # In-process only: cached results hold unsecreted settings, so never move this to Redis/DB
    def __init__(self, max_entries: int = 2000, ttl_seconds: float = 600,
                 max_total_bytes: int = MAX_TOTAL_CACHE_BYTES, clock=time.monotonic):
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.max_total_bytes = max_total_bytes
        self._clock = clock
        self._entries: OrderedDict = OrderedDict()
        self._total_bytes = 0
        self._lock = threading.Lock()

    def get(self, key: Optional[str]) -> Optional[dict]:
        if key is None:
            return None
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, result, _ = entry
            if expires_at <= self._clock():
                self._drop(key)
                return None
            self._entries.move_to_end(key)
        return deepcopy(result)

    def put(self, key: Optional[str], result: dict) -> None:
        if key is None:
            return
        size = _result_size(result)
        if size > self.max_total_bytes:
            return
        with self._lock:
            if key in self._entries:
                self._drop(key)
            self._entries[key] = (self._clock() + self.ttl_seconds, deepcopy(result), size)
            self._total_bytes += size
            while len(self._entries) > self.max_entries or self._total_bytes > self.max_total_bytes:
                self._drop(next(iter(self._entries)))

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._total_bytes = 0

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    def _drop(self, key: str) -> None:
        self._total_bytes -= self._entries.pop(key)[2]

    def __len__(self) -> int:
        return len(self._entries)


toolkit_validator_cache = ValidatorResultCache()
